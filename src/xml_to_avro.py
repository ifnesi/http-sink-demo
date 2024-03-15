"""
The following example demonstrates how to perform a consume-transform-produce
loop with exactly-once semantics.

In order to achieve exactly-once semantics we use the transactional producer
and a single transaction aware consumer.

The following assumptions apply to the source data (input_topic below):
    1. There are no duplicates in the input topic.

##  A quick note about exactly-once-processing guarantees and Kafka. ##

The exactly once, and idempotence, guarantees start after the producer has been
provided a record. There is no way for a producer to identify a record as a
duplicate in isolation. Instead it is the application's job to ensure that only
a single copy of any record is passed to the producer.

Special care needs to be taken when expanding the consumer group to multiple
members.
Review KIP-447 for complete details.
"""

import os
import sys
import json
import signal
import logging
import argparse
import fastavro
import xmltodict

from functools import partial
from configparser import ConfigParser
from confluent_kafka import Producer, Consumer, KafkaError, TopicPartition
from confluent_kafka.serialization import (
    StringDeserializer,
    SerializationContext,
    MessageField,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


from utils import delivery_report


def signal_handler(producer, consumer, sig, frame):
    # commit processed message offsets to the transaction
    logging.debug("Committing final transaction(s)")
    producer.send_offsets_to_transaction(
        consumer.position(consumer.assignment()),
        consumer.consumer_group_metadata(),
    )

    # commit transaction
    producer.commit_transaction()
    consumer.close()

    sys.exit(0)


def convert_to_avro(
    msg,
    string_deserializer,
    topic,
    dlq_topic,
    avro_serializer,
    schema_avro,
):
    output_topic, key, value, headers = topic, None, None, list()
    try:
        key, value, headers = msg.key(), msg.value(), msg.headers()
        headers = headers or list()
        if value is not None:
            value = dict(xmltodict.parse(string_deserializer(msg.value())))
            root = schema_avro.get("xpath", "objectRoot")
            raw_message = dict(value.get(root, dict()))
            message = dict()
            for field in schema_avro.get("fields", list()):
                field_name = field["name"]
                field_type = field["type"]
                field_value = raw_message.get(field_name)
                if field_name in raw_message.keys():
                    if field_type in ["int", "long"]:
                        message[field_name] = int(field_value)
                    elif field_type in ["double"]:
                        message[field_name] = float(field_value)
                    elif field_type in ["bool"]:
                        message[field_name] = field_value.lower() in ["true", "1", "t", "y", "yes"]
                    else:
                        message[field_name] = str(field_value)

            value = avro_serializer(
                message,
                SerializationContext(
                    output_topic,
                    MessageField.VALUE,
                ),
            )

    except Exception as err:
        logging.error(f"{err}")
        output_topic = dlq_topic
        key = msg.key()
        value = msg.value()
        headers = msg.headers()
        headers.append({"error": f"{err}"})

    return output_topic, key, value, headers


def main(args):
    print(args.debug)
    logging.basicConfig(
        format="%(asctime)s.%(msecs)03d [%(levelname)s]: %(message)s",
        level=logging.INFO if not args.debug else logging.DEBUG,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    kconfig = ConfigParser()
    kconfig.read(
        os.path.join(
            "config",
            args.config_filename,
        )
    )

    schema_registry_config = dict(kconfig["schema-registry"])
    schema_registry_client = SchemaRegistryClient(schema_registry_config)
    with open(os.path.join("schemas", args.schema), "r") as f:
        schema_str = f.read()
        schema_avro = fastavro.parse_schema(json.loads(schema_str))
    avro_serializer = AvroSerializer(
        schema_registry_client,
        schema_str=schema_str,
    )

    consumer_config = {
        "group.id": args.group_id,
        "auto.offset.reset": "earliest",
        # Do not advance committed offsets outside of the transaction.
        # Consumer offsets are committed along with the transaction
        # using the producer's send_offsets_to_transaction() API.
        "enable.auto.commit": False,
        "enable.partition.eof": True,
    }
    consumer_config.update(dict(kconfig["kafka"]))
    consumer = Consumer(consumer_config)

    # Prior to KIP-447 being supported each input partition requires
    # its own transactional producer, so in this example we use
    # assign() to a single partition rather than subscribe().
    # A more complex alternative is to dynamically create a producer per
    # partition in subscribe's rebalance callback.
    consumer.assign([TopicPartition(args.input_topic, args.input_partition)])
    string_deserializer = StringDeserializer("utf_8")

    producer_config = {
        "transactional.id": "xml_to_avro.py",
    }
    producer_config.update(dict(kconfig["kafka"]))
    producer = Producer(producer_config)

    # Signal handlers
    signal_handler_producer_consumer = partial(signal_handler, producer, consumer)
    signal.signal(signal.SIGINT, signal_handler_producer_consumer)
    signal.signal(signal.SIGTERM, signal_handler_producer_consumer)

    # Initialize producer transaction
    producer.init_transactions()

    # Start producer transaction
    producer.begin_transaction()

    eof = dict()
    msg_cnt = 0
    logging.debug("Starting XML to AVRO Streaming Application")
    while True:
        try:
            # serve delivery reports from previous produces
            producer.poll(0)

            # read message from input_topic
            msg = consumer.poll(timeout=1.0)
            if msg is not None:
                topic, partition = msg.topic(), msg.partition()
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        eof[(topic, partition)] = True
                        logging.debug(
                            f"Reached the end of {topic} [{partition}] at {msg.offset()}"
                        )

                        if len(eof) == len(consumer.assignment()):
                            logging.debug("Reached end of input")

                    continue

                # clear EOF if a new message has been received
                eof.pop((topic, partition), None)

                # process message
                (
                    output_topic,
                    processed_key,
                    processed_value,
                    processed_headers,
                ) = convert_to_avro(
                    msg,
                    string_deserializer,
                    args.output_topic,
                    args.dlq_topic,
                    avro_serializer,
                    schema_avro,
                )

                # produce transformed message to output topic
                producer.produce(
                    topic=output_topic,
                    headers=processed_headers,
                    key=processed_key,
                    value=processed_value,
                    on_delivery=delivery_report,
                )

                msg_cnt += 1
                if msg_cnt % 10 == 0:
                    logging.debug(
                        f"Committing transaction with {msg_cnt} messages at input offset {msg.offset()}"
                    )
                    # Send the consumer's position to transaction to commit
                    # them along with the transaction, committing both
                    # input and outputs in the same transaction is what provides EOS.
                    producer.send_offsets_to_transaction(
                        consumer.position(consumer.assignment()),
                        consumer.consumer_group_metadata(),
                    )

                    # Commit the transaction
                    producer.commit_transaction()

                    # Begin new transaction
                    producer.begin_transaction()
                    msg_cnt = 0

        except KeyboardInterrupt:
            signal_handler_producer_consumer(
                producer,
                consumer,
                None,
                None,
            )

        except Exception as err:
            logging.error(f"{err}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Streaming Application to Convert from XML to AVRO"
    )
    parser.add_argument(
        "--config-filename",
        dest="config_filename",
        type=str,
        help="Select config filename for additional configuration, such as credentials. Files must be inside the folder config/ (default: config/localhost_docker.ini)",
        default="localhost_docker.ini",
    )
    parser.add_argument(
        "--input_topic",
        help="Input Topic name (default: 'stock_trade_xml')",
        dest="input_topic",
        type=str,
        default="stock_trade_xml",
    )
    parser.add_argument(
        "--schema",
        help="Schema filename. Files must be inside the folder schemas/ (default: schemas/stock_trade.avsc)",
        dest="schema",
        type=str,
        default="stock_trade.avsc",
    )
    parser.add_argument(
        "--output_topic",
        help="Input Topic name (default: 'stock_trade_avro_stream_app')",
        dest="output_topic",
        type=str,
        default="stock_trade_avro_stream_app",
    )
    parser.add_argument(
        "--dlq_topic",
        help="Dead Letter Queue Topic name (default: 'stock_trade_avro_stream_app_DLQ')",
        dest="dlq_topic",
        type=str,
        default="stock_trade_avro_stream_app_DLQ",
    )
    parser.add_argument(
        "--input_partition",
        dest="input_partition",
        default=0,
        type=int,
        help="Input partition to consume from",
    )
    parser.add_argument(
        "--group_id",
        dest="group_id",
        help="Group ID (default: 'xml-stream-app-demo')",
        default="xml-stream-app-demo",
    )
    parser.add_argument(
        "--client-id",
        dest="client_id",
        type=str,
        help="Client ID (default: 'xml-stream-app-demo-01')",
        default="xml-stream-app-demo-01",
    )
    parser.add_argument(
        "--debug",
        dest="debug",
        help="Set as debug level",
        action="store_false",
    )

    main(parser.parse_args())
