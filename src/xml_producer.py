import os
import sys
import time
import signal
import random
import logging
import argparse

from functools import partial
from configparser import ConfigParser
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer

from utils import real_sleep, delivery_report


def signal_handler(producer, sig, frame):
    logging.info("Flushing records...")
    producer.flush()
    sys.exit(0)


def main(args):
    logging.basicConfig(
        format="%(asctime)s.%(msecs)03d [%(levelname)s]: %(message)s",
        level=logging.INFO if not args.debug else logging.DEBUG,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    XML_SCHEMA = """
    <?xml version="1.0" encoding="UTF-8"?>
    <objectRoot>
        <side>{side}</side>
        <quantity>{quantity}</quantity>
        <symbol>{symbol}</symbol>
        <price>{price}</price>
        <account>{account}</account>
        <userid>{userid}</userid>
    </objectRoot>
    """

    string_serializer = StringSerializer("utf_8")

    kconfig = ConfigParser()
    kconfig.read(
        os.path.join(
            "config",
            args.config_filename,
        )
    )

    producer = Producer(dict(kconfig["kafka"]))

    signal_handler_producer = partial(signal_handler, producer)
    signal.signal(signal.SIGINT, signal_handler_producer)
    signal.signal(signal.SIGTERM, signal_handler_producer)

    while True:
        producer.poll(0.0)
        try:
            start_time = time.time()
            message = XML_SCHEMA.format(
                side=random.choice(["BUY", "SELL"]),
                quantity=random.randint(1, 10),
                symbol=f"STK_{random.randint(0, 9)}",
                price=random.randint(100, 2500) / 100,
                account=f"Account_{random.randint(0, 9)}{random.randint(0, 9)}",
                userid=f"User_{random.randint(0, 9)}{random.randint(0, 9)}",
            )
            producer.produce(
                topic=args.topic,
                value=string_serializer(
                    "".join([line.strip() for line in message.split("\n")])
                ),
                on_delivery=delivery_report,
            )
        except KeyboardInterrupt:
            signal_handler_producer(
                producer,
                None,
                None,
            )
        except Exception as err:
            logging.error(f"{err}")
        finally:
            real_sleep(
                args.interval,
                start_time,
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple XML Producer")
    parser.add_argument(
        "--config-filename",
        dest="config_filename",
        type=str,
        help="Select config filename for additional configuration, such as credentials. Files must be inside the folder config/ (default: config/localhost_docker.ini)",
        default="localhost_docker.ini",
    )
    parser.add_argument(
        "--topic",
        help="Topic name (default: 'stock_trade_xml')",
        dest="topic",
        type=str,
        default="stock_trade_xml",
    )
    parser.add_argument(
        "--interval",
        help="Max interval between messages in milliseconds (default: 500))",
        dest="interval",
        default=3000,
        type=int,
    )
    parser.add_argument(
        "--client-id",
        dest="client_id",
        type=str,
        help="Producer's Client ID (default: 'xml-producer-demo-01')",
        default="xml-producer-demo-01",
    )
    parser.add_argument(
        "--debug",
        dest="debug",
        help="Set as debug level",
        action="store_false",
    )

    main(parser.parse_args())
