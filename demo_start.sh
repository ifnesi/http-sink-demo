#!/bin/bash
docker compose up -d --build

# Waiting services to be ready
echo ""
while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' http://localhost:8081)" != "200" ]]
do
    echo "Waiting Schema Registry to be ready..."
    sleep 2
done

echo ""
while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' http://localhost:8083)" != "200" ]]
do
    echo "Waiting Connect Cluster to be ready..."
    sleep 2
done

echo ""
while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' http://localhost:9021)" != "200" ]]
do
    echo "Waiting Confluent Control Center to be ready..."
    sleep 2
done

# Create Datagen connectors
echo ""
echo "Creating Datagen connector (Stock Trading)"
curl -i -X PUT http://localhost:8083/connectors/datagen_stock_trade/config \
     -H "Content-Type: application/json" \
     -d '{
            "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "kafka.topic": "stock_trade",
            "schema.string": "{\"namespace\": \"ksql\", \"name\": \"StockTrade\", \"type\": \"record\", \"fields\": [{\"name\": \"side\", \"type\": {\"type\": \"string\", \"arg.properties\": {\"options\": [\"BUY\", \"SELL\"]}}}, {\"name\": \"quantity\", \"type\": {\"type\": \"int\", \"arg.properties\": {\"range\": {\"min\": 1, \"max\": 10}}}}, {\"name\": \"symbol\", \"type\": {\"type\": \"string\", \"arg.properties\": {\"regex\": \"STK_[0-9]\"}}}, {\"name\": \"price\", \"type\": {\"type\": \"double\", \"arg.properties\": {\"range\": {\"min\": 1, \"max\": 25}}}}, {\"name\": \"account\", \"type\": {\"type\": \"string\", \"arg.properties\": {\"regex\": \"Account_[0-9]{2}\"}}}, {\"name\": \"userid\", \"type\": {\"type\": \"string\", \"arg.properties\": {\"regex\": \"User_[0-9]{2}\"}}}]}",
            "max.interval": 3000,
            "iterations": 10000000,
            "tasks.max": "1"
        }'
sleep 2

# Create HTTP Sink connector
echo ""
echo "Creating HTTP Sink connector"
curl -i -X PUT http://localhost:8083/connectors/http_sink/config \
     -H "Content-Type: application/json" \
     -d '{
            "topics": "stock_trade",  
            "tasks.max": "1",    
            "connector.class": "io.confluent.connect.http.HttpSinkConnector",
            "request.method": "post",
            "auth.type": "NONE",
            "http.api.url": "http://http-server:8888/api/webhook",
            "headers": "Content-Type:application/json",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schema.registry.url": "http://schema-registry:8081",
            "value.converter.schemas.enable": "false",
            "request.body.format": "json",
            "batch.max.size": "1",
            "batch.prefix": "",
            "batch.json.as.array": "true",
            "confluent.topic.bootstrap.servers": "broker:9094",
            "confluent.topic.replication.factor": "1",
            "reporter.bootstrap.servers": "broker:9094",
            "reporter.result.topic.name": "success-responses",
            "reporter.result.topic.replication.factor": "1",
            "reporter.error.topic.name": "error-responses",
            "reporter.error.topic.replication.factor": "1"
        }'
sleep 2

# Check connector statuses
echo ""
echo ""
echo "Datagen connector status (Stock Trading)"
curl -s http://localhost:8083/connectors/datagen_stock_trade/status
sleep 1

echo ""
echo ""
echo "HTTP Sink connector status"
curl -s http://localhost:8083/connectors/http_sink/status
sleep 1

echo ""
echo ""
echo "Demo environment is ready!"
echo "Confluent Control Center -> http://localhost:9021"
echo ""
