#!/bin/bash
# Registers the Debezium MongoDB connector with the Kafka Connect REST API

echo "Waiting for Debezium Kafka Connect to be ready..."
until curl -s http://localhost:8083/ > /dev/null; do
  sleep 5;
done

echo "Registering MongoDB CDC connector..."
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/ -d '{
  "name": "mongo-source-connector",
  "config": {
    "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
    "mongodb.connection.string": "mongodb://admin:password@mongodb:27017/?replicaSet=rs0&authSource=admin",
    "mongodb.user": "admin",
    "mongodb.password": "password",
    "topic.prefix": "mongo",
    "collection.include.list": "flinkdb.user_data",
    "tasks.max": "1",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}'

echo -e "\nConnector registered!"
