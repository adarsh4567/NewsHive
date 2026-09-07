#!/bin/bash
# Wait for Kafka to be ready and create required topics

set -e

KAFKA_HOST=${KAFKA_HOST:-kafka}
KAFKA_PORT=${KAFKA_PORT:-9092}
BOOTSTRAP="${KAFKA_HOST}:${KAFKA_PORT}"

echo "Waiting for Kafka at ${BOOTSTRAP}..."
until kafka-topics --bootstrap-server "${BOOTSTRAP}" --list > /dev/null 2>&1; do
  echo "  Kafka not ready yet, sleeping 5s..."
  sleep 5
done
echo "Kafka is ready!"

create_topic() {
  local topic=$1
  local partitions=${2:-1}
  local replication=${3:-1}
  if kafka-topics --bootstrap-server "${BOOTSTRAP}" --list | grep -q "^${topic}$"; then
    echo "Topic '${topic}' already exists, skipping."
  else
    kafka-topics --bootstrap-server "${BOOTSTRAP}" \
      --create \
      --topic "${topic}" \
      --partitions "${partitions}" \
      --replication-factor "${replication}"
    echo "Created topic '${topic}'"
  fi
}

create_topic "user-events"       3 1
create_topic "output-signal"     1 1
create_topic "output-group-signals" 1 1
create_topic "processed-events"  1 1

echo "All Kafka topics created successfully!"
