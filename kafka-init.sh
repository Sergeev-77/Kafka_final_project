#!/bin/bash

# KAFKA TOPICS
set -e
echo 'Waiting for Kafka to be ready...'

# Waiting for kafka
while ! kafka-broker-api-versions --bootstrap-server kafka-0:9092 --command-config /etc/kafka/config/admin.properties 2>/dev/null; do
  echo 'Kafka not ready yet. Waiting...'
  sleep 5
done

# Waiting for kafka-mirror
while ! kafka-broker-api-versions --bootstrap-server kafka-mirror-0:9092 --command-config /etc/kafka/config/admin.properties 2>/dev/null; do
  echo 'Kafka not ready yet. Waiting...'
  sleep 5
done

# topics creation
TOPICS=(
    "goods=2=2=compact=kafka-0:9092"
    "goods-filtered=2=2=compact=kafka-0:9092"
    "goods-recomendations=2=2=compact=kafka-mirror-0:9092"    
    "events=2=2=compact=kafka-mirror-0:9092"    
)

for topic in "${TOPICS[@]}"; do
    name=$(echo "$topic" | cut -d= -f1)
    partitions=$(echo "$topic" | cut -d= -f2)
    replication=$(echo "$topic" | cut -d= -f3)
    policy=$(echo "$topic" | cut -d= -f4)
    broker=$(echo "$topic" | cut -d= -f5)

    if ! kafka-topics --bootstrap-server "$broker" --list --command-config /etc/kafka/config/admin.properties | grep -q "^${name}$"; then
      echo "Creating topic $name..."
      kafka-topics --bootstrap-server "$broker" --create --topic "$name" --partitions  "$partitions" --replication-factor "$replication" --command-config /etc/kafka/config/admin.properties --config cleanup.policy="$policy"
    else
      echo "Topic $name already exists"
    fi
done

# MM2-CONNECTOR
echo 'Waiting for Kafka-connect to be ready...'

# Wait for Kafka Connect
until curl -s http://kafka-connect:8083/connectors > /dev/null; do
  echo 'Kafka-connect not ready yet. Waiting 5 seconds...'
  sleep 5
done

# Deploy mm2
echo 'Deploying MM2 connector...'
sleep 5
curl -X POST http://kafka-connect:8083/connectors -H 'Content-Type: application/json' -d @/opt/connect-config/mm2-source.json
echo

if ! kafka-topics --bootstrap-server kafka-mirror-0:9092 --list --command-config /etc/kafka/config/admin.properties; then
  echo "Wating for topic mirroring"
fi


# KAFKA ACl's
echo "Starting for ACL"
sleep 5
kafka-acls --bootstrap-server kafka-0:9092 --add --allow-principal User:producer --operation Write --topic goods --command-config /etc/kafka/config/admin.properties
kafka-acls --bootstrap-server kafka-0:9092 --add --allow-principal User:blocker --operation Read --topic goods --command-config /etc/kafka/config/admin.properties
kafka-acls --bootstrap-server kafka-0:9092 --add --allow-principal User:blocker --operation All  --group blocking_app --command-config /etc/kafka/config/admin.properties
kafka-acls --bootstrap-server kafka-0:9092 --add --allow-principal User:blocker --operation Write --topic goods-filtered --command-config /etc/kafka/config/admin.properties
kafka-acls --bootstrap-server kafka-0:9092 --add --allow-principal User:consumer --operation Read --topic goods-filtered --command-config /etc/kafka/config/admin.properties
kafka-acls --bootstrap-server kafka-mirror-0:9092 --add --allow-principal User:consumer --operation Read --topic goods-recomendations --command-config /etc/kafka/config/admin.properties
kafka-acls --bootstrap-server kafka-mirror-0:9092 --add --allow-principal User:producer --operation Write --topic goods-recomendations --command-config /etc/kafka/config/admin.properties
kafka-acls --bootstrap-server kafka-mirror-0:9092 --add --allow-principal User:producer --operation Write --topic events --command-config /etc/kafka/config/admin.properties
kafka-acls --bootstrap-server kafka-mirror-0:9092 --add --allow-principal User:consumer --operation All  --group recomendation_reader --command-config /etc/kafka/config/admin.properties

sleep 5

# HDFS-sink connector
echo 'Deploying HDFS-sink connector...'
sleep 5
curl -X POST http://kafka-connect:8083/connectors -H 'Content-Type: application/json' -d @/opt/connect-config/hdfs-sink.json
echo
echo 'Initialization complete.'
