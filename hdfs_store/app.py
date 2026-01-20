import os
import json
from confluent_kafka import Consumer
from hdfs import InsecureClient
import uuid


brokers = "localhost:9097, localhost:9096"
ca_location = "../certs/kafka-cert.crt"
hdfs_client = InsecureClient("http://localhost:9870", user="root")
if os.path.exists("/.dockerenv"):
    print("Starting with docker")
    brokers = "kafka-mirror-0:9092, kafka-mirror-1:9092"
    ca_location = "/etc/kafka/secrets/kafka-cert.crt"
    hdfs_client = InsecureClient("http://hdfs-nn:9870", user="root")

user = "consumer"
password = "consumer_password"
topic = "source.goods-filtered"


def deserialize_jsonsr(data: bytes):
    if data[0] != 0:
        raise ValueError("Not Confluent Wire Format")
    json_bytes = data[5:]
    return json.loads(json_bytes.decode("utf-8"))


config = {
    "bootstrap.servers": brokers,
    "group.id": "hdfs_app",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    "session.timeout.ms": 6_000,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": user,
    "sasl.password": password,
    "ssl.ca.location": ca_location,
}


consumer = Consumer(config)
consumer.subscribe([topic])

try:
    while True:
        msg = consumer.poll(0.1)

        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        value = deserialize_jsonsr(msg.value())
        print(f"Got message: {value=}")

        # Writing
        hdfs_file = f"data/message_{uuid.uuid4()}"
        with hdfs_client.write(hdfs_file, encoding="utf-8") as writer:
            writer.write(json.dumps(value, ensure_ascii=False) + "\n")
        print(f"Message '{value=}' written to HDFS '{hdfs_file}'")

        # Test reading
        with hdfs_client.read(hdfs_file, encoding="utf-8") as reader:
            content = reader.read()
        print(
            f"Reading file '{hdfs_file}' from HDFS. Content: '{content.strip()}'"
        )
finally:
    consumer.close()
