import os
import json
import time
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import random
from datetime import datetime, timezone

brokers = "localhost:9094, localhost:9095"
schema_registry_url = "http://localhost:8081"
ca_location = "../certs/kafka-cert.crt"

if os.path.exists("/.dockerenv"):
    print("Starting with docker")
    brokers = "kafka-0:9092, kafka-1:90922"
    schema_registry_url = "http://schema-registry:8081"
    ca_location = "/etc/kafka/secrets/kafka-cert.crt"

user = "producer"
password = "producer_password"
topic = "goods"


def key_serializer(key, ctx):
    if key is None:
        return None
    return str(key).encode("utf-8")


config = {
    "bootstrap.servers": brokers,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": user,
    "sasl.password": password,
    "ssl.ca.location": ca_location,
    "key.serializer": key_serializer,
}


schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})


def register_schema(schema_subject) -> str:
    schema_object = Schema(
        schema_str=open("good.schema.json").read(), schema_type="JSON"
    )

    while True:
        try:
            subjects = schema_registry_client.get_subjects()
            print("Schema Registry is available, subjects:", subjects)
            break
        except Exception as e:
            print("Schema Registry is not available:", e)
            print("Wait...")
            time.sleep(5)

    try:
        latest = schema_registry_client.get_latest_version(schema_subject)
        print(f"Schema is already registered for {schema_subject}")
        return latest.schema.schema_str
    except Exception:
        schema_id = schema_registry_client.register_schema(
            schema_subject, schema_object
        )
        print(f"Registered schema for {schema_subject} with id: {schema_id}")
        return schema_registry_client.get_latest_version(
            schema_subject
        ).schema.schema_str


def get_id():
    return random.choice(
        [
            {"key": "12345", "name": "ABC"},
            {"key": "12346", "name": "DEF"},
            {"key": "12347", "name": "GHI"},
            {"key": "12348", "name": "JKL"},
            {"key": "12349", "name": "MNO"},
        ]
    )


if __name__ == "__main__":
    for schema_subject in [
        topic,
        topic + "-filtered",
        topic + "-recomendations",
        "source." + topic + "-filtered",
    ]:
        schema_str = register_schema(schema_subject + "-value")
    config["value.serializer"] = JSONSerializer(
        schema_str,
        schema_registry_client,
        conf={"auto.register.schemas": False},
    )
    producer = SerializingProducer(config)
    good = json.load(open("good.json", "r", encoding="utf-8"))
    initial_price = good["price"]["amount"]

    def report(error, message):
        if error:
            print(f"Delivery failed: {error}")
            return
        print(f"Delivered to {message.topic()} ={message.partition()}=")

    while True:
        # Random update for goods prices with generate id's
        good_instance = good.copy()
        id = get_id()
        good_instance["product_id"] = id["key"]
        good_instance["name"] += id["name"]
        good_instance["brand"] = "brand_" + id["name"]
        good_instance["updated_at"] = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        price = round(initial_price * random.uniform(0.5, 2.0), 2)
        good_instance["price"]["amount"] = price
        good_instance[
            "description"
        ] += f" за {price} {good_instance["price"]["currency"]}"
        producer.produce(
            topic=topic,
            key=good_instance["product_id"],
            value=good_instance,
            on_delivery=report,
        )
        producer.flush()
        time.sleep(5)
