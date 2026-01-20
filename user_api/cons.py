from pprint import pprint
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer

bootstrap_servers = "localhost:9096, localhost:9097"
username, password = "consumer", "consumer_password"
group_id = "recomendation_reader"
ssl_ca_location = "../certs/kafka-cert.crt"

conf = {
    "bootstrap.servers": bootstrap_servers,
    "group.id": group_id,
    "auto.offset.reset": "latest",
    "enable.auto.commit": False,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": username,
    "sasl.password": password,
    "ssl.ca.location": ssl_ca_location,
}
consumer = Consumer(conf)
topic = "goods-recomendations"
schema_registry_url = "http://localhost:8081"
sr_client = SchemaRegistryClient({"url": schema_registry_url})
subject = f"{topic}-value"
schema_info = sr_client.get_latest_version(subject)
schema_str = schema_info.schema.schema_str
deserializer = JSONDeserializer(schema_str, schema_registry_client=sr_client)

consumer.subscribe([topic])


def stream_recommendations():
    messages = []
    try:
        while True:
            msg = consumer.poll(timeout=10.0)
            if msg is None:
                print("No new mesages, Ctrl+C for exit or wait")
                continue
            if msg and msg.error():
                print(f"Error: {msg.error()}")
                break

            partition, offset = msg.partition(), msg.offset()
            value = deserializer(
                msg.value(), SerializationContext(topic, MessageField.VALUE)
            )
            messages.append(value)
            print("=" * 40)
            print(f"======New recomendation [{partition}:{offset}] =========")
            print("=" * 40)
            pprint(value)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    print(f"\nThere was {len(messages)} messages")
