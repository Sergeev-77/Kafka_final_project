import json
from datetime import datetime
from confluent_kafka import Producer

from datetime import datetime

brokers = "localhost:9096, localhost:9097"
ca_location = "../certs/kafka-cert.crt"


user = "producer"
password = "producer_password"
topic = "events"

config = {
    "bootstrap.servers": brokers,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": user,
    "sasl.password": password,
    "ssl.ca.location": ca_location,
}


producer = Producer(config)


def report(error, message):
    if error:
        print(f"Delivery failed: {error}")
        return
    # print(f"Delivered to {message.topic()} ={message.partition()}=")


def send_event_to_topic(command="", type="", value=""):
    event = {
        "command": command,
        "type": type,
        "value": value,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    producer.produce(
        topic=topic,
        key=event["timestamp"],
        value=json.dumps(event).encode("utf-8"),
        on_delivery=report,
    )
    producer.flush()
    print(event)
