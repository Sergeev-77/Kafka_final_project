import sys
import faust
import ssl
import os


BLOCKED_IDS = []

brokers = "localhost:9094, localhost:9095, localhost:9096"
schema_registry_url = "http://localhost:8081"
ca_location = "../certs/kafka-cert.crt"

if os.path.exists("/.dockerenv"):
    print("Starting with docker")
    brokers = "kafka-0:9092, kafka-1:9092, kafka-2:9092"
    schema_registry_url = "http://schema-registry:8081"
    ca_location = "/etc/kafka/secrets/kafka-cert.crt"

assert os.path.exists(ca_location)
ssl_context = ssl.create_default_context(cafile=ca_location)
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_REQUIRED

username = "blocker"
password = "blocker_password"
sasl_mechanism = "PLAIN"

app = faust.App(
    "blocking_app",
    broker=brokers,
    broker_credentials=faust.SASLCredentials(
        ssl_context=ssl_context,
        mechanism=sasl_mechanism,
        username=username,
        password=password,
    ),
    topic_disable_leader=True,
    consumer_auto_offset_reset="earliest",
    debug=True,
    loglevel="debug",
)

print("Broker URLs:", app.conf.broker)
print("Broker credentials:", app.conf.broker_credentials)


def print_cnsl(*mess):
    print(*mess, file=sys.__stdout__)


@app.task
async def on_start(app):
    BLOCKED_IDS.extend(
        open("blocked.txt", "r", encoding="utf-8").read().splitlines()
    )
    print_cnsl("BLOCKED: ", BLOCKED_IDS)


raw_goods = app.topic("goods", value_serializer="raw")
goods_filered = app.topic("goods-filtered", value_serializer="json")


@app.agent(raw_goods)
async def filter_messages(stream):
    async for event in stream.events():
        key = event.message.key.decode()
        value = event.value
        if not key in BLOCKED_IDS:
            await goods_filered.send(key=key, value=value)
            print_cnsl("PROCESSED: ", key)
        else:
            print_cnsl("BLOCKED: ", key)
