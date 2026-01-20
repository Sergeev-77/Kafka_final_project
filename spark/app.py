from pyspark.sql import SparkSession
from pyspark.errors import AnalysisException
import time
from py4j.protocol import Py4JJavaError
import os
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import Schema

spark = (
    SparkSession.builder.appName("ReadKafkaHDFS")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

dir_path = (
    "hdfs://hdfs-nn:9000/data/kafka/topics/source.goods-filtered/*/*.json"
)

check_point_path = "/app/checkpoints/recomendations"

os.makedirs(check_point_path, exist_ok=True)

kafka_bootstrap_servers = "kafka-mirror-0:9092"
kafka_topic = "goods-recomendations"
user = "producer"
password = "producer_password"

schema_registry_url = "http://schema-registry:8081"
schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})
subject = f"{kafka_topic}-value"
schema_info = schema_registry_client.get_latest_version(subject)
json_schema_str = schema_info.schema.schema_str
json_serializer = JSONSerializer(
    json_schema_str,
    schema_registry_client,
    conf={"auto.register.schemas": False},
)
key_serializer = StringSerializer("utf-8")


ca_location = "/etc/kafka/secrets/kafka-cert.crt"

kafka_options = {
    "bootstrap.servers": kafka_bootstrap_servers,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": user,
    "sasl.password": password,
    "ssl.ca.location": ca_location,
    "value.serializer": json_serializer,
    "key.serializer": key_serializer,
}

producer = SerializingProducer(kafka_options)


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()}[{msg.partition()}]")


# Wait for hdfs
while True:
    try:
        temp_df = spark.read.json(dir_path)
        if temp_df.count() > 0:
            print(f"Found {temp_df.count()} initial files. Starting streaming.")
            break
    except AnalysisException as e:
        if "PATH_NOT_FOUND" in str(e):
            print(f"Path not found: {dir_path}")
    else:
        raise e
    print("No files yet. Waiting 5 seconds...")
    time.sleep(5)


# get schema
df_static = spark.read.json(dir_path)
hdfs_schema = df_static.schema

# streaming
streaming_df = (
    spark.readStream.schema(hdfs_schema)
    .format("json")
    .option("maxFilesPerTrigger", 1)
    .load(dir_path)
)


# parse to dict
def row_to_dict(row):
    if isinstance(row, dict):
        return {k: row_to_dict(v) for k, v in row.items()}
    elif isinstance(row, list):
        return [row_to_dict(item) for item in row]
    elif hasattr(row, "asDict"):
        return {k: row_to_dict(v) for k, v in row.asDict().items()}
    else:
        return row


def process_batch(batch_df, batch_id):
    print(f"\n================ Incoming batch {batch_id} ================")
    batch_df.show(truncate=False)

    if batch_id % 10 == 0 and not batch_df.isEmpty():
        print("Sending batch to recommendations")
        rows = [row_to_dict(row) for row in batch_df.collect()]

        for row in rows:
            try:
                producer.produce(
                    topic=kafka_topic,
                    key=row.get("product_id", ""),
                    value=row,
                    on_delivery=delivery_report,
                )
            except Exception as e:
                print(f"Error serializing row {row}: {e}")

        producer.flush()
    else:
        print("Skipping batch")


# processing
query = (
    streaming_df.writeStream.outputMode("append")
    .foreachBatch(process_batch)
    .option("checkpointLocation", check_point_path)
    .start()
)

query.awaitTermination()
