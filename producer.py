# pip install pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadKafkaHDFS") \
    .getOrCreate()

# Чтение всех файлов топика из HDFS
df = spark.read.json("hdfs://hdfs-nn:9000/data/kafka/topics/source.goods-filtered/*/*.json")

df.printSchema()
df.show(5, truncate=False)