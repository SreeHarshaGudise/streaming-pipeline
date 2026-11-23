import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from transforms import EVENT_SCHEMA

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "user-events")

spark = (
    SparkSession.builder.appName("kafka-events-stream")
    # default is 200 shuffle partitions, far too many for tiny micro-batches
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

events = (
    raw.select(F.col("value").cast("string").alias("raw"), "partition", "offset")
    .withColumn("data", F.from_json("raw", EVENT_SCHEMA))
    .select("partition", "offset", "data.*")
)

query = (
    events.writeStream.format("console")
    .option("truncate", "false")
    .option("numRows", 5)
    .trigger(processingTime="2 seconds")
    .start()
)
query.awaitTermination()
