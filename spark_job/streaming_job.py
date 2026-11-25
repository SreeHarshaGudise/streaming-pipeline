import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from transforms import parse_events

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "user-events")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "user-events-dlq")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/data/checkpoints/events")
DELTA_PATH = os.getenv("DELTA_PATH", "/data/delta/events_silver")

spark = (
    SparkSession.builder.appName("kafka-events-stream")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")  # first run only; the checkpoint decides after that
    .option("maxOffsetsPerTrigger", 10000)
    .option("failOnDataLoss", "false")
    .load()
    .select(F.col("value").cast("string").alias("raw"), "partition", "offset")
)

events = parse_events(raw)


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return
    batch_df.persist()
    valid = None
    try:
        invalid = batch_df.filter(F.col("reason").isNotNull())
        valid = (
            batch_df.filter(F.col("reason").isNull())
            .select("data.*")
            .withColumn("event_time", F.col("timestamp").cast("timestamp"))
            .dropDuplicates(["event_id"])  # in-batch dedupe
        ).persist()

        # 1) Delta Lake: idempotent append. If this batch is replayed after a crash,
        #    Delta sees the same (txnAppId, txnVersion) and skips the write.
        (
            valid.write.format("delta")
            .mode("append")
            .option("txnAppId", "events_silver")
            .option("txnVersion", batch_id)
            .save(DELTA_PATH)
        )

        # 2) Dead-letter topic
        n_valid, n_invalid = valid.count(), invalid.count()
        if n_invalid:
            (
                invalid.select(
                    F.to_json(
                        F.struct(
                            "raw",
                            "reason",
                            F.col("partition").alias("source_partition"),
                            F.col("offset").alias("source_offset"),
                            F.current_timestamp().alias("failed_at"),
                        )
                    ).alias("value")
                )
                .write.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("topic", DLQ_TOPIC)
                .save()
            )
        print(f"batch {batch_id}: valid={n_valid} invalid={n_invalid}", flush=True)
    finally:
        if valid is not None:
            valid.unpersist()
        batch_df.unpersist()


query = (
    events.writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="2 seconds")
    .start()
)
query.awaitTermination()
