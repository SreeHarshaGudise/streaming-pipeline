import os

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from transforms import parse_events

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "user-events")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "user-events-dlq")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/data/checkpoints/events")
DELTA_PATH = os.getenv("DELTA_PATH", "/data/delta/events_silver")
WINDOW_SECONDS = 10  # keep in sync with the /10.0 in the Grafana events/sec query

PG = {
    "host": os.getenv("PG_HOST", "postgres"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "events_db"),
    "user": os.getenv("POSTGRES_USER", "pipeline"),
    "password": os.getenv("POSTGRES_PASSWORD", "pipeline"),
}

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


def write_postgres(batch_id, rows, valid_count, invalid_count, avg_ms, max_ms):
    """One transaction: the batch_metrics row is the idempotency ledger."""
    conn = psycopg2.connect(**PG)
    try:
        with conn:  # commit on success, rollback on any error
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO batch_metrics
                         (batch_id, valid_count, invalid_count, avg_latency_ms, max_latency_ms)
                       VALUES (%s, %s, %s, %s, %s)
                       ON CONFLICT (batch_id) DO NOTHING""",
                    (batch_id, valid_count, invalid_count, avg_ms, max_ms),
                )
                if cur.rowcount == 0:
                    print(f"batch {batch_id}: already applied to Postgres, skipping", flush=True)
                    return
                if rows:
                    execute_values(
                        cur,
                        """INSERT INTO event_agg (window_start, event_type, event_count)
                           VALUES %s
                           ON CONFLICT (window_start, event_type) DO UPDATE
                           SET event_count = event_agg.event_count + EXCLUDED.event_count""",
                        rows,
                    )
    finally:
        conn.close()


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

        # 1) Delta Lake: idempotent append (txnAppId + txnVersion)
        (
            valid.write.format("delta")
            .mode("append")
            .option("txnAppId", "events_silver")
            .option("txnVersion", batch_id)
            .save(DELTA_PATH)
        )

        # 2) Dead-letter topic (at-least-once)
        invalid_count = invalid.count()
        if invalid_count:
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

        # 3) Postgres: window counts + latency, transactional and idempotent.
        #    Results are tiny, so collecting them to the driver is fine.
        latency_ms = (F.current_timestamp().cast("double") - F.col("timestamp")) * 1000
        stats = valid.agg(
            F.count("*").alias("n"),
            F.avg(latency_ms).alias("avg_ms"),
            F.max(latency_ms).alias("max_ms"),
        ).first()
        agg_rows = (
            valid.groupBy(
                F.window("event_time", f"{WINDOW_SECONDS} seconds").alias("w"), "event_type"
            )
            .count()
            .select(F.col("w.start").alias("window_start"), "event_type", F.col("count"))
            .collect()
        )
        write_postgres(
            batch_id,
            [(r["window_start"], r["event_type"], r["count"]) for r in agg_rows],
            stats["n"],
            invalid_count,
            stats["avg_ms"],
            stats["max_ms"],
        )
        avg = stats["avg_ms"]
        print(
            f"batch {batch_id}: valid={stats['n']} invalid={invalid_count} "
            f"avg_latency_ms={avg:.0f}" if avg is not None else f"batch {batch_id}: no valid rows",
            flush=True,
        )
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
