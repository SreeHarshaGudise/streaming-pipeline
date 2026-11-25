from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("check-delta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
df = spark.read.format("delta").load("/data/delta/events_silver")
print("rows:", df.count(), "distinct event_ids:", df.select("event_id").distinct().count())
df.orderBy("timestamp", ascending=False).show(5, truncate=False)
