"""Pure DataFrame logic (no I/O) so it can be unit-tested."""
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("user_id", IntegerType()),
        StructField("event_type", StringType()),
        StructField("amount", DoubleType()),
        StructField("timestamp", DoubleType()),  # epoch seconds
    ]
)
