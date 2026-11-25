"""Pure DataFrame logic (no I/O) so it can be unit-tested."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

VALID_EVENT_TYPES = ["click", "view", "purchase", "add_to_cart", "logout"]

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("user_id", IntegerType()),
        StructField("event_type", StringType()),
        StructField("amount", DoubleType()),
        StructField("timestamp", DoubleType()),  # epoch seconds
    ]
)


def parse_events(df: DataFrame) -> DataFrame:
    """Parse and validate. `df` needs a string column `raw`.

    Adds `data` (parsed struct) and `reason`: NULL when valid, else why it was rejected.
    """
    data = F.col("data")
    reason = (
        F.when(data.isNull(), "malformed_json")
        .when(
            data["event_id"].isNull()
            | data["user_id"].isNull()
            | data["event_type"].isNull()
            | data["timestamp"].isNull(),
            "missing_or_invalid_field",
        )
        .when(~data["event_type"].isin(*VALID_EVENT_TYPES), "unknown_event_type")
        .when(data["user_id"] <= 0, "invalid_user_id")
        .when(data["timestamp"] <= 0, "invalid_timestamp")
    )  # no .otherwise(): NULL means "valid"
    return df.withColumn("data", F.from_json(F.col("raw"), EVENT_SCHEMA)).withColumn("reason", reason)
