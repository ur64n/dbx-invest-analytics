from pyspark.sql.types import StructType, StructField, StringType

fred_metadata_schema = StructType([
    StructField("indicator_id", StringType(), False),
    StructField("unit", StringType(), False),
    StructField("frequency", StringType(), False),
])

METADATA_REQUIRED_COLUMNS = {
    "indicator_id",
    "unit",
    "frequency",
}

KEY_METADATA_COLUMNS = {
    "indicator_id"
}