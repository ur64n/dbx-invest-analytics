from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType

fred_schema = StructType([
    StructField("indicator_id", StringType(), False),
    StructField("date", DateType(), False),
    StructField("value", DoubleType(), True),
])

REQUIRED_COLUMNS = {
    "indicator_id",
    "date",
    "value",
}

KEY_COLUMNS = {
    "indicator_id",
    "date"
}