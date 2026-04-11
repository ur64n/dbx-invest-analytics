from pyspark.sql.types import StructType, StructField, StringType, DoubleType

QQQ_SCHEMA = StructType([
    StructField("Symbol", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("% Holding", StringType(), True),
    StructField("Shares", StringType(), True)
])

REQUIRED_RAW_COLUMNS = {
    "Symbol",
    "Name",
    "percent_holding"
}

REQUIRED_SILVER_COLUMNS = {
    "symbol",
    "name",
    "percent_holding"
}

NOT_NULL_COLUMNS = [
    "symbol",
    "name",
    "percent_holding"
]