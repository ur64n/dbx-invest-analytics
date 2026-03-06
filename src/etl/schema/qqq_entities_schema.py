from pyspark.sql.types import StructType, StructField, StringType, DoubleType

qqq_schema = StructType([
    StructField("Symbol", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("% Holding", StringType(), True),
    StructField("Shares", StringType(), True)
])

required_raw_columns = {
    "Symbol",
    "Name",
    "% Holding"
}

required_silver_columns = {
    "symbol",
    "name",
    "percent_holding"
}

not_null_columns = [
    "symbol",
    "name",
    "percent_holding"
]