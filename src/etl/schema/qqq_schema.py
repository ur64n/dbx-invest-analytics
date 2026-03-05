from pyspark.sql.types import StructType, StructField, StringType, DoubleType

qqq_schema = StructType([
    StructField("Symbol", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("% Holding", StringType(), True),
    StructField("Shares", StringType(), True)
])

required_columns = {
    "Symbol",
    "Name",
    "% Holding"
}

not_null_columns = [
    "symbol",
    "name",
    "percent_holding"
]