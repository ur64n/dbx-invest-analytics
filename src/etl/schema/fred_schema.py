from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType

fred_schema = StructType([
    StructField("indicator_id", StringType(), False),
    StructField("date", DateType(), False),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("frequency", StringType(), True),
])
