from pyspark.sql.types import StructType, StructField, StringType

QQQ_CATEGORIES_SCHEMA = StructType([
        StructField("symbol", StringType(), True),
        StructField("sector", StringType(), True),
        StructField("industry", StringType(), True),
    ])


REQUIRED_COLUMNS = {
    "symbol",
    "sector",
    "industry"
}

KEY_COLUMNS = {
    "symbol"
}