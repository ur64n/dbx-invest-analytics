from pyspark.sql.types import StructType, StructField, StringType

gdelt_schema = StructType([
    StructField("article_id", StringType(), False),
    StructField("title", StringType(), True),
    StructField("url", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("source_country", StringType(), True),
    StructField("language", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("search_keyword", StringType(), True),
])
