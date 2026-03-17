from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DoubleType,
    IntegerType,
    TimestampType,
)

# ---------- bronze (per-article, per-ticker grain) ----------

av_sentiment_bronze_schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("published_at", TimestampType(), False),
    StructField("source", StringType(), True),
    StructField("title", StringType(), True),
    StructField("ticker_relevance_score", DoubleType(), True),
    StructField("ticker_sentiment_score", DoubleType(), True),
    StructField("ticker_sentiment_label", StringType(), True),
    StructField("article_overall_sentiment_score", DoubleType(), True),
    StructField("article_overall_sentiment_label", StringType(), True),
])

BRONZE_REQUIRED_COLUMNS = {
    "symbol",
    "published_at",
    "ticker_sentiment_score",
    "ticker_sentiment_label",
}

ALLOWED_VALUES = {
    "bullish",
    "somewhat-bullish",
    "neutral",
    "somewhat-bearish",
    "bearish"
}

# ---------- silver (daily aggregation per ticker) ----------

av_sentiment_silver_schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("date", DateType(), False),
    StructField("avg_sentiment_score", DoubleType(), True),
    StructField("article_count", IntegerType(), True),
    StructField("avg_relevance_score", DoubleType(), True),
    StructField("bullish_count", IntegerType(), True),
    StructField("bearish_count", IntegerType(), True),
    StructField("neutral_count", IntegerType(), True),
    StructField("source_api", StringType(), False),
])

SILVER_REQUIRED_COLUMNS = {
    "symbol",
    "date",
    "avg_sentiment_score",
    "article_count",
    "source_api",
}