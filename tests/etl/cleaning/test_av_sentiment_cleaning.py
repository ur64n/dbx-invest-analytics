import pytest
from src.etl.cleaning.av_sentiment_cleaning import AVSentimentCleaner
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime, date

# ---------- happy path ----------
def test_standardize_columns():

    schema = StructType([
        StructField("symbol", StringType()),
        StructField("published_at", TimestampType()),
        StructField("source", StringType()),
        StructField("ticker_sentiment_label", StringType()),
        StructField("article_overall_sentiment_label", StringType())
    ])

    df = spark.createDataFrame(
        [
            (" AAPL ", datetime(2026, 3, 17, 11, 13, 0), "MarketBeat", "Neutral", "Somewhat-Bullish")
        ],
        schema=schema
    )

    result = AVSentimentCleaner.standardize_columns(df).collect()

    assert result[0].symbol == "aapl"
    assert result[0].date == date(2026, 3, 17)
    assert result[0].source == "marketbeat"
    assert result[0].ticker_sentiment_label == "neutral"
    assert result[0].article_overall_sentiment_label == "somewhat-bullish"

