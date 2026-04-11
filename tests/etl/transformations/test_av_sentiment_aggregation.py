import pytest
from src.etl.transformations.av_sentiment_aggregation import AVSentimentAggregator
from datetime import date

def test_aggregate_daily():

    df = spark.createDataFrame(
        [
            ("aapl", date(2024, 1, 1), 0.5, 0.8, "bullish"),
            ("aapl", date(2024, 1, 1), 0.0, 0.3, "neutral"),
            ("aapl", date(2024, 1, 1), -0.5, 0.4, "bearish")
        ],
        [
            "symbol", "date", "ticker_sentiment_score", "ticker_relevance_score", "ticker_sentiment_label"
        ]
    )

    result = AVSentimentAggregator.aggregate_daily(df).collect()

    assert result[0].avg_sentiment_score == 0.0
    assert result[0].avg_relevance_score == 0.5
    assert result[0].article_count == 3
    assert result[0].bullish_count == 1
    assert result[0].bearish_count == 1
    assert result[0].neutral_count == 1
    assert result[0].source_api == "alpha_vantage"