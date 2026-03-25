import pytest
from src.etl.transformations.sentiment_sector_aggregation import SentimentSectorAggregator
from datetime import date

def test_aggregate_daily_sentiment_per_sector():

    df = spark.createDataFrame(
        [
            ("technology", "semiconductors", date(2024, 1, 1), 6, 0.5, 0.5, 3, 1, 1, 1),
            ("technology", "semiconductors", date(2024, 1, 1), 4, 0.8, 0.6, 2, 1, 0, 0),
        ],
        [
            "sector", "industry", "date", "article_count",
            "avg_sentiment_score", "avg_relevance_score",
            "bullish_count", "somewhat_bullish_count",
            "somewhat_bearish_count", "bearish_count",
        ]
    )

    result = SentimentSectorAggregator.aggregate_daily_sentiment_per_sector(df).collect()

    assert result[0].article_count == 10
    assert result[0].avg_sentiment_score == pytest.approx((0.5 * 6 + 0.8 * 4) / 10)
    assert result[0].avg_relevance_score == pytest.approx((0.5 * 6 + 0.6 * 4) / 10)
    assert result[0].bullish_bearish_ratio == pytest.approx((5 + 2 - 1 - 1) / 10)