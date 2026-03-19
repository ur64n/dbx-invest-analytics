
SOURCE_SENTIMENT_COLUMNS = [
    "symbol", "date", "article_count", "avg_sentiment_score", "avg_relevance_score", "bullish_count", "somewhat_bullish_count", "neutral_count", "somewhat_bearish_count", "bearish_count"
]

SOURCE_ENTITIES_COLUMNS = [
    "symbol", 
    "sector",
    "industry"
]

REQUIRED_COLUMNS = {
    "sector",
    "date",
    "article_count",
    "avg_sentiment_score",
    "avg_relevance_score",
    "bullish_bearish_ratio"
}

