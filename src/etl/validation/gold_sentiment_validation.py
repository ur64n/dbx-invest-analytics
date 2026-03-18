from pyspark.sql import DataFrame
from src.config.logger import get_logger

from pyspark.sql.functions import col

logger = get_logger("gold_sentiment_validation")

class GoldAVSentimentValidator:

    @staticmethod
    def validate_uniqueness(df: DataFrame) -> None:
        logger.info("Starting uniqueness validation on (symbol+date) columns")
        
        total = df.count()
        unique_rows = df.select("symbol","date").distinct().count()

        if total != unique_rows:
            raise ValueError("Dataset contains duplicated values")

        logger.info("Complete, all rows are unique")

    @staticmethod
    def validate_domain_rules(df: DataFrame) -> None:

        logger.info("Validating all symbols contain articles data")

        if df.filter(col("article_count") <= 0).limit(1).count() > 0:
            raise ValueError("Some symbols have no articles sentiment data")

        logger.info("Validating avg_sentiment_score correct value range")

        if df.filter((col("avg_sentiment_score") > 1) | (col("avg_sentiment_score") < -1)).head(1):
            raise ValueError("Values avg_sentiment_score is out of range")

        logger.info("Validating avg_relevance_score correct value range")

        if df.filter((col("avg_relevance_score") > 1) | (col("avg_relevance_score") < 0)).head(1):
            raise ValueError("Values avg_relevance_score is out of range")

        logger.info("Validating non negative values in count columns")

        if df.filter(
            (col("bullish_count") < 0) |
            (col("somewhat_bullish_count") < 0) |
            (col("neutral_count") < 0) |
            (col("somewhat_bearish_count") < 0) |
            (col("bearish_count") < 0)
        ).limit(1).count() > 0:
            raise ValueError("Some of count columns contain negative value")

        

