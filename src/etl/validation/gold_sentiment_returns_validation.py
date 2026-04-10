from pyspark.sql import DataFrame
from pyspark.sql.functions import col, abs as spark_abs
from src.config.logger import get_logger


logger = get_logger("gold_sentiment_returns_validation")

class GoldSentimentReturnValidator:
    """Validates sentiment vs returns, article count > 0, score range, extreme daily return warnings."""

    @staticmethod
    def validate_domain_rules(df: DataFrame) -> None:
        logger.info("Starting domain rules validation (nulls, ranges, extreme values) in gold_sentiment_vs_returns")

        if df.filter(col("article_count") < 1).head(1):
            raise ValueError("Column article_count constains 0")

        if df.filter((col("avg_sentiment_score") < -1) | (col("avg_sentiment_score") > 1)).head(1):
            raise ValueError("avg_sentimen_score contains out of range values")

        if df.filter(spark_abs(col("daily_return")) > 0.5).head(1):
            logger.warning("The values in the daily_return column exceed extreme differences")