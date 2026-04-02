from pyspark.sql import DataFrame
from pyspark.sql.functions import col, abs as spark_abs
from src.config.logger import get_logger

logger = get_logger("gold_sentiment_lead_lag_validation")

class GoldSentimentLeadLagValidator:

    @staticmethod
    def validate_domain_rules(df: DataFrame) -> None:
        logger.info("Starting domain rules validation (nulls, ranges, extreme values) in gold_sentiment_vs_returns")

        if df.filter(col("article_count") < 1).head(1):
            raise ValueError("Column article_count constains 0")

        if df.filter((col("avg_sentiment_score") < -1) | (col("avg_sentiment_score") > 1)).head(1):
            raise ValueError("avg_sentimen_score contains out of range values")

        if df.filter(
            col("return_t1").isNull() |
            col("return_t2").isNull() |
            col("return_t5").isNull()
        ).head(1):
            raise ValueError("Columns with returns values contain nulls")

        if df.filter(spark_abs(col("return_t1")) > 0.5).head(1):
            logger.warning("return_t1 contains extreme values")

        if df.filter(spark_abs(col("return_t2")) > 0.6).head(1):
            logger.warning("return_t2 contains extreme values")
        
        if df.filter(spark_abs(col("return_t5")) > 0.8).head(1):
            logger.warning("return_t5 contains extreme values")

