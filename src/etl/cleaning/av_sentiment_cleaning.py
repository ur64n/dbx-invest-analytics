from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    to_date
)
from src.config.logger import get_logger

logger = get_logger("av_sentiment_cleaning")

class AVSentimentCleaner:

    @staticmethod
    def standardize_columns(df: DataFrame) -> DataFrame:
        logger.info("Standardizing AV Sentiment dataset columns")

        df = (
            df.withColumnRenamed("sentiment_label", "ticker_sentiment_label")
            .withColumnRenamed("sentiment_score", "ticker_sentiment_score")
            .withColumnRenamed("relevance_score", "ticker_relevance_score")
            .withColumnRenamed("overall_sentiment_score", "article_sentiment_score")
            .withColumnRenamed("overall_sentiment_label", "article_sentiment_label")
        )

        df = (
            df.withColumn("symbol", lower(trim(col("symbol"))))
            .withColumn("source",lower(trim(col("source"))))
            .withColumn("date", to_date(col("published_at")))
            .withColumn("ticker_sentiment_label", lower(trim(col("ticker_sentiment_label"))))
            .withColumn("article_sentiment_label", lower(trim(col("article_sentiment_label"))))
        )

        logger.info("Standardizing AV Sentiment dataset columns finished")

        return df