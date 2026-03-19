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
    def drop_duplicates(df: DataFrame, keys: list[str]) -> DataFrame:
        logger.info(f"Starting drop duplicates on keys columns")
    
        before = df.count()
        df = df.dropDuplicates(keys)
        after = df.count()
    
        if before != after:
            logger.warning(f"Removed {before - after} duplicate rows")
    
        return df
    
    @staticmethod
    def standardize_columns(df: DataFrame) -> DataFrame:
        logger.info("Start cleaning whitespaces and adjusting cannonical form")
        
        df = (
            df.withColumn("symbol", lower(trim(col("symbol"))))
            .withColumn("source",lower(trim(col("source"))))
            .withColumn("date", to_date(col("published_at")))
            .withColumn("ticker_sentiment_label", lower(trim(col("ticker_sentiment_label"))))
            .withColumn("article_overall_sentiment_label", lower(trim(col("article_overall_sentiment_label"))))
        )

        logger.info("Standardizing AV Sentiment dataset columns finished")

        return df