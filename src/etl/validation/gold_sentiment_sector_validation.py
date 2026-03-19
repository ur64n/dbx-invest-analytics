from pyspark.sql import DataFrame
from src.config.logger import get_logger
from pyspark.sql.functions import col

logger = get_logger("gold_sentiment_sector_validation")

class GoldSentimentSectorValidator:

    @staticmethod
    def validate_negative_values(df: DataFrame) -> None:
        logger.info("Starting negative values validation in article_count column")

        if df.filter(col("article_count")) < 0:
            raise ValueError("column article_count contains negative values")

    @staticmethod
    def validate_value_ranges(df: DataFrame) -> None:
        logger.info("Starting validation range values in float columns")

        if df.filter((col("avg_sentiment_score") < -1) | (col("avg_sentiment_score") > 1)).head(1):
            raise ValueError("avg_sentiment_score column contains out of range value")
        
        if df.filter((col("avg_relevance_score") < 0) | (col("avg_relevance_score") > 1)).head(1):
            raise ValueError("avg_relevance_score column contains out of range value")
        
        
        if df.filter((col("bullish_bearish_ratio") < -1) | (col("bullish_bearish_ratio") > 1)).head(1):
            raise ValueError("bullish_bearish_ratio column contains out of range value")




