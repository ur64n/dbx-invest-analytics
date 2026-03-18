from pyspark.sql import DataFrame
from src.config.logger import get_logger

logger = get_logger("gold_sentiment_validation")

class GoldAVSentimentValidator:

    @staticmethod
    def validate_emptiness(df: DataFrame) -> None:
        return None