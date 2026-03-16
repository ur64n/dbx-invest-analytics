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
    def 