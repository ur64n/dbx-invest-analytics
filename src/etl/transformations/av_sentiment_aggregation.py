from pyspark.sql import DataFrame
from src.config.logger import get_logger

logger = get_logger("av_sentiment_aggregation")

class AVSentimentAggregator:

    @staticmethod
    def 