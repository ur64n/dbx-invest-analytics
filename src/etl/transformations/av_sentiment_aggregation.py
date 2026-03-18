from pyspark.sql import DataFrame
from src.config.logger import get_logger
from pyspark.sql.functions import col, count, avg, sum, when, lit

logger = get_logger("av_sentiment_aggregation")

class AVSentimentAggregator:

    @staticmethod
    def aggregate_daily(df: DataFrame) -> DataFrame:
        logger.info("Starting aggregation daily sentiment per symbol: counts articles, averages scores, and computes sentiment label distribution")

        return df.groupBy("symbol","date").agg(
                count("*").alias("article_count"),
                avg("ticker_sentiment_score").alias("avg_sentiment_score"),
                avg("ticker_relevance_score").alias("avg_relevance_score"),
                sum(when(col("ticker_sentiment_label") == "bullish", 1).otherwise(0)).alias("bullish_count"),
                sum(when(col("ticker_sentiment_label") == "somewhat-bullish", 1).otherwise(0)).alias("somewhat_bullish_count"),
                sum(when(col("ticker_sentiment_label") == "neutral", 1).otherwise(0)).alias("neutral_count"),
                sum(when(col("ticker_sentiment_label") == "somewhat-bearish", 1).otherwise(0)).alias("somewhat_bearish_count"),
                sum(when(col("ticker_sentiment_label") == "bearish", 1).otherwise(0)).alias("bearish_count"),
                lit("alpha_vantage").alias("source_api")
            )
        
        logger.info("Finished aggregation: daily sentiment per symbol and date")