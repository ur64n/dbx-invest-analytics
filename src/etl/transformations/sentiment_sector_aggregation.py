from pyspark.sql import DataFrame
from src.config.logger import get_logger
from pyspark.sql.functions import col, count, avg, sum

logger = get_logger("sentiment_sector_aggregation")

class SentimentSectorAggregator:
    
    @staticmethod
    def aggregate_daily_sentiment_per_sector(df: DataFrame) -> DataFrame:
        logger.info("Starting aggregation daily sentiment data per sector")

        df = df.groupBy("sector", "industry", "date").agg(
            sum("article_count").alias("article_count"),

            (sum(col("avg_sentiment_score") * col("article_count"))
             / sum(col("article_count"))).alias("avg_sentiment_score"),

            (sum(col("avg_relevance_score") * col("article_count"))
             / sum(col("article_count"))).alias("avg_relevance_score"),

            ((sum(col("bullish_count")) + sum(col("somewhat_bullish_count"))
              - sum(col("bearish_count")) - sum(col("somewhat_bearish_count")))
             / sum(col("article_count"))).alias("bullish_bearish_ratio")
            )

        return df
        