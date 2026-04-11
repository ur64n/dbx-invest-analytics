from pyspark.sql import DataFrame
from src.config.logger import get_logger

logger = get_logger("sentiment_return_enrichment")

class SentimentReturnEnricher:
    """Joins OHLCV daily returns with aggregated daily sentiment on (symbol, date)."""

    @staticmethod
    def enrich_sentiment_return(ohlcv_daily_return_df: DataFrame, sentiment_df: DataFrame) -> DataFrame:
        logger.info("Starting enrichment ohlcv daily return dataset with aggregated daily sentiment data")

        enriched_df = ohlcv_daily_return_df.join(sentiment_df, on=["symbol", "date"], how="left")

        logger.info("Enrichment complete")

        return enriched_df