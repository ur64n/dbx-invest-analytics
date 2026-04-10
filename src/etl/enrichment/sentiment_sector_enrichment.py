from pyspark.sql import DataFrame
from src.config.logger import get_logger

logger = get_logger("sentiment_sector_enrichment")

class SentimentSectorEnricher:
    """Joins aggregated sentiment data with sector/industry dimension on symbol."""

    @staticmethod
    def enrich_sector_sentiment(sector_df: DataFrame, sentiment_df: DataFrame) -> DataFrame:
        logger.info("Starting enrichment aggregated sentiment data with sector and industry")

        enriched_df = sentiment_df.join(sector_df, on="symbol", how="left")
        
        logger.info("Enriching complete")

        return enriched_df