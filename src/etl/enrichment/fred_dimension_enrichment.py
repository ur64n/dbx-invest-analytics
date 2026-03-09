from pyspark.sql import DataFrame
from src.config.logger import get_logger

logger = get_logger("fred_dimension_enrichment")

class FredDimensionEnricher:

    @staticmethod
    def enrich_fred_dimension(fact_df: DataFrame, dim_df: DataFrame) -> DataFrame:
        logger.info("Start enriching FRED indicators by frequency and unit metadata")

        return (
            fact_df
            .join(dim_df, on="indicator_id", how="left")
        )