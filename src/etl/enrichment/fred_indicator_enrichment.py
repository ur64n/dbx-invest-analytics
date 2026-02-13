from pyspark.sql import DataFrame
from src.config.logger import get_logger

logger = get_logger("fred-enrichment")

class FredIndicatorEnricher:

    @staticmethod
    def enrich(indicator_df: DataFrame, metadata_df: DataFrame) -> DataFrame:
        logger.info("Start enriching Fred indicators")
        
        return(
            indicator_df
            .join(metadata_df, on="indicator_id", how="left")
        )