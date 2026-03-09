from pyspark.sql import DataFrame
from src.config.logger import get_logger

logger = get_logger("ohlcv_dimension_enrichment")

class OHLCVDimensionEnricher:

    @staticmethod
    def enrich_ohlcv_dimension(ohlcv_df: DataFrame, qqq_ent_df: DataFrame, qqq_cat_df: DataFrame) -> DataFrame:
        logger.info("Start enriching OHLCV with QQQ categories and entities dimensions")

        return ( 
            ohlcv_df
            .join(qqq_ent_df, on="symbol", how="left")
            .join(qqq_cat_df, on="symbol", how="left")
        )