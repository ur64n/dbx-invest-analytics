from pyspark.sql import DataFrame
from src.config.logger import get_logger

logger = get_logger("fred-enrichment")

class FredIndicatorEnricher:

    @staticmethod
    def enrich(fact_df: DataFrame, dim_metadata_df: DataFrame) -> DataFrame:
        logger.info("Start enriching Fred indicators")

        return (
            fact_df.alias("f")
            .join(dim_metadata_df.alias("d"), on="indicator_id", how="left")
            .select(
                "f.indicator_id",
                "f.date",
                "f.value",
                "d.unit",
                "d.frequency"
            )
        )