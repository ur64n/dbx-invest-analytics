# Plik nie jest używany
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from src.config.logger import get_logger

logger = get_logger("fred_enrichment")


class FredEnricher:
    """
    Pure enrichment / shaping logic.
    No IO. No API. No writes.
    """

    @staticmethod
    def enrich(
        raw_df: DataFrame,
        indicator_id: str,
        unit: str,
        frequency: str,
    ) -> DataFrame:
        """
        raw_df columns expected:
        - date
        - value
        """

        logger.info(f"Enriching FRED indicator: {indicator_id}")

        df = (
            raw_df
            .withColumn("indicator_id", lit(indicator_id))
            .withColumn("unit", lit(unit))
            .withColumn("frequency", lit(frequency))
            .select(
                "indicator_id",
                col("date").cast("date").alias("date"),
                col("value").cast("double").alias("value"),
                "unit",
                "frequency",
            )
        )

        return df
