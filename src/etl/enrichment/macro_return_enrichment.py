from pyspark.sql import DataFrame
from src.config.logger import get_logger

logger = get_logger("macro_return_enrichment")

class MacroReturnEnricher:
    """Joins QQQ monthly returns with monthly macro indicators on year_month."""

    @staticmethod
    def enrich(monthly_return_df: DataFrame, macro_monthly_df: DataFrame) -> DataFrame:
        logger.info("Enriching QQQ monthly returns with macro indicators")

        enriched_df = monthly_return_df.join(macro_monthly_df, on="year_month", how="inner")

        logger.info("Enrichment complete")

        return enriched_df