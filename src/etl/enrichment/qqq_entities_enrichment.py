from pyspark.sql import DataFrame
from src.config.logger import get_logger
from src.etl.validation.qqq_entities_validation import QQQEntitiesValidator

logger = get_logger("qqq-enrichment")

class QQQEntitiesEnricher:
    """
    Enrichment = joining datasets.
    No IO, no API calls.
    """

    @staticmethod
    def enrich(base_df: DataFrame, category_df: DataFrame) -> DataFrame:
        logger.info("Enriching QQQ entities")

        QQQEntitiesValidator.validate_enrichment_inputs(
            base_df, category_df
        )

        result_df = base_df.join(
            category_df, on="symbol", how="left"
        )

        QQQEntitiesValidator.validate_missing_categories(
            base_df.select("symbol").distinct().count(),
            category_df,
        )

        return result_df