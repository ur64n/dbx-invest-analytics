from pyspark.sql import DataFrame
from pyspark.sql.functions import col, abs as spark_abs
from src.config.logger import get_logger

logger = get_logger("gold_macro_impact_validation")

class GoldMacroImpactValidator:
    """Validates macro impact table, non-null monthly returns, extreme value warnings."""

    @staticmethod
    def validate_domain_rules(df: DataFrame) -> None:
        logger.info("Starting domain rules validation in gold_macro_impact_on_tech")

        if df.filter(col("monthly_return").isNull()).head(1):
            raise ValueError("monthly_return contains nulls")

        if df.filter(spark_abs(col("monthly_return")) > 0.5).head(1):
            logger.warning("monthly_return contains extreme values (>50%)")