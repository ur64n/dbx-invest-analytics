from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src.config.logger import get_logger
from pyspark.sql.functions import current_date

logger = get_logger("fred_validation")

class FredValidator:
    """Domain-specific validation for FRED indicator data.

    Checks: indicator_id not null, date not future, value >= 0 (nulls allowed).
    """
    
    @staticmethod
    def validate_domain_rules(df: DataFrame) -> None:
        logger.info("Validating domain rules (indicator_id, date, value)")

        # indicator_id not null
        if df.filter(col("indicator_id").isNull()).head(1):
            raise ValueError("indicator_id contains NULLs")

        # date not in the future
        if df.filter(col("date") > current_date()).head(1):
            raise ValueError("date contains future values")

        # value >= 0 (NULL allowed)
        if df.filter(col("value") < 0).head(1):
            raise ValueError("Negative indicator values found")
