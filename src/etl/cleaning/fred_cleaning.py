from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    regexp_replace,
    to_date,
)
from src.config.logger import get_logger

logger = get_logger("fred_cleaning")

class FredCleaner:
    """Standardizes FRED indicator data: lowercase indicator_id, cast types."""

    @staticmethod
    def standardize_columns(df: DataFrame) -> DataFrame:
        logger.info("Standardizing FRED dataset columns")

        # --- column standardization ---
        df = df.select(
            lower(trim(col("indicator_id"))).alias("indicator_id"),
            to_date(col("date")).alias("date"),
            col("value").cast("double").alias("value"),
        )

        logger.info("Standardizing FRED dataset columns finished")
        
        return df