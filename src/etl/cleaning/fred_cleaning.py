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

    @staticmethod
    def clean(df: DataFrame) -> DataFrame:
        logger.info("Cleaning FRED dataset")

        # --- column standardization ---
        df = df.select(
            lower(trim(col("indicator_id"))).alias("indicator_id"),
            to_date(col("date")).alias("date"),
            col("value").cast("double").alias("value"),
        )

        logger.info("FRED cleaning finished")
        return df