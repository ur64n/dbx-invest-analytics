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
    """
    Cleaning only.
    - no IO
    - no writes
    - DataFrame -> DataFrame
    """

    @staticmethod
    def clean(df: DataFrame) -> DataFrame:
        logger.info("Cleaning FRED dataset")

        # --- column standardization ---
        df = df.select(
            lower(trim(col("indicator_id"))).alias("indicator_id"),
            to_date(col("date")).alias("date"),
            col("value").cast("double").alias("value"),
            lower(trim(col("unit"))).alias("unit"),
            lower(trim(col("frequency"))).alias("frequency"),
        )

        # --- value normalization ---
        df = df.withColumn(
            "frequency",
            regexp_replace(col("frequency"), "monthly", "m"),
        ).withColumn(
            "frequency",
            regexp_replace(col("frequency"), "daily", "d"),
        )

        logger.info("FRED cleaning finished")
        return df
