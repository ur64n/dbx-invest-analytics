from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    when,
)

from src.config.logger import get_logger

logger = get_logger("fred_metadata_cleaning")

class FredMetadataCleaner:

    @staticmethod
    def clean_metadata(df: DataFrame) -> DataFrame:
        logger.info("Cleaning FRED metadata")

        df = (
            df.select(
                lower(trim(col("indicator_id"))).alias("indicator_id"),
                lower(trim(col("unit"))).alias("unit"),
                lower(trim(col("frequency"))).alias("frequency"),
            )
        )

        # zmienia puste stringi "" na none/null
        # .otherwise mówi: (w przeciwnym razie pozostaw bez zmian)
        df = (
            df.withColumn(
                "indicator_id",
                when(col("indicator_id") == "", None).otherwise(col("indicator_id"))
            )
            .withColumn(
                "unit",
                when(col("unit") == "", None).otherwise(col("unit"))
            )
            .withColumn(
                "frequency",
                when(col("frequency") == "", None).otherwise(col("frequency"))
            )
        )

        # canonical frequency 
        df = (
            df.withColumn(
                "frequency",
                when(col("frequency") == "monthly", "m")
                .when(col("frequency") == "daily", "d")
                .when(col("frequency") == "quarterly", "q")
                .when(col("frequency") == "annual", "a")
                .otherwise(col("frequency"))
            )
        )

        logger.info("FRED metadata cleaning finished")
        return df
