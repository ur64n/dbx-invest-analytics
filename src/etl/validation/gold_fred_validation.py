from pyspark.sql import DataFrame
from src.config.logger import get_logger
from pyspark.sql.functions import col

logger = get_logger("gold_fred_validation")

class GoldFredValidator:

    @staticmethod
    def validate_row_after_join(fact_df: DataFrame, enriched_df: DataFrame) -> None:
        logger.info("Comparing datasets total rows, before and after join")
        
        if fact_df.count() != enriched_df.count():
            raise ValueError("Number of rows after join is different than before")

    @staticmethod
    def validate_nulls(df: DataFrame) -> None:
        logger.info("Checking for nulls in joined columns")
        
        total_nulls = df.filter(
            (col("frequency").isNull()) |
            (col("unit").isNull()))
        
        if total_nulls.limit(1).count() > 0:
            raise ValueError("Nulls found in joined columns")
