from pyspark.sql import DataFrame
from src.config.logger import get_logger
from pyspark.sql.functions import col

logger = get_logger("gold_fred_validation")

class GoldFredValidator:

    @staticmethod
    def validate_nulls(df: DataFrame) -> None:
        logger.info("Checking for nulls in joined columns")

        if df.filter((col("frequency").isNull()) | (col("unit").isNull())).head(1):
            raise ValueError("Nulls found in joined columns")
