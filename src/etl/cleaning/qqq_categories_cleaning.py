from src.config.logger import get_logger
from pyspark.sql import DataFrame

from pyspark.sql.functions import col, trim, lower

logger = get_logger("qqq_categories_cleaning")

class QQQCategoriesCleaner:
    """Standardizes QQQ category strings to lowercase and trimmed."""

    @staticmethod
    def standardize_strings(df: DataFrame) -> DataFrame:
        logger.info("Standarizing strings columns lower and trim")
        
        return df.select(
            lower(trim(col("symbol"))).alias("symbol"),
            lower(trim(col("sector"))).alias("sector"),
            lower(trim(col("industry"))).alias("industry")
        )
