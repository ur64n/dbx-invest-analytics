from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, trim, regexp_replace
from src.config.logger import get_logger

logger = get_logger("qqq_entities_cleaning")

class QQQEntitiesCleaner:

    @staticmethod
    def remove_invalid_rows(df: DataFrame) -> DataFrame:
        logger.info("Start cleaning rows in qqq_entities")

        return df.filter(
            col("Symbol").isNotNull() 
            & ~col("Symbol").rlike("(?i)^(qqq|downloaded)")
        )

    @staticmethod
    def clean_rows(df: DataFrame, text_cols: list[str]) -> DataFrame:
        logger.info("Standardizing row values")

        for c in text_cols:
            df = df.withColumn(
                c,
                lower(trim(regexp_replace(col(c), "\\s+", " ")))
            )

        return df