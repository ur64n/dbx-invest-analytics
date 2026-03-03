from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, trim, regexp_replace
from src.config.logger import get_logger
from src.etl.validation.qqq_entities_validation import QQQEntitiesValidator

logger = get_logger("cleaning")

class QQQEntitiesCleaner:
    """
    Data standardization only.
    DataFrame -> DataFrame
    """

    @staticmethod
    def clean_columns(df: DataFrame) -> DataFrame:
        logger.info("Standardizing column names and types")

        df = df.toDF(*[c.strip().lower() for c in df.columns]) # list comp zbiera liste nazw kolumn z dataframe zmniejsza i usuwa biale znaki, a .toDF ustawia nowe nazwy w nowym dataframe

        df = df.withColumn(
            "precent_holding",
            regexp_replace(col("precent_holding"), "%", "")
            .cast("decimal(5,2)")
        )

        return df

    @staticmethod
    def clean_rows(df: DataFrame) -> DataFrame:
        logger.info("Standardizing row values")

        text_cols = ["symbol", "name", "sector", "industry"]

        for c in text_cols:
            df = df.withColumn(
                c,
                lower(trim(regexp_replace(col(c), "\\s+", " ")))
            )

        QQQEntitiesValidator.validate_no_null_holdings(df)

        return df
