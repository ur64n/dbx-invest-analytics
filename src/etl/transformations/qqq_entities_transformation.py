from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace
from src.config.logger import get_logger

logger = get_logger("qqq_entities_transformation")

class QQQEntitiesTransformer:
    """Transforms raw QQQ CSV columns to Silver standard.

    Renames columns to snake_case, drops unused columns (Shares),
    casts percent_holding from string '5.23%' to decimal.
    """
    
    @staticmethod
    def transform_raw(df: DataFrame) -> DataFrame:
        logger.info("Transforming raw QQQ CSV")

        df = (
            df
            .withColumnRenamed("Symbol", "symbol")
            .withColumnRenamed("Name", "name")
            .withColumnRenamed("% Holding", "percent_holding")
            .drop("Shares")
        )

        df = df.withColumn(
            "percent_holding",
            regexp_replace(col("percent_holding"), "%", "")
            .cast("decimal(5,2)")
        )

        return df