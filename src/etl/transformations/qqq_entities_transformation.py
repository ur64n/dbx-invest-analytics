from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace
from src.config.logger import get_logger

logger = get_logger("qqq_entities_transformation")

class QQQEntitiesTransformer:

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