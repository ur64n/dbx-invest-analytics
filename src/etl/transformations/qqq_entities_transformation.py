from pyspark.sql import DataFrame
from src.config.logger import get_logger
from pyspark.sql.functions import lower, col

logger = get_logger("transformation")

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

        df = df.withColumn("symbol", lower(col("symbol")))
        
        return df