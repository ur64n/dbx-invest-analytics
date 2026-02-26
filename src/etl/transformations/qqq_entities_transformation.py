from pyspark.sql import DataFrame
from src.config.logger import get_logger
from src.etl.validation.qqq_entities_validation import QQQEntitiesValidator
from pyspark.sql.functions import lower, col

logger = get_logger("transformation")

class QQQEntitiesTransformer:

    @staticmethod
    def transform_raw(df: DataFrame) -> DataFrame:
        logger.info("Transforming raw QQQ CSV")

        QQQEntitiesValidator.validate_raw_csv_schema(df)
        QQQEntitiesValidator.validate_raw_csv_not_empty(df)

        df = (
            df
            .withColumnRenamed("Symbol", "symbol")
            .withColumnRenamed("Name", "name")
            .withColumnRenamed("% Holding", "precent_holding")
            .drop("Shares")
        )

        df = df.withColumn("symbol", lower(col("symbol")))

        QQQEntitiesValidator.validate_column_values(df)
        QQQEntitiesValidator.validate_symbol_uniqueness(df)

        return df