from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src.config.logger import get_logger

from src.etl.schema.qqq_categories_schema import required_columns


logger = get_logger("qqq_categories_validation")

class QQQCategoriesValidator:

    @staticmethod
    def validate_symbols_consistency(symbols: list, df: DataFrame) -> None:
        logger.info("Validating symbols consistency")

        missing_symbols = set(symbols) - set(df.select("symbol").distinct()
        .collect())

        if missing_symbols:
            raise ValueError(f"Missing symbols in categories df: {missing_symbols}")

    @staticmethod
    def validate_not_empty(df: DataFrame) -> None:
        logger.info("Validating categories df not empty")
      
        if df.limit(1).count() == 0:
            raise ValueError("Extracted category df is empty")

    @staticmethod
    def validate_schema(df: DataFrame) -> None:
        logger.info("Validating categories df required columns")

        if set(required_columns) != set(df.columns):
            raise ValueError("Missing required columns")

    @staticmethod
    def validate_symbol_uniqueness(df: DataFrame) -> None:
        logger.info("Validating categories df symbol uniqueness")

        if df.select("symbol").distinct().count() != df.select("symbol").count():
            raise ValueError("Duplicate symbols found in categories df")
    
    @staticmethod
    def validate_symbol_nulls(df: DataFrame) -> None:
        logger.info("Validating categories df symbol nulls")

        if df.filter(col("symbol").isNull()).count() > 0:
            raise ValueError("Null symbols found in categories df")
    
    @staticmethod
    def validate_attribute_nulls(df: DataFrame) -> None:
        logger.info("Validating categories df attribute nulls")

        missing_values = df.filter(
            col("sector").isNull() | col("industry").isNull()).count()

        if missing_values:
            logger.warning(f"In categories df, found: {missing_values} rows with null sector or industry")

        

    