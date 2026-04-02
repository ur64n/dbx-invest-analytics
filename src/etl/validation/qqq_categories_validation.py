from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src.config.logger import get_logger

logger = get_logger("qqq_categories_validation")

class QQQCategoriesValidator:

    @staticmethod
    def validate_symbols_consistency(symbols: list, df: DataFrame) -> None:
        logger.info("Validating symbols consistency")

        missing_symbols = set(symbols) - set(row.symbol for row in df.select("symbol").distinct().collect())

        if missing_symbols:
            raise ValueError(f"Missing symbols in categories df: {missing_symbols}")
    
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

