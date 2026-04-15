from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src.config.logger import get_logger

logger = get_logger("gold_ohlcv_validation")

class GoldOHLCVValidator:
    """
    Validates Gold OHLCV table, QQQ entities have dimensions, benchmarks don't.
    """
    
    @staticmethod
    def validate_qqq_null_values(df: DataFrame) -> None:
        logger.info("Validating null values in qqq entities dimensions columns")

        non_benchmark = df.filter(~col("symbol").isin("^vix", "qqq", "spy", "tlt", "gld"))

        nulls = non_benchmark.filter(
            col("name").isNull() |
            col("sector").isNull() |
            col("industry").isNull()
        ).count()

        if nulls > 0:
            logger.warning(f"Found {nulls} rows with missing dimensions")

    @staticmethod
    def validate_benchmark_null_values(df: DataFrame) -> None:
        logger.info("Validating benchmark symbols have null dimensions")

        benchmark = df.filter(col("symbol").isin("^vix", "qqq", "spy", "tlt", "gld"))

        if benchmark.filter(
            col("name").isNotNull() |
            col("sector").isNotNull() |
            col("industry").isNotNull()
        ).head(1):
            raise ValueError("Benchmark symbols have unexpected dimension values")