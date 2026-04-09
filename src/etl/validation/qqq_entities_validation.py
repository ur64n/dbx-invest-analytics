from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from src.config.logger import get_logger

from src.etl.schema.qqq_entities_schema import NOT_NULL_COLUMNS

logger = get_logger("qqq_entities_validation")

class QQQEntitiesValidator:

    # ---------- Bronze ----------
    @staticmethod
    def validate_column_values(df: DataFrame) -> None:
        logger.info("Validating column values")

        for c in NOT_NULL_COLUMNS:
            null_count = df.filter(col(c).isNull()).count()

            if df.schema[c].dataType == StringType():
                null_count += df.filter(col(c) == lit("")).count()

            if null_count > 0:
                if c == "Symbol":
                    raise ValueError(f"Found empty values in critical column: {c}")
                logger.warning(f"Found {null_count} empty values in column: {c}")

    # ---------- Silver ----------
    @staticmethod
    def validate_holding_range(df: DataFrame) -> None:
        logger.info("Validating holding range")

        if df.filter((col("percent_holding") < 0) | (col("percent_holding") > 100)).head(1):
            raise ValueError("Found values outside of range 0-100 in precent_holding column")

        if df.filter(col("percent_holding") == 0).head(1):
            raise ValueError("Found 0 values in precent_holding column")

    @staticmethod
    def validate_no_null_holdings(df: DataFrame) -> None:
        logger.info("Validating no null percent holdings")

        null_count = df.filter(df["percent_holding"].isNull()).count()

        if null_count > 0:
            logger.warning(
                f"Found {null_count} null values in precent_holding column"
            )