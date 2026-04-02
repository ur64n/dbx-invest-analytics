from pyspark.sql import DataFrame
from pyspark.sql.functions import col, min, max
from src.config.logger import get_logger

from pyspark.sql.functions import current_date

from src.etl.schema.ohlcv_schema import NOT_NULL_COLUMNS, PRICE_COLUMNS

logger = get_logger("ohlcv_validation")

class OHLCVValidator:

    @staticmethod
    def validate_records_number_per_symbol(df: DataFrame) -> None:
        logger.info("Validating ohlcv records number per symbol")

        if df.groupBy("symbol").count().filter(col("count") < 2).count() > 0:
            raise ValueError("Symbol with less than 2 records found")

    @staticmethod
    def validate_null_values(df: DataFrame) -> None:
        logger.info(f"Validating ohlcv null values in {NOT_NULL_COLUMNS}")

        for column_name in NOT_NULL_COLUMNS:
            if df.filter(col(column_name).isNull()).count() > 0:
                raise ValueError(f"Null values found in column {column_name}")

    @staticmethod
    def validate_empty_values(df: DataFrame) -> None:
        logger.info("Validating ohlcv empty values in symbol column")

        if df.filter(col("symbol") == "").limit(1).count() > 0:
            raise ValueError("Empty values found in symbol column")

    @staticmethod
    def validate_domain_rules(df: DataFrame) -> None:
        logger.info("Validating ohlcv domain rules")

        # high < low (not allowed)
        if df.filter(col("high") < col("low")).head(1):
            raise ValueError("High value is less than low value")
        
        # one row per symbol (not allowed)
        agg_df = (
            df
            .groupBy("symbol")
            .agg(
                min("date").alias("min_date"),
                max("date").alias("max_date")
            )
        )

        if agg_df.filter(col("min_date") == col("max_date")).count() > 0:
            raise ValueError("Symbol with only one date found")

        # open must be between low and high
        if df.filter(
            (col("open") < col("low")) |
            (col("open") > col("high"))
        ).count() > 0:
            raise ValueError("Open value is not between low and high value")

        # close must be between low and high
        if df.filter(
            (col("close") < col("low")) |
            (col("close") > col("high"))
        ).count() > 0:
            raise ValueError("Close value is not between low and high value")
        
        # price columns must be > 0
        condition = None

        for c in PRICE_COLUMNS:
            expr = col(c) <= 0
            condition = expr if condition is None else condition | expr
            # "\" = or
            if condition is not None and df.filter(condition).limit(1).count()> 0:
                raise ValueError(f"Invalid price values found in column: {c}")

        # volume must be >= 0
        if df.filter(col("volume") < 0).count() > 0:
            raise ValueError("Volume value is less than zero")

        # date > today (not allowed)
        if df.filter(col("date") > current_date()).head(1):
            raise ValueError("Date value is greater than today")



