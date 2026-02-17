from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src.config.logger import get_logger
from pyspark.sql.functions import current_date

logger = get_logger("fred_validation")

class FredValidator:

    REQUIRED_COLUMNS = {
        "indicator_id",
        "date",
        "value",
        "unit",
        "frequency",
    }

    @staticmethod
    def validate_schema(df: DataFrame) -> None:
        logger.info("Validating FRED schema")

        missing = FredValidator.REQUIRED_COLUMNS - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

    @staticmethod
    def validate_not_empty(df: DataFrame) -> None:
        logger.info("Validating FRED dataset not empty")

        if df.count() == 0:
            raise ValueError("FRED dataset is empty")

    @staticmethod
    def validate_domain_rules(df: DataFrame) -> None:
        logger.info("Validating FRED domain rules")

        # indicator_id not null
        if df.filter(col("indicator_id").isNull()).count() > 0:
            raise ValueError("indicator_id contains NULLs")

        # date not in the future
        if df.filter(col("date") > current_date()).count() > 0:
            raise ValueError("date contains future values")

        # value >= 0 (NULL allowed)
        negatives = df.filter(col("value") < 0).count()
        if negatives > 0:
            raise ValueError(f"Found {negatives} negative indicator values")

    @staticmethod
    def validate_uniqueness(df: DataFrame) -> None:
        logger.info("Validating uniqueness (indicator_id, date)")

        total = df.count()
        distinct = df.select("indicator_id", "date").distinct().count()

        if total != distinct:
            raise ValueError(
                f"Duplicate (indicator_id, date): {total - distinct}"
            )
