from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src.config.logger import get_logger

logger = get_logger("fred_metadata_validation")

class FredMetadataValidator:

    REQUIRED_COLUMNS = {
        "indicator_id",
        "unit",
        "frequency",
    }

    @staticmethod
    def validate_metadata_schema(df: DataFrame) -> None:
        logger.info("Validating metadata schema")

        missing_columns = FredMetadataValidator.REQUIRED_COLUMNS - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

    @staticmethod
    def validate_metadata_not_empty(df: DataFrame) -> None:
        logger.info("Validating fred metadata is not empty")

        if df.filter(col("indicator_id").isNull()).count() > 0:
            raise ValueError("indicator_id contains NULL")

        if df.filter(col("unit").isNull()).count() > 0:
                    raise ValueError("unit contains NULL")

        if df.filter(col("frequency").isNull()).count() > 0:
            raise ValueError("frequency contains NULL")

    @staticmethod
    def validate_metadata_key_uniqueness(df: DataFrame) -> None:
        logger.info("Validating fred metadata key uniqueness")

        total = df.count()
        distinct = df.select("indicator_id").distinct().count()

        if total != distinct:
            raise ValueError(f"Duplicate indicator_id in metadata")

    @staticmethod
    def validate_cannonical_frequency(df: DataFrame) -> None:
        logger.info("Validating fred metadata frequency cannonical standard")

        allowed = ["m", "d", "q", "a"]

        if df.filter(~col("frequency").isin(allowed)).count() > 0:
            raise ValueError(f"Invalid frequency in metadata")

            # ("~"col) ~ odwaraca warunek. Czyli (isin = not in)

