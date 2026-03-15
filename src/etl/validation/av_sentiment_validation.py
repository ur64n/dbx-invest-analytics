from pyspark.sql import DataFrame
from src.config.logger import get_logger

from src.etl.schema.av_schema import BRONZE_REQUIRED_COLUMNS

logger = get_logger("av_validation")

class AVValidator:

    @staticmethod
    def validate_schema(df: DataFrame) -> None:
        logger.info("Validating AV required columns")

        missing = BRONZE_REQUIRED_COLUMNS - set(df.columns)

        if missing:
            raise ValueError(f"Missing required columns: {missing}")

    @staticmethod
    def validate_not_empty(df: DataFrame) -> None:
        logger.info("Validating AV dataset is not empty")

        if df.limit(1).count() == 0:
            raise ValueError("AV dataset is empty")

    