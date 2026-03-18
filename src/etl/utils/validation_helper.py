from pyspark.sql import DataFrame
from src.config.logger import get_logger

logger = get_logger("validation_helper")

class ValidationHelper:

    @staticmethod
    def validate_schema(df: DataFrame, required_columns: set, context: str = ""):
        logger.info("Starting validation of required columns")

        missing = required_columns - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns in {context}: {missing}")

        logger.info("All required columns available in dataset")
    
    @staticmethod
    def validate_not_empty(df: DataFrame, context: str = ""):
        logger.info("Starting dataset emptiness validation")

        if df.limit(1).count() == 0:
            raise ValueError(f"Dataset is empty: {context}")

        logger.info("Dataset contains rows")