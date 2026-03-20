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

    @staticmethod
    def validate_uniqueness(df: DataFrame, key_columns: list[str]) -> None:
        logger.info("Staring validation key columns uniqueness")

        total = df.count()
        distinct = df.select(*key_columns).distinct().count()

        if total != distinct:
            raise ValueError(f"Duplicates in dataset on key columns: {total - distinct}")

    @staticmethod
    def validate_row_after_join(source_df: DataFrame, enriched_df: DataFrame) -> None:
        logger.info("Comparing datasets total rows, before and after join")
        
        if source_df.count() != enriched_df.count():
            raise ValueError("Number of rows after join is different than before")

            
