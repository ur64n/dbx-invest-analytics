from pyspark.sql import SparkSession
from src.config.logger import get_logger
from src.config.config_loader import load_config

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.cleaning.qqq_categories_cleaning import QQQCategoriesCleaner
from src.etl.validation.qqq_categories_validation import QQQCategoriesValidator

logger = get_logger("qqq_categories_bronze_to_silver")

def run():
    logger.info("Starting qqq_categories_bronze_to_silver pipeline")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()
    
    # ---------- read data ----------
    df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["bronze_qqq_categories"]
    ).read()

    # ---------- cleaning ----------
    df = QQQCategoriesCleaner.standardize_strings(df)

    # ---------- validation ----------
    QQQCategoriesValidator.validate_schema(df)
    QQQCategoriesValidator.validate_not_empty(df)


if __name__ == "__main__":
    run()