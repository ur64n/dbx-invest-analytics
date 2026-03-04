from pyspark.sql import SparkSession
from src.config.logger import get_logger
from src.config.config_loader import load_config

from src.etl.extraction.qqq_categories_extraction import QQQCategoriesExtractor
from src.etl.validation.qqq_categories_validation import QQQCategoriesValidator

logger = get_logger("qqq_categories_api_to_bronze_pipeline")

def run():
    logger.info("Starting qqq_categories_api_to_bronze_pipeline")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()
    qqq_entities_df = config["tables"]["bronze_qqq"]

    # ---------- data load ----------
    df = spark.read.table(qqq_entities_df)

    # ---------- extraction ----------
    symbols = [r.symbol for r in df.select("symbol").distinct().collect()]

    df = QQQCategoriesExtractor(spark).extract(symbols)

    df.display()

    # ---------- cleaning ----------
    

    # ---------- validation ----------
    # QQQCategoriesValidator.validate_symbols_consistency(symbols, df)
    # QQQCategoriesValidator.validate_not_empty(df)
    # QQQCategoriesValidator.validate_schema(df)
    # QQQCategoriesValidator.validate_symbol_uniqueness(df)
    # QQQCategoriesValidator.validate_symbol_nulls(df)
    # QQQCategoriesValidator.validate_attribute_nulls(df)

    


if __name__ == "__main__":
    run()