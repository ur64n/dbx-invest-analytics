from pyspark.sql import SparkSession
from src.config.logger import get_logger
from src.config.config_loader import load_config

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.extraction.qqq_categories_extraction import QQQCategoriesExtractor
from src.etl.validation.qqq_categories_validation import QQQCategoriesValidator
from src.etl.write.delta_table_writer import DeltaTableWriter

logger = get_logger("qqq_categories_api_to_bronze_pipeline")

def run():
    logger.info("Starting qqq_categories_api_to_bronze_pipeline")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    # ---------- read data ----------
    df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["bronze_qqq"]
    ).read()

    # ---------- extraction ----------
    symbols = [r.symbol for r in df.select("symbol").distinct().collect()]

    df = QQQCategoriesExtractor(spark).extract(symbols)

    # ---------- validation ----------
    QQQCategoriesValidator.validate_symbols_consistency(symbols, df)
    QQQCategoriesValidator.validate_not_empty(df)
    QQQCategoriesValidator.validate_schema(df)
    QQQCategoriesValidator.validate_symbol_uniqueness(df)
    QQQCategoriesValidator.validate_symbol_nulls(df)
    QQQCategoriesValidator.validate_attribute_nulls(df)

    # ---------- write data ----------
    DeltaTableWriter(
        table_name=config["tables"]["bronze_qqq_categories"],
        spark=spark
    ).overwrite(df)
    
if __name__ == "__main__":
    run()