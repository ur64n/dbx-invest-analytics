from pyspark.sql import SparkSession
from src.config.logger import get_logger
from src.config.config_loader import load_config

from src.etl.schema.qqq_categories_schema import REQUIRED_COLUMNS

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.extraction.qqq_categories_extraction import QQQCategoriesExtractor
from src.etl.utils.validation_helper import ValidationHelper
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

    # ---------- collect symbols ----------
    symbols = [r.symbol for r in df.select("symbol").distinct().collect()]
    
    # ---------- extraction ----------
    df = QQQCategoriesExtractor(spark).extract(symbols)

    # ---------- validation ----------
    ValidationHelper.validate_schema(df, REQUIRED_COLUMNS, context="qqq_etf_categories")
    ValidationHelper.validate_not_empty(df, context="qqq_etf_categories")
    QQQCategoriesValidator.validate_symbols_consistency(symbols, df)

    # ---------- write data ----------
    DeltaTableWriter(
        table_name=config["tables"]["bronze_qqq_categories"],
        spark=spark
    ).overwrite(df)
    
if __name__ == "__main__":
    run()