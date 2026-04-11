from pyspark.sql import SparkSession
from src.config.logger import get_logger
from src.config.config_loader import load_config

from src.etl.schema.qqq_categories_schema import REQUIRED_COLUMNS, QQQ_CATEGORIES_SCHEMA

from src.etl.monitoring.pipeline_run_logger import PplLogger
from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.extraction.qqq_categories_extraction import QQQCategoriesExtractor
from src.etl.utils.validation_helper import ValidationHelper
from src.etl.validation.qqq_categories_validation import QQQCategoriesValidator
from src.etl.write.delta_table_writer import DeltaTableWriter

logger = get_logger("qqq_categories_api_to_bronze_pipeline")

def run():

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()
    ppl_name = "run_qqq_categories_api_to_bronze"
    layer = "bronze"
    source_table = config["tables"]["bronze_qqq"]
    target_table = config["tables"]["bronze_qqq_categories"]

    # -------- monitoring --------
    ppl_logger = PplLogger(spark)
    ppl_logger.start(
        ppl_name,
        layer,
        source_table,
        target_table
    )

    try:    
        # ---------- read data ----------
        df = DeltaTableExtractor(
            spark=spark,
            table_name=config["tables"]["bronze_qqq"]
        ).read()

        # ---------- collect symbols ----------
        symbols = [r.symbol for r in df.select("symbol").distinct().collect()]
        
        # ---------- extraction ----------
        df = QQQCategoriesExtractor(spark).extract(symbols, QQQ_CATEGORIES_SCHEMA)

        # -------- monitoring --------
        input_rows = df.count()

        # ---------- validation ----------
        ValidationHelper.validate_schema(df, REQUIRED_COLUMNS, context="qqq_etf_categories")
        ValidationHelper.validate_not_empty(df, context="qqq_etf_categories")
        QQQCategoriesValidator.validate_symbols_consistency(symbols, df)

        # ---------- write data ----------
        DeltaTableWriter(
            table_name=config["tables"]["bronze_qqq_categories"],
            spark=spark
        ).overwrite(df)

        # -------- monitoring --------
        output_rows = df.count()
        rows_rejected = input_rows - output_rows

        ppl_logger.finish(
            input_rows,
            output_rows,
            rows_rejected,
            config_params = None
        )

    except Exception as e:
        ppl_logger.fail(str(e))
        raise
    
if __name__ == "__main__":
    run()