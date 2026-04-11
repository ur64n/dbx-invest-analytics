from pyspark.sql import SparkSession
from src.config.logger import get_logger
from src.config.config_loader import load_config

from src.etl.schema.qqq_categories_schema import KEY_COLUMNS

from src.etl.monitoring.pipeline_run_logger import PplLogger
from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.cleaning.qqq_categories_cleaning import QQQCategoriesCleaner
from src.etl.utils.validation_helper import ValidationHelper
from src.etl.validation.qqq_categories_validation import QQQCategoriesValidator
from src.etl.write.delta_table_writer import DeltaTableWriter

logger = get_logger("qqq_categories_bronze_to_silver")

def run():

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()
    ppl_name = "run_qqq_categories_bronze_to_silver"
    layer = "silver"
    source_table = config["tables"]["bronze_qqq_categories"]
    target_table = config["tables"]["silver_qqq_categoties"]

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
            table_name=config["tables"]["bronze_qqq_categories"]
        ).read()

        # -------- monitoring --------
        input_rows = df.count()

        # ---------- cleaning ----------
        df = QQQCategoriesCleaner.standardize_strings(df)

        # ---------- validation ----------
        ValidationHelper.validate_uniqueness(df, KEY_COLUMNS)
        QQQCategoriesValidator.validate_symbol_nulls(df)
        QQQCategoriesValidator.validate_attribute_nulls(df)

        # ---------- write data ----------
        DeltaTableWriter(
            table_name=config["tables"]["silver_qqq_categoties"],
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