from pyspark.sql import SparkSession

from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.schema.ohlcv_schema import KEY_COLUMNS

from src.etl.monitoring.pipeline_run_logger import PplLogger
from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.transformations.ohlcv_transformation import OHLCVTransformer
from src.etl.utils.validation_helper import ValidationHelper
from src.etl.validation.ohlcv_validation import OHLCVValidator
from src.etl.write.delta_table_writer import DeltaTableWriter

logger = get_logger("ohlcv_bronze_to_silver")

def run():
    
    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()
    ppl_name = "run_ohlcv_bronze_to_silver"
    layer = "silver"
    source_table = config["tables"]["bronze_ohlcv"]
    target_table = config["tables"]["silver_ohlcv"]

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
            table_name=config["tables"]["bronze_ohlcv"]
        ).read()

        # -------- monitoring --------
        input_rows = df.count()

        # ---------- transformation ----------
        df = OHLCVTransformer.cast_column_types(df)
        df = OHLCVTransformer.normalize_symbol(df)

        # ---------- validation ----------
        ValidationHelper.validate_uniqueness(df, KEY_COLUMNS)
        OHLCVValidator.validate_records_number_per_symbol(df)
        OHLCVValidator.validate_null_values(df)
        OHLCVValidator.validate_empty_values(df)
        OHLCVValidator.validate_domain_rules(df)

        # ---------- write data ----------
        DeltaTableWriter(
            spark=spark,
            table_name=config["tables"]["silver_ohlcv"]
        ).upsert(df, merge_keys=["date", "symbol"])

        # -------- monitoring --------
        output_rows = df.count()
        rows_rejected = input_rows - output_rows

        ppl_logger.finish(
            input_rows,
            output_rows,
            rows_rejected,
            None
        )

    except Exception as e:
        ppl_logger.fail(str(e))
        raise

if __name__ == "__main__":
    run()