from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.schema.fred_schema import KEY_COLUMNS
from src.etl.schema.fred_metadata_schema import KEY_METADATA_COLUMNS

import json

from src.etl.monitoring.pipeline_run_logger import PplLogger
from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.cleaning.fred_cleaning import FredCleaner
from src.etl.cleaning.fred_metadata_cleaning import FredMetadataCleaner
from src.etl.utils.validation_helper import ValidationHelper
from src.etl.validation.fred_validation import FredValidator
from src.etl.validation.fred_metadata_validation import FredMetadataValidator
from src.etl.write.delta_table_writer import DeltaTableWriter

logger = get_logger("fred_bronze_to_silver")

def run():

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()
    ppl_name = "run_fred_bronze_to_silver"
    layer = "silver"
    source_table = config["tables"]["bronze_fred_macro_indicators"]
    target_table = config["tables"]["silver_fred_macro_indicators"]

    # -------- monitoring --------
    ppl_logger = PplLogger(spark)
    ppl_logger.start(
        ppl_name,
        layer,
        source_table,
        target_table
    )
    
    try:
        # ---------- extraction ----------
        fact_df = DeltaTableExtractor(
            spark=spark,
            table_name=config["tables"]["bronze_fred_macro_indicators"]
        ).read()

        dim_df = DeltaTableExtractor(
            spark=spark,
            table_name=config["tables"]["bronze_fred_macro_indicator_metadata"]
        ).read()

        # -------- monitoring --------
        input_rows = fact_df.count()
        input_metadata_rows = dim_df.count()

        # ---------- cleaning ----------
        fact_df = FredCleaner.standardize_columns(fact_df)
        dim_df = FredMetadataCleaner.clean_metadata(dim_df)

        # ---------- domain validation ----------
        FredValidator.validate_domain_rules(fact_df)
        ValidationHelper.validate_uniqueness(fact_df, KEY_COLUMNS)
        
        ValidationHelper.validate_uniqueness(dim_df, KEY_METADATA_COLUMNS)
        FredMetadataValidator.validate_canonical_frequency(dim_df)

        # -------- monitoring --------
        config_params = json.dumps({
            "metadata_rows_after_cleaning": dim_df.count(),
            "input_metadata_rows": input_metadata_rows
        })

        # ---------- write ----------
        DeltaTableWriter(
            spark=spark,
            table_name=config["tables"]["silver_fred_macro_indicators"]
        ).upsert(fact_df, merge_keys=["indicator_id","date"])

        DeltaTableWriter(
            spark=spark,
            table_name=config["tables"]["silver_fred_macro_indicator_metadata"]
        ).upsert(dim_df, merge_keys=["indicator_id"])

        # -------- monitoring --------
        output_rows = fact_df.count()
        rows_rejected = input_rows - output_rows

        ppl_logger.finish(
            input_rows,
            output_rows,
            rows_rejected,
            config_params
        )

    except Exception as e:
        ppl_logger.fail(str(e))
        raise

if __name__ == "__main__":
    run()