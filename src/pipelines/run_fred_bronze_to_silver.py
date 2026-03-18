from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.cleaning.fred_cleaning import FredCleaner
from src.etl.cleaning.fred_metadata_cleaning import FredMetadataCleaner
from src.etl.validation.fred_validation import FredValidator
from src.etl.validation.fred_metadata_validation import FredMetadataValidator
from src.etl.write.delta_table_writer import DeltaTableWriter

logger = get_logger("fred_bronze_to_silver")

def run():
    logger.info("Starting bronze to silver fred pipeline")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()
    
    # ---------- extraction ----------
    fact_df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["bronze_fred_macro_indicators"]
    ).read()

    dim_df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["bronze_fred_macro_indicator_metadata"]
    ).read()

    # ---------- cleaning ----------
    fact_df = FredCleaner.standardize_columns(fact_df)
    dim_df = FredMetadataCleaner.clean_metadata(dim_df)

    # ---------- domain validation ----------
    FredValidator.validate_domain_rules(fact_df)
    FredValidator.validate_uniqueness(fact_df)
    
    FredMetadataValidator.validate_metadata_key_uniqueness(dim_df)
    FredMetadataValidator.validate_canonical_frequency(dim_df)

    # ---------- write ----------
    DeltaTableWriter(
        spark=spark,
        table_name=config["tables"]["silver_fred_macro_indicators"]
    ).upsert(fact_df, merge_keys=["indicator_id","date"])

    DeltaTableWriter(
        spark=spark,
        table_name=config["tables"]["silver_fred_macro_indicator_metadata"]
    ).upsert(dim_df, merge_keys=["indicator_id"])

    logger.info("Fred Pipeline bronze to silver finished successfully")

if __name__ == "__main__":
    run()