from pyspark.sql import SparkSession
from src.config.logger import get_logger
from src.config.config_loader import load_config

from src.etl.schema.qqq_entities_schema import QQQ_SCHEMA, REQUIRED_RAW_COLUMNS

from src.etl.extraction.qqq_entities_extraction import QQQEntitiesExtractor
from src.etl.cleaning.qqq_entities_cleaning import QQQEntitiesCleaner
from src.etl.utils.validation_helper import ValidationHelper
from src.etl.write.delta_table_writer import DeltaTableWriter

logger = get_logger("qqq_entities_raw_to_bronze_pipeline")

def run(env: str = "dev"):
    logger.info("Starting qqq_entities_raw_to_bronze_pipeline")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate() 
    cfg = load_config(env)

    # ---------- extraction ----------
    extractor = QQQEntitiesExtractor(
        spark=spark,
        raw_path=cfg["paths"]["raw_csv"],
        expected_filename=cfg["files"]["qqq_entities"],
        schema=QQQ_SCHEMA,
    )

    # ---------- read data ----------
    df = extractor.read()

    # ---------- cleaning ----------
    df = QQQEntitiesCleaner.remove_invalid_rows(df)

    # ---------- validation ----------
    ValidationHelper.validate_schema(
        df, 
        REQUIRED_RAW_COLUMNS, 
        context="qqq_etf_constituents"
    )
    
    ValidationHelper.validate_not_empty(
        df,
        context="qqq_etf_constituents"
    )

    # ---------- write BRONZE ----------
    DeltaTableWriter(
        table_name=cfg["tables"]["bronze_qqq"],
        spark=spark
    ).overwrite(df)

    logger.info("qqq_entities_raw_to_bronze_pipeline completed successfully")

if __name__ == "__main__":
    run()