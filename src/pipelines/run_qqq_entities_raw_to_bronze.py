from pyspark.sql import SparkSession
from src.config.logger import get_logger
from src.config.config_loader import load_config

from src.etl.schema.qqq_entities_schema import qqq_schema, required_raw_columns

from src.etl.extraction.qqq_entities_extraction import QQQEntitiesExtractor
from src.etl.cleaning.qqq_entities_cleaning import QQQEntitiesCleaner
from src.etl.validation.qqq_entities_validation import QQQEntitiesValidator
from src.etl.transformations.qqq_entities_transformation import QQQEntitiesTransformer
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
        schema=qqq_schema,
    )

    # ---------- read data ----------
    raw_df = extractor.read()

    # ---------- cleaning ----------
    cleaned_df = QQQEntitiesCleaner.remove_invalid_rows(raw_df)

    # ---------- validation ----------
    QQQEntitiesValidator.validate_schema(cleaned_df, required_raw_columns)
    QQQEntitiesValidator.validate_not_empty(cleaned_df)

    # ---------- transform ----------
    bronze_df = QQQEntitiesTransformer.transform_raw(cleaned_df)

    # ---------- validation ----------
    QQQEntitiesValidator.validate_column_values(bronze_df)
    QQQEntitiesValidator.validate_symbol_uniqueness(bronze_df)

    # ---------- write BRONZE ----------
    DeltaTableWriter(
        table_name=cfg["tables"]["bronze_qqq"],
        spark=spark
    ).overwrite(bronze_df)

    logger.info("qqq_entities_raw_to_bronze_pipeline completed successfully")

if __name__ == "__main__":
    run()
