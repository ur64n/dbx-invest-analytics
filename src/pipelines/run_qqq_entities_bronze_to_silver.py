from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.schema.qqq_entities_schema import required_silver_columns

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.cleaning.qqq_entities_cleaning import QQQEntitiesCleaner
from src.etl.validation.qqq_entities_validation import QQQEntitiesValidator
from src.etl.write.delta_table_writer import DeltaTableWriter

logger = get_logger("qqq_entities_bronze_to_silver")

def run():
    logger.info("Starting qqq_entities_bronze_to_silver")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    # ---------- read data ----------
    df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["bronze_qqq"]
    ).read()

    # ---------- cleaning ----------
    df = QQQEntitiesCleaner.clean_columns(df)
    df = QQQEntitiesCleaner.clean_rows(df,["symbol","name"])

    # ---------- validation ----------
    QQQEntitiesValidator.validate_schema(df, required_silver_columns)
    QQQEntitiesValidator.validate_holding_range(df)
    QQQEntitiesValidator.validate_no_null_holdings(df)

    # ---------- write data ----------
    DeltaTableWriter(
        table_name=config["tables"]["silver_qqq"],
        spark=spark
    ).overwrite(df)
    
    logger.info("Pipeline finished successfully")

if __name__ == "__main__":
    run()