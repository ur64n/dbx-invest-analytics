from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.schema.qqq_entities_schema import REQUIRED_SILVER_COLUMNS

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.cleaning.qqq_entities_cleaning import QQQEntitiesCleaner
from src.etl.transformations.qqq_entities_transformation import QQQEntitiesTransformer
from src.etl.utils.validation_helper import ValidationHelper
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

    # ---------- transformation ----------
    df = QQQEntitiesTransformer.transform_raw(df)

    # ---------- cleaning ----------
    df = QQQEntitiesCleaner.clean_rows(df, ["symbol", "name"])

    # ---------- validation ----------
    ValidationHelper.validate_schema(
        df,
        REQUIRED_SILVER_COLUMNS,
        context="qqq_entities_silver"
    )

    ValidationHelper.validate_not_empty(
        df,
        context="qqq_entities_silver"
    )

    ValidationHelper.validate_uniqueness(
        df,
        key_columns=["symbol"]
    )

    QQQEntitiesValidator.validate_column_values(df)
    QQQEntitiesValidator.validate_holding_range(df)
    QQQEntitiesValidator.validate_no_null_holdings(df)

    # ---------- write data ----------
    DeltaTableWriter(
        table_name=config["tables"]["silver_qqq"],
        spark=spark
    ).overwrite(df)
    
    logger.info("qqq_entities_bronze_to_silver pipeline finished successfully")

if __name__ == "__main__":
    run()