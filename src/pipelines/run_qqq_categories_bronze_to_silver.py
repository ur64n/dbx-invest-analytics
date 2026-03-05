from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor

logger = get_logger("qqq_categories_bronze_to_silver")

def main():
    logger.info("Starting qqq_categories_bronze_to_silver")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    # ---------- read data ----------
    table_name = config["tables"]["bronze_qqq"]

    df = DeltaTableExtractor(
        spark=spark,
        table_name=table_name
    ).read()

if __name__ == "__main__":
    main()