from pyspark.sql import SparkSession
from src.config.logger import get_logger
from src.config.config_loader import load_config

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor

logger = get_logger("qqq_entities_bronze_to_silver")

def run():
    logger.info("Starting qqq_entities_bronze_to_silver pipeline")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()
    
    # ---------- extraction ----------
    extractor = DeltaTableExtractor(
        spark,
        table_name=config["tables"]["bronze_qqq"]
    )

    df = extractor.read()

    df.limit(10).display()

if __name__ == "__main__":
    run()