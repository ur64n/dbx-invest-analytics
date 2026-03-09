from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor

logger = get_logger("gold_ohlcv_with_dimension")

def run():
    logger.info("Starting gold_ohlcv_with_dimension pipeline")

    # ---------- setup ----------
    spark = sparkSession.builder.getOrCreate()
    config = load_config()

    # ---------- read data ----------


if __name__ == "__main__":
    run()