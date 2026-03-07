from pyspark.sql import SparkSession
from src.etl.config.config_loader import load_config
from src.etl.config.logger import get_logger

logger = get_logger("fred_bronze_to_silver")

def run():
    logger.info("Starting bronze to silver fred pipeline")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()
    

if __name__ == "__main__":
    run()