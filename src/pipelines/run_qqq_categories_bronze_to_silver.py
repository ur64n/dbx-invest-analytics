from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

logger = get_logger("qqq_categories_bronze_to_silver")

def main():
    logger.info("Starting qqq_categories_bronze_to_silver")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()
    

if __name__ == "__main__":
    main()