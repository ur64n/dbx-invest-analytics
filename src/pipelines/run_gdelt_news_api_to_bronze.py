from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

logger = get_logger("gdelt_new_api_to_bronze")

def run():
    logger.info("Starting gdelt_new_api_to_bronze pipeline")
    
    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    # ---------- extractor ----------
    

if __name__ == "__main__":
    run()