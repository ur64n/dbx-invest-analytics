from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.extraction.gdelt.gdelt_client import GdeltClient

logger = get_logger("gdelt_new_api_to_bronze")

def run():
    logger.info("Starting gdelt_new_api_to_bronze pipeline")
    
    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    # ---------- extractor ----------
    for kw in config["gdelt"]["keywords"]:
    
    gdelt_data = GdeltClient(
        config=config,
        spark=spark
    ).fetch_articles(kw)

if __name__ == "__main__":
    run()
