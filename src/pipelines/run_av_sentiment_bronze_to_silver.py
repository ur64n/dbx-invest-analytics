from pyspark.sql import SparkSession
from src.config.logger import get_logger
from src.confing.config_loader import load_config

logger = get_logger("av_sentiment_bronze_to_silver")

def run():
    logger.info("Starting Alpha Vantage sentiment pipeline bronze to silver")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    # ---------- read data ----------


if __name__ == "__main__":
    run()