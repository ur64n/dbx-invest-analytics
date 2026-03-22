from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

logger = get_logger("run_gold_sentiment_lead_lag")

def run()
    logger.info("Starting gold sentiment lead lag pipeline")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreat()
    config = load_config()

    # ---------- read data ----------
    

if __name__ == "__main__":
    run()