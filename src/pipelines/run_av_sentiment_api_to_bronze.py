from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.dbutils import DButils

from src.config.logger import get_logger
from src.config.config_loader import load_config

logger = get_logger("av_sentiment_api_to_bronze")

def run():
    logger.info("Starting Aplha Vantage sentiment pipeline API to bronze")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    # ---------- secrets ----------
    dbutils = DButils(spark)
    api_key = dbutils.secrets.get("my-scope","API_KEY_AV")

    # ---------- parameters ----------
    

if __name__ == "__main__":
    run()