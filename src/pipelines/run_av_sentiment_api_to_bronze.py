from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.dbutils import DButils

from src.config.logger import get_logger
from src.config.config_loader import load_config

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor

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
    rate_limit = config["alpha_vantage"]["rate_limit_per_min"]
    limit_per_request = config["alpha_vantage"]["articles_per_request"]

    run_ts = datetime.now(ZoneInfo("Europe/Warsaw"))

    # ---------- load symbols from silver_qqq ----------
    entities_df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["silver_qqq"]
    ).read()

    symbols = [
        row["symbol"].upper()
        for row in entities_df.select("symbol").distinct().collect()
    ]

    logger.info(f"Total symbols to fetch sentiment for {len(symbols)}")

    # ---------- extraction + parsing ----------

if __name__ == "__main__":
    run()