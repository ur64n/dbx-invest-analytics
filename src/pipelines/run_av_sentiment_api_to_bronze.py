from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.dbutils import DButils

from src.config.logger import get_logger
from src.config.config_loader import load_config

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.extraction.alpha_vantage.av_sentiment_clinet import AlphaVantageSentimentClient

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
    base_url = config["alpha_vantage"]["base_url"]

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
    client = AlphaVantageSentimentClient(
        config=config,
        api_key=api_key
    )

    #parser = AlphaVantageSentimentParser()

    all_rows; list[dict] = []
    success_count = 0
    failure_count = 0

    for symbol in symbols:
        try:
            raw_json = client.fetch_sentiment(
                ticker=symbol
                limit=limit_per_request,
            )

            if raw_json.get("_rate_limited"):
                logger.warning(
                    f"Rate limited at {symbol} - stopping extraction"
                )



if __name__ == "__main__":
    run()