from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pyspark.sql.functions import col

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
    dbutils = DBUtils(spark)
    api_key = dbutils.secrets.get("my-scope","API_KEY_AV")

    # ---------- parameters ----------
    refresh_window_months = config["alpha_vantage"]["window_refresh_months"]


    run_ts = datetime.now(ZoneInfo("Europe/Warsaw"))

    # ---------- tables ----------
    bronze_av_sentiment = config["tables"]["bronze_av_sentiment"]
    silver_av_sentiment = config["tables"]["silver_av_sentiment"]

    # ---------- load symbols ----------
    entities_df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["ohlcv_with_dimension"]
    ).read()

    symbols = [
        row["symbol"].upper()
        for row in (
            entities_df
            .filter(col("sector") == "technology")
            .orderBy(col("percent_holding").desc())
            .select("symbol")
            .distinct()
            .limit(25)
            .collect()
        )
    ]

    logger.info(f"Total symbols to fetch sentiment for {len(symbols)}")

    # ---------- window refresh logic ----------
    exists = spark.catalog.tableExists(silver_av_sentiment)

    has_data = (
        exists
        and spark.table(silver_av_sentiment).limit(1).count() > 0
    )
    if not has_data:
        observation_start = None
    else:
        observation_start = (
            datetime.utcnow() - timedelta(days=30 * refresh_window_months)
        ).strftime("%Y%m%dT0000")

    logger.info(
        f"Extraction mode: {'BOOTSTRAP' if not has_data else 'REFRESH'} | "
        f"Observation start date = {observation_start}"
    )

    # ---------- extraction + parsing ----------
    client = AlphaVantageSentimentClient(
        config=config,
        api_key=api_key
    )

    #parser = AlphaVantageSentimentParser()

    all_rows: list[dict] = []

    for symbol in symbols:
        try:
            raw_json = client.fetch_sentiment(
                ticker=symbol,
                time_from=observation_start
                )

if __name__ == "__main__":
    run()