from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pyspark.sql.functions import col

from src.config.logger import get_logger
from src.config.config_loader import load_config

from src.etl.schema.av_schema import av_sentiment_bronze_schema

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.extraction.alpha_vantage.av_sentiment_client import AlphaVantageSentimentClient
from src.etl.transformations.av_json_parser import AvJsonParser
from src.etl.validation.av_sentiment_validation import AVValidator
from src.etl.write.delta_table_writer import DeltaTableWriter

logger = get_logger("av_sentiment_api_to_bronze")

def run():
    logger.info("Starting Aplha Vantage sentiment pipeline API to bronze")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    # ---------- secrets ----------
    dbutils = DBUtils(spark)
    api_key = dbutils.secrets.get("my-scope","API_KEY_AV")

    # ---------- tables ----------
    bronze_av_sentiment = config["tables"]["bronze_av_sentiment"]

    # ---------- load symbols ----------
    entities_df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["gold_ohlcv_with_dimension"]
    ).read()

    symbols = [
        row["symbol"].upper()
        for row in (
            entities_df
            .filter(col("sector") == "technology")
            .orderBy(col("percent_holding").desc())
            .select("symbol")
            .distinct()
            .limit(50)
            .collect()
        )
    ]

    logger.info(f"Total symbols to fetch sentiment for {len(symbols)}")

    # ---------- coverage check per symbol ----------
    coverage = {}
    if spark.catalog.tableExists(bronze_av_sentiment):
        coverage = {
            row["symbol"]: row["max_date"]
            for row in spark.table(bronze_av_sentiment)
            .groupBy("symbol")
            .agg({"published_at": "max"})
            .withColumnRenamed("max(published_at)", "max_date")
            .collect()
        }

    # ---------- sort: bootstrap symbols first ----------
    symbols.sort(key=lambda s: 0 if s not in coverage else 1)

    logger.info(
        f"Coverage: {len(coverage)} symbols in bronze | "
        f"Bootstrap needed: {sum(1 for s in symbols if s not in coverage)}"
    )

    # ---------- extraction + parsing ----------
    client = AlphaVantageSentimentClient(
        config=config,
        api_key=api_key
    )

    all_rows: list[dict] = []

    for symbol in symbols:
        try:
            max_date = coverage.get(symbol)
            time_from = None if max_date is None else max_date.strftime("%Y%m%dT%H%M")

            logger.info(f"{'BOOTSTRAP' if max_date is None else 'REFRESH'} for {symbol}")

            raw_json = client.fetch_sentiment(
                ticker=symbol,
                time_from=time_from
            )

            if raw_json.get("_rate_limited"):
                logger.warning(f"Rate limit hit at {symbol} — stopping extraction")
                break

            all_rows.extend(raw_json.get("feed", []))

        except Exception as e:
            logger.error(f"Extraction failed for {symbol}", exc_info=True)

    parsed_rows = list(AvJsonParser.parse(all_rows, set(symbols)))
    df = spark.createDataFrame(parsed_rows, schema=av_sentiment_bronze_schema)

    # ---------- Validation ----------
    AVValidator.validate_schema(df)
    AVValidator.validate_not_empty(df)

    # ---------- Write ----------
    DeltaTableWriter(
        spark=spark,
        table_name=bronze_av_sentiment
    ).upsert(df, merge_keys=["symbol", "published_at", "title"])

    logger.info("Aplha Vantage sentiment pipeline API to bronze finished successfully")

if __name__ == "__main__":
    run()