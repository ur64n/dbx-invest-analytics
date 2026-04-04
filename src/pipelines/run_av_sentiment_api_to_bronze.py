from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional
import json

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pyspark.sql.functions import col

from src.config.logger import get_logger
from src.config.config_loader import load_config

from src.etl.schema.av_schema import av_sentiment_bronze_schema, BRONZE_REQUIRED_COLUMNS

from src.etl.monitoring.pipeline_run_logger import PplLogger
from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.extraction.alpha_vantage.av_sentiment_client import AlphaVantageSentimentClient
from src.etl.transformations.av_json_parser import AvJsonParser
from src.etl.cleaning.av_sentiment_cleaning import AVSentimentCleaner
from src.etl.utils.validation_helper import ValidationHelper
from src.etl.write.delta_table_writer import DeltaTableWriter

logger = get_logger("av_sentiment_api_to_bronze")

def run():
    logger.info("Starting Aplha Vantage sentiment pipeline API to bronze")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()
    ppl_name = "av_sentiment_api_to_bronze"
    layer = "bronze"
    source_table = None
    target_table = config["tables"]["bronze_av_sentiment"]

    # -------- monitoring --------
    ppl_logger = PplLogger(spark)
    ppl_logger.start(
        ppl_name, 
        layer, 
        source_table, 
        target_table
    )

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

    # ---------- collect symbols ----------
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

    # -------- monitoring --------
    config_params = json.dumps({
        "symbol_count": len(symbols),
        "bootstrap_needed": sum(1 for s in symbols if s not in coverage),
    })

    # ---------- extraction ----------
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
    
    # ---------- parsing ----------
    parsed_rows = list(AvJsonParser.parse(all_rows, set(symbols)))

    # ---------- create df ----------
    df = spark.createDataFrame(parsed_rows, schema=av_sentiment_bronze_schema)

    # ---------- Validation ----------
    ValidationHelper.validate_schema(
        df,
        required_columns=BRONZE_REQUIRED_COLUMNS,
        context="bronze_av_sentiment")
    
    ValidationHelper.validate_not_empty(
        df,
        context="bronze_av_sentiment"
        )

    # ---------- Cleaning ----------
    df = AVSentimentCleaner.drop_duplicates(df, ["symbol", "published_at", "title"])

    input_rows = df.count()

    # ---------- Write upsert ----------
    DeltaTableWriter(
        spark=spark,
        table_name=bronze_av_sentiment
    ).upsert(df, merge_keys=["symbol", "published_at", "title"])

    # -------- monitoring --------
    output_rows = df.count()
    rows_rejected = input_rows - output_rows

    ppl_logger = PplLogger(spark)

    try:
        ppl_logger.start(
            ppl_name, 
            layer, 
            source_table, 
            target_table
        )
    
    ppl_logger.finish(
        input_rows,
        output_rows,
        rows_rejected,
        config_params
    )

    logger.info("Aplha Vantage sentiment pipeline API to bronze finished successfully")

if __name__ == "__main__":
    run()