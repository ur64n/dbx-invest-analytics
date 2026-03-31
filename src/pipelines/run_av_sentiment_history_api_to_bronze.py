from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.schema.av_schema import av_sentiment_bronze_schema

from src.etl.transformations.av_json_parser import AvJsonParser
from src.etl.cleaning.av_sentiment_cleaning import AVSentimentCleaner
from src.etl.validation.av_sentiment_validation import AVValidator
from src.etl.write.delta_table_writer import DeltaTableWriter

from pyspark.sql.functions import col
from pyspark.dbutils import DBUtils

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.extraction.alpha_vantage.av_sentiment_history_client import AVSentimentHistoryClient

logger = get_logger("run_av_sentiment_history_api_to_bronze")

def run():
    logger.info("Starting av sentiment history data api to bronze pipeline")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    # ---------- secrets ----------
    dbutils = DBUtils(spark)
    api_key = dbutils.secrets.get("my-scope", "API_KEY_AV")

    # ---------- tables ----------
    bronze_av_sentiment = config["tables"]["bronze_av_sentiment"]

    # ---------- load data ----------
    entities_df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["gold_ohlcv_with_dimension"]
    ).read()

    # ---------- collect symbols ----------
    symbols = [
        row["symbol"].upper()
        for row in(
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

    # ---------- finds last date & build dict (symbol:min_date) ----------
    coverage = {
        row["symbol"]: row["min_date"]
        for row in spark.table(bronze_av_sentiment)
        .groupBy("symbol")
        .agg({"published_at": "min"})
        .withColumnRenamed("min(published_at)", "min_date")
        .collect()
    }

    # ---------- sort list ----------
    symbols = [s for s in symbols if s in coverage]

    # ---------- extraction ----------
    client = AVSentimentHistoryClient(
        config=config,
        api_key=api_key
    )

    all_rows: list[dict] = []

    for symbol in symbols:
        try:
            min_date = coverage[symbol]
            time_to = min_date.strftime("%Y%m%dT%H%M")

            raw_json = client.fetch_sentiment(
                ticker=symbol,
                time_to=time_to
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

    # ---------- validation ----------
    AVValidator.validate_schema(df)
    AVValidator.validate_not_empty(df)

    # ---------- cleaning ----------
    df = AVSentimentCleaner.drop_duplicates(df, ["symbol", "published_at", "title"])

    # ---------- write upsert ----------
    DeltaTableWriter(
        spark=spark,
        table_name=bronze_av_sentiment
    ).upsert(df, merge_keys=["symbol", "published_at", "title"])

    logger.info("Aplha Vantage sentiment pipeline API to bronze finished successfully")

if __name__ == "__main__":
    run()
