from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.schema.av_schema import av_sentiment_bronze_schema, BRONZE_REQUIRED_COLUMNS

from src.etl.transformations.av_json_parser import AvJsonParser
from src.etl.cleaning.av_sentiment_cleaning import AVSentimentCleaner
from src.etl.utils.validation_helper import ValidationHelper
from src.etl.write.delta_table_writer import DeltaTableWriter

from pyspark.sql.functions import col
from datetime import datetime
from pyspark.dbutils import DBUtils
import json

from src.etl.monitoring.pipeline_run_logger import PplLogger
from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.extraction.alpha_vantage.av_sentiment_history_client import AVSentimentHistoryClient

logger = get_logger("run_av_sentiment_history_api_to_bronze")

def run():

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()
    ppl_name = "run_av_sentiment_history_api_to_bronze"
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
    
    try:
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
        cutoff = datetime(2015, 6, 1)
        symbols = [s for s in symbols if s in coverage and coverage[s] > cutoff]

        # -------- monitoring --------
        config_params = json.dumps({
            "symbols_total": len(symbols),
            "cutoff_date": str(cutoff)
        })

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

        # -------- monitoring --------
        input_rows = df.count()

        # ---------- validation ----------
        ValidationHelper.validate_schema(
            df,
            BRONZE_REQUIRED_COLUMNS,
            context="bronze_av_sentiment"
        )

        ValidationHelper.validate_not_empty(
            df, 
            context="bronze_av_sentiment"
        )

        # ---------- cleaning ----------
        df = AVSentimentCleaner.drop_duplicates(df, ["symbol", "published_at", "title"])

        # ---------- write upsert ----------
        DeltaTableWriter(
            spark=spark,
            table_name=bronze_av_sentiment
        ).upsert(df, merge_keys=["symbol", "published_at", "title"])

        # -------- monitoring --------
        output_rows = df.count()
        rows_rejected = input_rows - output_rows

        ppl_logger.finish(
            input_rows,
            output_rows,
            rows_rejected,
            config_params
        )

    except Exception as e:
        ppl_logger.fail(str(e))
        raise

if __name__ == "__main__":
    run()
