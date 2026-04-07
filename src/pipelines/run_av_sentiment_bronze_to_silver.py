from pyspark.sql import SparkSession
from src.config.logger import get_logger
from src.config.config_loader import load_config

from src.etl.schema.av_schema import KEY_COLUMNS

from src.etl.monitoring.pipeline_run_logger import PplLogger
from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.cleaning.av_sentiment_cleaning import AVSentimentCleaner
from src.etl.utils.validation_helper import ValidationHelper
from src.etl.validation.av_sentiment_validation import AVValidator
from src.etl.write.delta_table_writer import DeltaTableWriter

logger = get_logger("av_sentiment_bronze_to_silver")

def run():

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()
    ppl_name = "run_av_sentiment_bronze_to_silver"
    layer = "silver"
    source_table = config["tables"]["bronze_av_sentiment"]
    target_table = config["tables"]["silver_av_sentiment"]

    # -------- monitoring --------
    ppl_logger = PplLogger(spark)
    ppl_logger.start(
        ppl_name,
        layer,
        source_table,
        target_table
    )

    try:
    # ---------- read data ----------
        df = DeltaTableExtractor(
            spark=spark,
            table_name=config["tables"]["bronze_av_sentiment"]
        ).read()

        # -------- monitoring --------
        input_rows = df.count()

        # ---------- cleaning ----------
        df = AVSentimentCleaner.standardize_columns(df)
        df = AVSentimentCleaner.drop_duplicates(df, ["symbol", "published_at", "title"])

        # ---------- validation ----------
        AVValidator.validate_symbol_not_null(df)
        ValidationHelper.validate_uniqueness(df, KEY_COLUMNS)
        AVValidator.validate_date_not_future(df)
        AVValidator.validate_negative_values(df)
        AVValidator.validate_score_label_consistency(df)
        AVValidator.validate_allowed_values(df)

        # ---------- upsert write ----------
        DeltaTableWriter(
            table_name=config["tables"]["silver_av_sentiment"],
            spark=spark
        ).upsert(df, merge_keys=["symbol", "published_at", "title"])

        # -------- monitoring --------
        output_rows = df.count()
        rows_rejected = input_rows - output_rows

        ppl_logger.finish(
            input_rows,
            output_rows,
            rows_rejected,
            config_params = None
        )

    except Exception as e:
        ppl_logger.fail(str(e))
        raise

if __name__ == "__main__":
    run()
