from pyspark.sql import SparkSession
from src.config.logger import get_logger
from src.config.config_loader import load_config

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.cleaning.av_sentiment_cleaning import AVSentimentCleaner
from src.etl.validation.av_sentiment_validation import AVValidator
from src.etl.write.delta_table_writer import DeltaTableWriter

logger = get_logger("av_sentiment_bronze_to_silver")

def run():
    logger.info("Starting Alpha Vantage sentiment pipeline bronze to silver")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    # ---------- read data ----------
    df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["bronze_av_sentiment"]
    ).read()

    # ---------- cleaning ----------
    df = AVSentimentCleaner.standardize_columns(df)

    # ---------- validation ----------
    AVValidator.validate_symbol_not_null(df)
    AVValidator.validate_uniqueness(df)
    AVValidator.validate_date_not_future(df)
    AVValidator.validate_negative_values(df)
    AVValidator.validate_score_label_consistency(df)
    AVValidator.validate_allowed_values(df)

    # ---------- upsert write ----------
    DeltaTableWriter(
        table_name=config["tables"]["silver_av_sentiment"],
        spark=spark
    ).upsert(df, merge_keys=["symbol", "published_at", "title"])

    logger.info("Aplha Vantage sentiment pipeline bronze to silver finished successfully")

if __name__ == "__main__":
    run()
