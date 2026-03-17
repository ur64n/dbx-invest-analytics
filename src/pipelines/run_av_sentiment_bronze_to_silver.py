from pyspark.sql import SparkSession
from src.config.logger import get_logger
from src.config.config_loader import load_config

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.cleaning.av_sentiment_cleaning import AVSentimentCleaner
from src.etl.validation.av_sentiment_validation import AVValidator

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

    df.limit(10).display()

    # ---------- validation ----------


if __name__ == "__main__":
    run()