from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.schema.av_schema import GOLD_REQUIRED_COLUMNS

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.transformations.av_sentiment_aggregation import AVSentimentAggregator
from src.etl.utils.validation_helper import ValidationHelper
from src.etl.validation.gold_sentiment_validation import GoldAVSentimentalidator

logger = get_logger("gold_sentiment_daily_agg")

def run():
    logger.info("Starting gold sentiment daily aggregation pipeline")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    # ---------- read data ----------
    df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["silver_av_sentiment"]
    ).read()

    # ---------- aggregation ----------
    df = AVSentimentAggregator.aggregate_daily(df)

    # ---------- validation ----------
    ValidationHelper.validate_not_empty(df, context="")
    ValidationHelper.validate_schema(df, GOLD_REQUIRED_COLUMNS, context="gold sentiment aggregated df")
    GoldAVSentimentalidator.validate_uniqueness(df)
    GoldAVSentimentalidator.validate_domain_rules(df)
    
    # ---------- write ----------
    

if __name__ == "__main__":
    run()