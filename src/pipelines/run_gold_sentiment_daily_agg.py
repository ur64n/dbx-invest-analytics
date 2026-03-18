from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.transformations.av_sentiment_aggregation import AVSentimentAggregator

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
    

if __name__ == "__main__":
    run()