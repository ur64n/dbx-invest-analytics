from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.schema.sentiment_vs_returns_schema import SOURCE_SENTIMENT_COLUMNS, SOURCE_OHLCV_COLUMNS

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor

logger = get_logger("run_gold_sentiment_vs_returns")

def run():
    logger.info("Starting gold sentiment vs returns pipeline")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    # ---------- read data ----------    
    avdsa_df = DeltaTableExtractor(
        table_name=config["tables"]["gold_av_sentiment_aggregated"],
        spark=spark
    ).read().select(SOURCE_SENTIMENT_COLUMNS)

    ohlcv_df = DeltaTableExtractor(
        table_name=config["tables"]["silver_ohlcv"],
        spark=spark
    ).read().select(SOURCE_OHLCV_COLUMNS)
    
    avdsa_df.limit(1).display()
    ohlcv_df.limit(1).display()

    # ---------- enrichment ---------- 
    


if __name__ == "__main__":
    run()