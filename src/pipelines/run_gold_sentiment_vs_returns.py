from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor

logger = get_logger("run_gold_sentiment_vs_returns")

def run():
    logger.info("Starting gold sentiment vs returns pipeline")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    # ---------- read data ----------    
    avdsa_df = DeltaTableExtractor(
        table_name=config["tables"]["gold_av_sentiment_sector_daily"],
        spark=spark
    ).read()

    ohlcv_df = DeltaTableExtractor(
        table_name=config["tables"]["silver_ohlcv"],
        spark=spark
    )



    # ---------- enrichment ---------- 
    


if __name__ == "__main__":
    run()