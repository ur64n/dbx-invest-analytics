from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor

logger = get_logger("gold_ohlcv_with_dimension")

def run():
    logger.info("Starting gold_ohlcv_with_dimension pipeline")

    # ---------- setup ----------
    spark = sparkSession.builder.getOrCreate()
    config = load_config()

    # ---------- read data ----------
    ohlcv_df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["ohlcv_indicators"]
    ).read()

    qqq_ent_df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["qqq_entities"]
    ).read()

    qqq_cat_df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["qqq_categories"]
    ).read()

    

if __name__ == "__main__":
    run()