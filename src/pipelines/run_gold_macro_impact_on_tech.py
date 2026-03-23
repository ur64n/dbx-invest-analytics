from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor

logger = get_logger("gold_macro_impact_on_tech")

def run():
    logger.info("Starting gold_macro_impact_on_tech pipeline")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    # ---------- read data ----------
    macro_df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["silver_fred_macro_indicators"]
    ).read()

    ohlcv_df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["silver_ohlcv"]
    ).read()

    # ----------  ----------

if __name__ == "__main__":
    run()