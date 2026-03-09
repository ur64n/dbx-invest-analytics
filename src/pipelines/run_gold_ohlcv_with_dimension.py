from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.enrichment.ohlcv_dimension_enrichment import OHLCVDimensionEnricher

logger = get_logger("gold_ohlcv_with_dimension")

def run():
    logger.info("Starting gold_ohlcv_with_dimension pipeline")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    # ---------- read data ----------
    ohlcv_df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["silver_ohlcv"]
    ).read()

    qqq_ent_df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["silver_qqq"]
    ).read()

    qqq_cat_df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["silver_qqq_categoties"]
    ).read()

    # ---------- enrichment ----------
    df = OHLCVDimensionEnricher.enrich_ohlcv_dimension(
        ohlcv_df, 
        qqq_ent_df, 
        qqq_cat_df
        )
    
    logger.info(f"OHLCV with dimension enrichment completed")
    
    assert df.count() == ohlcv_df.count(), "Row count mismatch after join"

    
if __name__ == "__main__":
    run()