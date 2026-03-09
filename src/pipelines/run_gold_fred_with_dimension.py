from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.enrichment.fred_dimension_enrichment import FredDimensionEnricher
from src.etl.write.delta_table_writer import DeltaTableWriter

logger = get_logger("gold_fred_with_dimension pipeline")

def run():
    logger.info("Starting gold_fred_with_dimension pipeline")
    
    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    # ---------- read data ----------
    fact_df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["silver_fred_macro_indicators"]
    ).read()

    dim_df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["silver_fred_macro_indicator_metadata"]
    ).read()

    # ---------- enrichment ----------
    df = FredDimensionEnricher.enrich_fred_dimension(fact_df, dim_df)
    logger.info("Enrichment completed successfully")

    # ---------- validation ----------
    assert df.count() == fact_df.count(), "Row count mismatch after join"

    # ---------- write ----------
    DeltaTableWriter(
        table_name=config["tables"]["gold_fred_with_dimension"],
        spark=spark
    ).overwrite(df)
    
    logger.info("gold_fred_with_dimension pipeline completed successfully")

if __name__ == "__main__":
    run()