from pyspark.sql import SparkSession

from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.transformations.ohlcv_transformation import OHLCVTransformer
from src.etl.validation.ohlcv_validation import OHLCVValidator
from src.etl.write.ohlcvSilverWriter import OHLCVSilverWriter

logger = get_logger("ohlcv_bronze_to_silver")

def run():
    logger.info("Starting ohlcv_bronze_to_silver pipeline")
    
    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    raw_ohlcv = config["tables"]["bronze_ohlcv"]
    silver_ohlcv = config["tables"]["silver_ohlcv"]

    # ---------- read data ----------
    df = spark.read.table(raw_ohlcv)

    # ---------- transformation ----------
    df = OHLCVTransformer.cast_column_types(df)
    df = OHLCVTransformer.normalize_symbol(df)

    # ---------- validation ----------
    OHLCVValidator.validate_schema(df)
    OHLCVValidator.validate_not_empty(df)
    OHLCVValidator.validate_unique_key(df)
    OHLCVValidator.validate_records_number_per_symbol(df)
    OHLCVValidator.validate_null_values(df)
    OHLCVValidator.validate_empty_values(df)
    OHLCVValidator.validate_domain_rules(df)

    # ---------- write data ----------
    ohlcv_silver_writer = OHLCVSilverWriter(
        spark=spark,
        path=silver_ohlcv
    )

    ohlcv_silver_writer.write_silver_ohlcv(df)

    logger.info("ohlcv_bronze_to_silver pipeline successfully completed")

if __name__ == "__main__":
    
    run()