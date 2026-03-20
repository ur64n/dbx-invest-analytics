from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.schema.sentiment_vs_returns_schema import SOURCE_SENTIMENT_COLUMNS, SOURCE_OHLCV_COLUMNS, REQUIRED_COLUMNS, KEY_COLUMNS

from pyspark.sql.functions import col

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.transformations.ohlcv_return_calculation import OHLCVCalculator
from src.etl.enrichment.sentiment_return_enrichment import SentimentReturnEnricher
from src.etl.utils.validation_helper import ValidationHelper

logger = get_logger("run_gold_sentiment_vs_returns")

def run():
    logger.info("Starting gold sentiment vs returns pipeline")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    # ---------- read data ----------    
    sentiment_df = DeltaTableExtractor(
        table_name=config["tables"]["gold_av_sentiment_aggregated"],
        spark=spark
    ).read().select(SOURCE_SENTIMENT_COLUMNS)

    ohlcv_df = DeltaTableExtractor(
        table_name=config["tables"]["silver_ohlcv"],
        spark=spark
    ).read().select(SOURCE_OHLCV_COLUMNS)

    # ---------- transformations ---------- 
    ohlcv_daily_return_df = OHLCVCalculator.calculate_daily_return(ohlcv_df)

    ohlcv_daily_return_df.limit(1).display()
    sentiment_df.limit(1).display()

    # ---------- enrichment ---------- 
    enriched_df = SentimentReturnEnricher.enrich_sentiment_return(ohlcv_daily_return_df, sentiment_df)

    df = enriched_df.filter(col("avg_sentiment_score").isNotNull())

    df.limit(6).display()

    # ---------- validation ---------- 
    ValidationHelper.validate_not_empty(df, context="")
    ValidationHelper.validate_schema(df, REQUIRED_COLUMNS, context="")
    ValidationHelper.validate_uniqueness(df, KEY_COLUMNS)


if __name__ == "__main__":
    run()