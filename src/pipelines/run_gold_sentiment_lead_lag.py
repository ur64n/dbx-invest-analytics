from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from pyspark.sql.functions import col

from src.etl.schema.sentiment_lead_lag_schema import OHLCV_SOURCE_COLUMNS, REQUIRED_COLUMNS, KEY_COLUMNS

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.transformations.ohlcv_return_calculation import OHLCVCalculator
from src.etl.enrichment.sentiment_return_enrichment import SentimentReturnEnricher
from src.etl.utils.validation_helper import ValidationHelper
from src.etl.validation.gold_sentiment_lead_lag_validation import GoldSentimentLeadLagValidator
from src.etl.write.delta_table_writer import DeltaTableWriter

logger = get_logger("run_gold_sentiment_lead_lag")

def run():
    logger.info("Starting gold sentiment lead lag pipeline")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    # ---------- read data ----------
    sentiment_df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["gold_av_sentiment_aggregated"]
    ).read()

    ohlcv_df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["silver_ohlcv"]
    ).read().select(OHLCV_SOURCE_COLUMNS)

    # ---------- transformation ----------
    ohlcv_forward_returns = OHLCVCalculator.calculate_forward_returns(ohlcv_df)

    # ---------- ernichment ----------
    enriched_df = SentimentReturnEnricher.enrich_sentiment_return(ohlcv_forward_returns, sentiment_df)

    # ---------- validation ----------
    ValidationHelper.validate_row_after_join(ohlcv_forward_returns, enriched_df)

    # ---------- filter ----------
    df = enriched_df.filter(col("avg_sentiment_score").isNotNull())

    # ---------- validation ----------
    ValidationHelper.validate_schema(df, REQUIRED_COLUMNS, context="sentimen_lead_lag")
    ValidationHelper.validate_not_empty(df, context="sentimen_lead_lag")
    ValidationHelper.validate_uniqueness(df, KEY_COLUMNS)
    GoldSentimentLeadLagValidator.validate_domain_rules(df)

    # ---------- write ----------
    DeltaTableWriter(
        table_name=config["tables"]["gold_sentiment_lead_lag"],
        spark=spark
    ).overwrite(df)

    logger.info("Gold sentiment lead lag pipeline finished successfully")

if __name__ == "__main__":
    run()