from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.schema.sentiment_vs_returns_schema import SOURCE_SENTIMENT_COLUMNS, SOURCE_OHLCV_COLUMNS, REQUIRED_COLUMNS, KEY_COLUMNS

from pyspark.sql.functions import col

from src.etl.monitoring.pipeline_run_logger import PplLogger
from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.transformations.ohlcv_return_calculation import OHLCVCalculator
from src.etl.enrichment.sentiment_return_enrichment import SentimentReturnEnricher
from src.etl.utils.validation_helper import ValidationHelper
from src.etl.validation.gold_sentiment_returns_validation import GoldSentimentReturnValidator
from src.etl.write.delta_table_writer import DeltaTableWriter

logger = get_logger("run_gold_sentiment_vs_returns")

def run():

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()
    ppl_name = "run_gold_sentiment_vs_returns"
    layer = "gold"
    source_table = config["tables"]["silver_ohlcv"]
    target_table = config["tables"]["gold_sentiment_vs_returns"]

    # -------- monitoring --------
    ppl_logger = PplLogger(spark)
    ppl_logger.start(
        ppl_name,
        layer,
        source_table,
        target_table
    )

    try:
        # ---------- read data ----------    
        sentiment_df = DeltaTableExtractor(
            table_name=config["tables"]["gold_av_sentiment_aggregated"],
            spark=spark
        ).read().select(SOURCE_SENTIMENT_COLUMNS)

        ohlcv_df = DeltaTableExtractor(
            table_name=config["tables"]["silver_ohlcv"],
            spark=spark
        ).read().select(SOURCE_OHLCV_COLUMNS)

        # -------- monitoring --------
        input_rows = ohlcv_df.count()

        # ---------- transformations ---------- 
        ohlcv_daily_return_df = OHLCVCalculator.calculate_daily_return(ohlcv_df)

        # ---------- enrichment ---------- 
        enriched_df = SentimentReturnEnricher.enrich_sentiment_return(ohlcv_daily_return_df, sentiment_df)

        # ---------- validation ---------- 
        ValidationHelper.validate_row_after_join(ohlcv_daily_return_df, enriched_df)

        # ---------- drop ---------- 
        df = enriched_df.filter(col("avg_sentiment_score").isNotNull())

        # ---------- validation ---------- 
        ValidationHelper.validate_not_empty(df, context="gold_sentiment_vs_returns after null filter")
        ValidationHelper.validate_schema(df, REQUIRED_COLUMNS, context="gold_sentiment_vs_returns")
        ValidationHelper.validate_uniqueness(df, KEY_COLUMNS)
        GoldSentimentReturnValidator.validate_domain_rules(df)

        # ---------- write ----------
        DeltaTableWriter(
            table_name=config["tables"]["gold_sentiment_vs_returns"],
            spark=spark
        ).overwrite(df)

        # -------- monitoring --------
        output_rows = df.count()
        rows_rejected = input_rows - output_rows

        ppl_logger.finish(
            input_rows,
            output_rows,
            rows_rejected,
            config_params = None
        )

    except Exception as e:
        ppl_logger.fail(str(e))
        raise
    
if __name__ == "__main__":
    run()