from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from pyspark.sql.functions import col

from src.etl.schema.sentiment_lead_lag_schema import OHLCV_SOURCE_COLUMNS, REQUIRED_COLUMNS, KEY_COLUMNS

from src.etl.monitoring.pipeline_run_logger import PplLogger
from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.transformations.ohlcv_return_calculation import OHLCVCalculator
from src.etl.enrichment.sentiment_return_enrichment import SentimentReturnEnricher
from src.etl.utils.validation_helper import ValidationHelper
from src.etl.validation.gold_sentiment_lead_lag_validation import GoldSentimentLeadLagValidator
from src.etl.write.delta_table_writer import DeltaTableWriter

logger = get_logger("run_gold_sentiment_lead_lag")

def run():

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()
    ppl_name = "run_gold_sentiment_lead_lag"
    layer = "gold"
    source_table = config["tables"]["silver_ohlcv"]
    target_table = config["tables"]["gold_sentiment_lead_lag"]

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
            spark=spark,
            table_name=config["tables"]["gold_av_sentiment_aggregated"]
        ).read()

        ohlcv_df = DeltaTableExtractor(
            spark=spark,
            table_name=config["tables"]["silver_ohlcv"]
        ).read().select(OHLCV_SOURCE_COLUMNS)

        # -------- monitoring --------
        input_rows = ohlcv_df.count()

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