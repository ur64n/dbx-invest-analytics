from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.schema.av_schema import GOLD_REQUIRED_COLUMNS, GOLD_KEY_COLUMNS

from src.etl.monitoring.pipeline_run_logger import PplLogger
from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.transformations.av_sentiment_aggregation import AVSentimentAggregator
from src.etl.utils.validation_helper import ValidationHelper
from src.etl.validation.gold_sentiment_validation import GoldAVSentimentValidator
from src.etl.write.delta_table_writer import DeltaTableWriter

logger = get_logger("gold_sentiment_daily_agg")

def run():

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()
    ppl_name = "run_gold_sentiment_daily_agg"
    layer = "gold"
    source_table = config["tables"]["silver_av_sentiment"]
    target_table = config["tables"]["gold_av_sentiment_aggregated"]

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
        df = DeltaTableExtractor(
            spark=spark,
            table_name=config["tables"]["silver_av_sentiment"]
        ).read()

        # -------- monitoring --------
        input_rows = df.count()

        # ---------- aggregation ----------
        df = AVSentimentAggregator.aggregate_daily(df)

        # ---------- validation ----------
        ValidationHelper.validate_not_empty(df, context="")
        ValidationHelper.validate_schema(df, GOLD_REQUIRED_COLUMNS, context="gold sentiment aggregated df")
        ValidationHelper.validate_uniqueness(df, GOLD_KEY_COLUMNS)
        GoldAVSentimentValidator.validate_domain_rules(df)
        
        # ---------- write ----------
        DeltaTableWriter(
            table_name=config["tables"]["gold_av_sentiment_aggregated"],
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