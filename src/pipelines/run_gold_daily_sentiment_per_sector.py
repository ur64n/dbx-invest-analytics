from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.schema.sentiment_sector_schema import SOURCE_SENTIMENT_COLUMNS, SOURCE_ENTITIES_COLUMNS, UNIQUE_KEY

from src.etl.monitoring.pipeline_run_logger  import PplLogger
from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.enrichment.sentiment_sector_enrichment import SentimentSectorEnricher
from src.etl.transformations.sentiment_sector_aggregation import SentimentSectorAggregator
from src.etl.utils.validation_helper import ValidationHelper
from src.etl.validation.gold_sentiment_sector_validation import GoldSentimentSectorValidator
from src.etl.write.delta_table_writer import DeltaTableWriter

logger = get_logger("run_gold_daily_sentiment_per_sector")

def run():

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()
    ppl_name = "run_gold_daily_sentiment_per_sector"
    layer = "gold"
    source_table = config["tables"]["gold_av_sentiment_aggregated"]
    target_table = config["tables"]["gold_av_sentiment_sector_daily"]

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
        sector_df = DeltaTableExtractor(
            table_name=config["tables"]["silver_qqq_categoties"],
            spark=spark
        ).read().select(SOURCE_ENTITIES_COLUMNS)

        sentiment_df = DeltaTableExtractor(
            table_name=config["tables"]["gold_av_sentiment_aggregated"],
            spark=spark
        ).read().select(SOURCE_SENTIMENT_COLUMNS)

        # -------- monitoring --------
        input_rows = sentiment_df.count()
        
        # ---------- enrichment ----------
        enriched_df = SentimentSectorEnricher.enrich_sector_sentiment(sector_df, sentiment_df)

        total_rows = enriched_df.count()

        logger.info(f"Rows before sector filter: {total_rows}")

        # ---------- cleaning ----------
        df = enriched_df.filter(col("sector").isNotNull())

        rows = df.count()

        logger.info(f"Rows after sector filter: {rows} | dropped: {total_rows - rows}")

        # ---------- transformation ----------
        df = SentimentSectorAggregator.aggregate_daily_sentiment_per_sector(df)

        # ---------- validation ----------
        ValidationHelper.validate_not_empty(df, context="gold_sentiment_sector_daily")
        ValidationHelper.validate_uniqueness(df, UNIQUE_KEY)
        GoldSentimentSectorValidator.validate_negative_values(df)
        GoldSentimentSectorValidator.validate_value_ranges(df)

        # ---------- write ----------
        DeltaTableWriter(
            spark=spark,
            table_name=config["tables"]["gold_av_sentiment_sector_daily"]
        ).overwrite(df)

        # ---------- monitoring ----------
        output_rows = df.count()
        rows_rejected = total_rows - rows
        config_params = None

        ppl_logger.finish(
            input_rows,
            output_rows,
            rows_rejected,
            config_params
        )

    except Exception as e:
        ppl_logger.fail(str(e))
        raise

if __name__ == "__main__":
    run()