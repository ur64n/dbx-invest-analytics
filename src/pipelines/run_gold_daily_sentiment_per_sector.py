from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.schema.sentiment_sector_schema import SOURCE_SENTIMENT_COLUMNS, SOURCE_ENTITIES_COLUMNS

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.enrichment.sentiment_sector_enrichment import SentimentSectorEnricher

logger = get_logger("run_gold_daily_sentiment_per_sector")

def run():
    logger.info("Starting gold daily sentiment per sector anylysis pipeline")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    # ---------- read data ----------
    sector_df = DeltaTableExtractor(
        table_name=config["tables"]["silver_qqq_categoties"],
        spark=spark
    ).read().select(SOURCE_ENTITIES_COLUMNS)

    sentiment_df = DeltaTableExtractor(
        table_name=config["tables"]["gold_av_sentiment_aggregated"],
        spark=spark
    ).read().select(SOURCE_SENTIMENT_COLUMNS)

    # ---------- enrichment ----------
    enriched_df = SentimentSectorEnricher.enrich_sector_sentiment(sector_df, sentiment_df)

    enriched_df.limit(50).display()

    # ---------- cleaning ----------


if __name__ == "__main__":
    run()