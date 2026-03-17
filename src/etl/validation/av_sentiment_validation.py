from pyspark.sql import DataFrame
from src.config.logger import get_logger

from pyspark.sql.functions import current_date, col, current_timestamp

from src.etl.schema.av_schema import BRONZE_REQUIRED_COLUMNS, ALLOWED_VALUES

logger = get_logger("av_validation")

class AVValidator:

    # ---------- bronze validation ----------
    @staticmethod
    def validate_schema(df: DataFrame) -> None:
        logger.info("Validating AV required columns")

        missing = BRONZE_REQUIRED_COLUMNS - set(df.columns)

        if missing:
            raise ValueError(f"Missing required columns: {missing}")

    @staticmethod
    def validate_not_empty(df: DataFrame) -> None:
        logger.info("Validating AV dataset is not empty")

        if df.limit(1).count() == 0:
            raise ValueError("AV dataset is empty")

    # ---------- silver validation ----------
    @staticmethod
    def validate_symbol_not_null(df: DataFrame) -> None:
        logger.info("Validating symbol column not null")

        if df.filter(col("symbol").isNull()).head(1):
            raise ValueError("symbol column contains nulls")

    @staticmethod
    def validate_uniqueness(df: DataFrame) -> None:
        logger.info("Validating uniqueness (symbol, published_at, title)")

        total = df.count()
        distinct = df.select("symbol", "published_at", "title").distinct().count()

        if total != distinct:
            raise ValueError(f"Duplicates in dataset on key columns: {total - distinct}")
    
    @staticmethod
    def validate_date_not_future(df: DataFrame) -> None:
        logger.info("Validating date columns not from future")

        if df.filter(col("date") > current_date()).head(1):
            raise ValueError("date contains future values")

        if df.filter(col("published_at") > current_timestamp()).head(1):
            raise ValueError("published_at contains future values")
    
    @staticmethod
    def validate_negative_values(df: DataFrame) -> None:
        logger.info("Validating negative values")

        if df.filter((col("ticker_relevance_score") < 0) | (col("ticker_relevance_score") > 1)).head(1):
            raise ValueError("ticker_relevance_score out of range 0 to 1")

        if df.filter((col("ticker_sentiment_score") < -1) | (col("ticker_sentiment_score") > 1)).head(1):
            raise ValueError("ticker_sentiment_score out of range -1 to 1")
    
    @staticmethod
    def validate_score_label_consistency(df: DataFrame) -> None:
        logger.info("Validating score label consistency")

        if df.filter(
            (col("ticker_sentiment_score") < -0.15) & 
            (~col("ticker_sentiment_label").isin("bearish","somewhat-bearish"))
            ).head(1):
            
            raise ValueError("Inconsistent sentiment score and label detected")

        if df.filter(
            (col("ticker_sentiment_score") > 0.15) &
            (~col("ticker_sentiment_label").isin("bullish", "somewhat-bullish"))
            ).head(1):

            raise ValueError("Inconsistent sentiment score and label detected")
    
    @staticmethod    
    def validate_allowed_values(df: DataFrame) -> None:
        logger.info("Validating allowed values")

        invalid_df = df.filter(
            (~col("ticker_sentiment_label").isin(ALLOWED_VALUES)) |
            (~col("article_overall_sentiment_label").isin(ALLOWED_VALUES))
        )

        if invalid_df.head(1):
            raise ValueError("Invalid sentiment label detected")

    
