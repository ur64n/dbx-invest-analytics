from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from src.config.logger import get_logger

from src.etl.schema.qqq_entities_schema import not_null_columns

logger = get_logger("validation")

class QQQEntitiesValidator:

    @staticmethod
    def validate_schema(df: DataFrame, req_cols: {set}) -> None:
        logger.info("Validating required columns in dataset")

        missing = req_cols - set(df.columns)

        if missing:
            raise ValueError(f"Missing required column in dataset: {missing}")

    @staticmethod
    def validate_not_empty(df: DataFrame) -> None:
        logger.info("Validating raw CSV not empty")

        if df.count() == 0:
            raise ValueError("Raw CSV DataFrame is empty")

    @staticmethod
    def validate_column_values(df: DataFrame) -> None:
        logger.info("Validating column values")

        for c in not_null_columns:
            empty_count = df.filter(
                col(c).isNull() | (col(c) == lit(""))
            ).count()

            if empty_count > 0:
                if c == "Symbol":
                    raise ValueError(f"Found empty values in critical column: {c}")
                logger.warning(
                    f"Found {empty_count} empty values in column: {c}"
                )

    @staticmethod
    def validate_holding_range(df: DataFrame) -> None:
        logger.info("Validating holding range")

        if df.filter((col("precent_holding")) < 0 | (col("precent_holding") > 100)).count() > 0:
            raise ValueError("Found values outside of range 0-100 in precent_holding column")

        if df.filter(col("precent_holding") == 0).count() > 0:
            raise ValueError("Found 0 values in precent_holding column")

    @staticmethod
    def validate_symbol_uniqueness(df: DataFrame) -> None:
        logger.info("Validating symbol uniqueness")

        total = df.count()
        distinct = df.select("Symbol").distinct().count()

        if total != distinct:
            raise ValueError(
                f"Found {total - distinct} duplicate Symbol values"
            )

    # ---------- ENRICHMENT VALIDATION ----------

    @staticmethod
    def validate_enrichment_inputs(
        base_df: DataFrame, category_df: DataFrame
    ) -> None:
        logger.info("Validating enrichment inputs")

        base_required = {"symbol"}
        category_required = {"symbol", "sector", "industry"}

        missing_base = base_required - set(base_df.columns)
        missing_category = category_required - set(category_df.columns)

        if missing_base:
            raise ValueError(
                f"Missing columns in base dataframe: {missing_base}"
            )

        if missing_category:
            raise ValueError(
                f"Missing columns in category dataframe: {missing_category}"
            )

    @staticmethod
    def validate_missing_categories(
        total_symbols: int, category_df: DataFrame
    ) -> None:
        logger.warning("Validating missing enrichment categories")

        missing_sector = category_df.filter(
            category_df.sector.isNull()
        ).count()

        missing_industry = category_df.filter(
            category_df.industry.isNull()
        ).count()

        if missing_sector > 0 or missing_industry > 0:
            logger.warning(
                f"Missing sector={missing_sector}, industry={missing_industry}"
            )

    # ---------- SILVER VALIDATION ----------

    @staticmethod
    def validate_no_null_holdings(df: DataFrame) -> None:
        logger.info("Validating no null percent holdings")

        null_count = df.filter(df["precent_holding"].isNull()).count()

        if null_count > 0:
            logger.warning(
                f"Found {null_count} null values in precent_holding column"
            )
