from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from src.config.logger import get_logger

logger = get_logger("validation")

class QQQEntitiesValidator:
    """
    Pure validation logic.
    No IO, no SparkSession, no filesystem access.
    """

    # ---------- RAW CSV VALIDATION ----------

    @staticmethod
    def validate_raw_csv_schema(df: DataFrame) -> None:
        logger.info("Validating raw CSV schema")

        required_cols = {"Symbol", "Name", "% Holding"}
        missing = required_cols - set(df.columns)

        if missing:
            raise ValueError(f"Missing required columns in raw CSV: {missing}")

    @staticmethod
    def validate_raw_csv_not_empty(df: DataFrame) -> None:
        logger.info("Validating raw CSV not empty")

        if df.count() == 0:
            raise ValueError("Raw CSV DataFrame is empty")


    # ---------- BRONZE QQQ VALIDATION ----------

    @staticmethod
    def validate_base_columns(df: DataFrame) -> None:
        logger.info("Validating base QQQ columns")

        required_cols = {"Symbol", "Name", "precent_holding"}
        missing = required_cols - set(df.columns)

        if missing:
            raise ValueError(f"Missing required columns: {missing}")

    @staticmethod
    def validate_column_values(df: DataFrame) -> None:
        logger.info("Validating column values")

        cols = ["Symbol", "Name", "precent_holding"]

        for c in cols:
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
