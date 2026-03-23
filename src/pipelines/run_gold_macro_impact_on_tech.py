from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.schema.macro_impact_schema import OHLCV_SOURCE_COLUMNS, REQUIRED_COLUMNS, KEY_COLUMNS

from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.transformations.ohlcv_return_calculation import OHLCVCalculator
from src.etl.transformations.ohlcv_monthly_aggregation import OHLCVMonthlyAggregator
from src.etl.transformations.fred_macro_transformation import FredMacroTransformer
from src.etl.enrichment.macro_return_enrichment import MacroReturnEnricher
from src.etl.utils.validation_helper import ValidationHelper
from src.etl.validation.gold_macro_impact_validation import GoldMacroImpactValidator
from src.etl.write.delta_table_writer import DeltaTableWriter

logger = get_logger("gold_macro_impact_on_tech")

def run():
    logger.info("Starting gold_macro_impact_on_tech pipeline")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    # ---------- read data ----------
    macro_df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["silver_fred_macro_indicators"]
    ).read()

    ohlcv_df = DeltaTableExtractor(
        spark=spark,
        table_name=config["tables"]["silver_ohlcv"]
    ).read().filter(col("symbol") == "qqq").select(OHLCV_SOURCE_COLUMNS)

    # ---------- transformations ----------
    ohlcv_df = OHLCVCalculator.calculate_daily_return(ohlcv_df)
    ohlcv_monthly_df = OHLCVMonthlyAggregator.calculate_monthly_return(ohlcv_df)
    macro_monthly_df = FredMacroTransformer.pivot(macro_df)
    macro_monthly_df = FredMacroTransformer.aggregate_monthly(macro_monthly_df)

    # ---------- enrichment ----------
    df = MacroReturnEnricher.enrich(ohlcv_monthly_df, macro_monthly_df)

    # ---------- validation ----------
    ValidationHelper.validate_not_empty(df, context="gold_macro_impact_on_tech")
    ValidationHelper.validate_schema(df, REQUIRED_COLUMNS, context="gold_macro_impact_on_tech")
    ValidationHelper.validate_uniqueness(df, KEY_COLUMNS)
    GoldMacroImpactValidator.validate_domain_rules(df)

    # ---------- write ----------
    DeltaTableWriter(
        table_name=config["tables"]["gold_macro_impact_on_tech"],
        spark=spark
    ).overwrite(df)

    logger.info("gold_macro_impact_on_tech pipeline completed successfully")

if __name__ == "__main__":
    run()