from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.schema.fred_schema import KEY_COLUMNS

from src.etl.monitoring.pipeline_run_logger import PplLogger
from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.enrichment.fred_dimension_enrichment import FredDimensionEnricher
from src.etl.validation.gold_fred_validation import GoldFredValidator
from src.etl.validation.fred_validation import FredValidator
from src.etl.utils.validation_helper import ValidationHelper
from src.etl.write.delta_table_writer import DeltaTableWriter

logger = get_logger("gold_fred_with_dimension pipeline")

def run():

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()
    ppl_name = "run_gold_fred_with_dimension"
    layer = "gold"
    source_table = config["tables"]["silver_fred_macro_indicators"]
    target_table = config["tables"]["gold_fred_with_dimension"]

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
        fact_df = DeltaTableExtractor(
            spark=spark,
            table_name=config["tables"]["silver_fred_macro_indicators"]
        ).read()

        dim_df = DeltaTableExtractor(
            spark=spark,
            table_name=config["tables"]["silver_fred_macro_indicator_metadata"]
        ).read()

        # -------- monitoring --------
        input_rows = fact_df.count()

        # ---------- enrichment ----------
        df = FredDimensionEnricher.enrich_fred_dimension(fact_df, dim_df)

        logger.info("Enrichment completed successfully")

        # ---------- validation ----------
        ValidationHelper.validate_uniqueness(df, KEY_COLUMNS)
        ValidationHelper.validate_row_after_join(fact_df, df)
        GoldFredValidator.validate_nulls(df)

        # ---------- write ----------
        DeltaTableWriter(
            table_name=config["tables"]["gold_fred_with_dimension"],
            spark=spark
        ).overwrite(df)

        # -------- monitoring --------
        output_rows = df.count()
        rows_rejected = input_rows - output_rows
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