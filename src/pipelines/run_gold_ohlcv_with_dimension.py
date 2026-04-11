from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.monitoring.pipeline_run_logger import PplLogger
from src.etl.extraction.delta_table_extractor import DeltaTableExtractor
from src.etl.enrichment.ohlcv_dimension_enrichment import OHLCVDimensionEnricher
from src.etl.utils.validation_helper import ValidationHelper
from src.etl.validation.gold_ohlcv_validation import GoldOHLCVValidator
from src.etl.write.delta_table_writer import DeltaTableWriter

logger = get_logger("gold_ohlcv_with_dimension")

def run():

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()
    ppl_name = "run_gold_ohlcv_with_dimension"
    layer = "gold"
    source_table = config["tables"]["silver_ohlcv"]
    target_table = config["tables"]["gold_ohlcv_with_dimension"]

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
        ohlcv_df = DeltaTableExtractor(
            spark=spark,
            table_name=config["tables"]["silver_ohlcv"]
        ).read()

        qqq_ent_df = DeltaTableExtractor(
            spark=spark,
            table_name=config["tables"]["silver_qqq"]
        ).read()

        qqq_cat_df = DeltaTableExtractor(
            spark=spark,
            table_name=config["tables"]["silver_qqq_categoties"]
        ).read()

        # -------- monitoring --------
        input_rows = ohlcv_df.count()

        # ---------- enrichment ----------
        df = OHLCVDimensionEnricher.enrich_ohlcv_dimension(
            ohlcv_df, 
            qqq_ent_df, 
            qqq_cat_df
            )

        logger.info(f"OHLCV with dimension enrichment completed")

        # ---------- validation ----------
        ValidationHelper.validate_row_after_join(ohlcv_df, df)
        GoldOHLCVValidator.validate_qqq_null_values(df)
        GoldOHLCVValidator.validate_benchmark_null_values(df)

        # ---------- write ----------
        DeltaTableWriter(
            table_name=config["tables"]["gold_ohlcv_with_dimension"],
            spark=spark
        ).overwrite(df)

        # -------- monitoring --------
        output_rows = df.count()
        rows_rejected = 0

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