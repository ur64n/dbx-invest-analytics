from pyspark.sql import SparkSession
from src.config.logger import get_logger
from src.config.config_loader import load_config

from src.etl.schema.qqq_entities_schema import QQQ_SCHEMA, REQUIRED_RAW_COLUMNS

from src.etl.monitoring.pipeline_run_logger import PplLogger
from src.etl.extraction.qqq_entities_extraction import QQQEntitiesExtractor
from src.etl.cleaning.qqq_entities_cleaning import QQQEntitiesCleaner
from src.etl.utils.validation_helper import ValidationHelper
from src.etl.write.delta_table_writer import DeltaTableWriter

logger = get_logger("qqq_entities_raw_to_bronze_pipeline")

def run(env: str = "dev"):

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate() 
    cfg = load_config(env)
    ppl_name = "run_qqq_entities_raw_to_bronze"
    layer = "bronze"
    source_table = None
    target_table = cfg["tables"]["bronze_qqq"]

    # -------- monitoring --------
    ppl_logger = PplLogger(spark)
    ppl_logger.start(
        ppl_name,
        layer,
        source_table,
        target_table
    )

    try:
        # ---------- extraction ----------
        extractor = QQQEntitiesExtractor(
            spark=spark,
            raw_path=cfg["paths"]["raw_csv"],
            expected_filename=cfg["files"]["qqq_entities"],
            schema=QQQ_SCHEMA,
        )

        # ---------- read data ----------
        df = extractor.read()

        # -------- monitoring --------
        input_rows = df.count()

        # ---------- cleaning ----------
        df = QQQEntitiesCleaner.remove_invalid_rows(df)
        df = QQQEntitiesCleaner.rename_column(df)

        # ---------- validation ----------
        ValidationHelper.validate_schema(
            df, 
            REQUIRED_RAW_COLUMNS, 
            context="qqq_etf_constituents"
        )
        
        ValidationHelper.validate_not_empty(
            df,
            context="qqq_etf_constituents"
        )

        # ---------- write BRONZE ----------
        DeltaTableWriter(
            table_name=cfg["tables"]["bronze_qqq"],
            spark=spark
        ).overwrite(df)

        # -------- monitoring --------
        output_rows = df.count()
        rows_rejected = input_rows - output_rows

        ppl_logger.finish(
            input_rows,
            output_rows,
            rows_rejected,
            None
        )

    except Exception as e:
        ppl_logger.fail(str(e))
        raise

if __name__ == "__main__":
    run()