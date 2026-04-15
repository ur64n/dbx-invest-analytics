from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

import json

from src.etl.monitoring.pipeline_run_logger import PplLogger
from src.etl.retention.delta_vacuum import DeltaVacuum

logger = get_logger("run_vacuum")

def run():

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()
    ppl_name = "run_vacuum"
    layer = "maintenance"

    # ---------- monitoring ----------
    ppl_logger = PplLogger(spark)
    ppl_logger.start(
        ppl_name,
        layer,
        source_table=None,
        target_table=None
    )

    try:
        # ---------- config ----------
        retention_hours = config["vacuum"]["retention_hours"]
        table_keys = config["vacuum"]["tables"]

        # ---------- vacuum ----------
        for table_key in table_keys:

            table_name = config["tables"][table_key]
            DeltaVacuum.vacuum_table(spark, table_name, retention_hours)

        # -------- monitoring --------
        ppl_logger.finish(
            input_rows=0,
            output_rows=0,
            rows_rejected=0,
            config_params=json.dumps({
                "tables_vacuumed": len(table_keys),
                "retention_hours": retention_hours
            })
        )

    except Exception as e:
        ppl_logger.fail(str(e))
        raise

if __name__ == "__main__":
    run()