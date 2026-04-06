from pyspark.sql import SparkSession
from datetime import datetime, timezone
from uuid import uuid4
from src.config.logger import get_logger
from src.etl.schema.pipeline_logger_schema import PPL_LOGGER_SCHEMA

logger = get_logger("pipeline_run_logger")

class PplLogger:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.schema = PPL_LOGGER_SCHEMA
        self.table_name = "ops.pipeline_runs"

    def _write_row(self, row: dict) -> None:
        logger.info(f"")

        df = self.spark.createDataFrame(
            [row],
            self.schema
        )

        df.write.format("delta").mode("append").saveAsTable(self.table_name)
    
    def start(self, ppl_name, layer, source_table, target_table):
        logger.info(f"Pipeline {ppl_name} started | run_id={self.run_id}")

        self.run_id = str(uuid4())
        self.started_at = datetime.now(timezone.utc)
        self.ppl_name = ppl_name
        self.layer = layer
        self.source_table = source_table
        self.target_table = target_table

    def finish(self, input_rows, output_rows, rows_rejected, config_params):
        logger.info(f"Pipeline {self.ppl_name} finished | status=SUCCESS | duration={self.duration_seconds:.1f}s | in={input_rows} out={output_rows}")
        
        self.finished_at = datetime.now(timezone.utc)
        self.duration_seconds = (self.finished_at - self.started_at).total_seconds()

        row = {
            "run_id": self.run_id,
            "pipeline_name": self.ppl_name,
            "layer": self.layer,
            "status": "SUCCESS",
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "input_rows": input_rows,
            "output_rows": output_rows,
            "error_message": None,
            "duration_seconds": self.duration_seconds,
            "source_table": self.source_table,
            "target_table": self.target_table,
            "rows_rejected": rows_rejected,
            "config_params": config_params
        }

        self._write_row(row)
        logger.info(f"")


    def fail(self, error_message) -> None:
        self.finished_at = datetime.now(timezone.utc)
        self.duration_seconds = (self.finished_at - self.started_at).total_seconds()

        row = {
            "run_id": self.run_id,
            "pipeline_name": self.ppl_name,
            "layer": self.layer,
            "status": "FAILURE",
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "input_rows": None,
            "output_rows": None,
            "error_message": error_message,
            "duration_seconds": self.duration_seconds,
            "source_table": self.source_table,
            "target_table": self.target_table,
            "rows_rejected": None,
            "config_params": None,
        }

        self._write_row(row)
        
        logger.info(f"Pipeline {self.ppl_name} finished | status=FAILURE | duration={self.duration_seconds:.1f}s | error={error_message}")
