from pyspark import SparkSession
from datetime import datetime, timezone
from uuid import uuid4
from src.config.logger import get_logger
from src.etl.schema.pipeline_logger_schema import PPL_LOGGER_SCHEMA

logger = get_logger("pipeline_run_logger")

class PplLogger:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.schema = PPL_LOGGER_SCHEMA

    def _write_row(self, row)
        
    
    def start(self, ppl_name, layer, source_table, target_table):
        self.run_id = str(uuid4())
        self.started_at = datetime.now(timezone.utc)
        self.ppl_name = ppl_name
        self.layer = layer
        self.source_table = source_table
        self.target_table = target_table

    def finish(self, input_rows, output_rows, rows_rejected, config_params):
        self.finished_at = datetime.now(timezone.utc)
        self.duration_seconds = (self.finished_at - self.started_at).total_seconds()

        row = {
            "run_id": self.run_id,
            "pipeline_name": self.ppl_name,
            "layer": self.layer,
            "status": "success",
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "input_rows": self.input_rows,
            "output_rows": self.output_rows,
            "error_message": None,
            "duration_seconds": self.duration_seconds,
            "source_table": self.source_table,
            "target_table": self.target_table,
            "rows_rejected": self.rows_rejected,
            "config_params": self.config_params
        }

        self._write_row(row)
        logger.info(f"")
