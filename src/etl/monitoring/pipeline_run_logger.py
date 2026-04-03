from pyspark import SparkSession
from datetime import datetime, timezone
from src.config.logger import get_logger
from src.etl.schema.pipeline_logger_schema import PPL_LOGGER_SCHEMA

logger = get_logger("pipeline_run_logger")

class PplLogger:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.schema = PPL_LOGGER_SCHEMA

    