from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
)

from src.config.logger import get_logger

logger = get_logger("fred_run_metadata")

class FredRunMetadataWriter:
    """
    Writes metadata about each FRED extraction run.
    No business logic, no validation.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.table_name = "bronze.fred_run_metadata"

        self.schema = StructType([
            StructField("series_id", StringType(), False),
            StructField("run_ts", TimestampType(), False),
            StructField("status", StringType(), False),
            StructField("xml_path", StringType(), True),
            StructField("error_message", StringType(), True),
        ]) #create schema, empty table

    def _write(self, row: dict):
        df = self.spark.createDataFrame([row], schema=self.schema)
        (df.write.format("delta").mode("append").saveAsTable(self.table_name))

    def write_success(self, series_id: str, run_ts, xml_path: str):
        logger.info(f"Recording SUCCESS for {series_id}")

        self._write({
            "series_id": series_id,
            "run_ts": run_ts,
            "status": "SUCCESS",
            "xml_path": xml_path,
            "error_message": None,
        })

    def write_failure(self, series_id: str, run_ts, error_message: str):
        logger.warning(f"Recording FAILURE for {series_id}")

        self._write({
            "series_id": series_id,
            "run_ts": run_ts,
            "status": "FAILURE",
            "xml_path": None,
            "error_message": error_message,
        })
