
from src.config.logger import get_logger
from pyspark.sql import DataFrame

logger = get_logger("DeltaTableExtractor")

class DeltaTableExtractor:
    def __init__(self, spark, table_name: str):
        self.spark = spark
        self.table_name = table_name

    def read(self) -> DataFrame:
        logger.info(f"Reading delta table {self.table_name}")
        
        return self.spark.read.table(self.table_name)