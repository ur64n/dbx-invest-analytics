from src.config.logger import get_logger
from pyspark.sql import DataFrame

logger = get_logger("delta_table_writer")

class DeltaTableWriter:
    def __init__(self, table_name:str, spark):
        self.table_name = table_name
        self.spark = spark

    def overwrite(self, df:DataFrame) -> None:
        logger.info(f"Writing {df.count()} rows to {self.table_name}")

        df.write.format("delta").mode("overwrite").saveAsTable(self.table_name)
        