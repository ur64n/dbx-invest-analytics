from src.config.logger import get_logger
from pyspark.sql import DataFrame

logger = get_logger("ohlcv_writer")

class OHLCVWriter:
    def __init__(self, spark, path):
        self.spark = spark
        self.path = path


    def write_bronze_ohlcv(self, df: DataFrame):

        row_count = df.count()

        logger.info(
            f"Writing {row_count} rows to {self.path} "
            f"(mode=overwrite, format=delta)"
        )

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(self.path)
        )

        logger.info("Write completed successfully")