from src.config.logger import get_logger
from pyspark.sql import DataFrame

logger = get_logger("ohlcv_silver_writer")

class OHLCVSilverWriter:
    def __init__(self, spark, path):
        self.spark = spark
        self.path = path

    def write_silver_ohlcv(self, df: dataframe):
        
        logger.info("Writing silver ohlcv table")
        
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(self.path)
        )

        logger.info(f"Silver ohlcv written successfully to:  {self.path}")