from pyspark.sql import DataFrame
from pyspark.sql.functions import x
from src.config.logger import get_logger

logger = get_logger("ohlcv_return_calculation")

class OHLCVCalculator:

    def xyz(df: DataFrame) -> DataFrame:
        logger.info("Starting...")

        