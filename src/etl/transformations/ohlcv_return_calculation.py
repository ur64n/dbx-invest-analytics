from pyspark.sql import DataFrame
from pyspark.sql.functions import lag, col
from pyspark.sql.window import Window
from src.config.logger import get_logger

logger = get_logger("ohlcv_return_calculation")

class OHLCVCalculator:

    def xyz(df: DataFrame) -> DataFrame:
        logger.info("Starting...")

        window = Window.partitionBy("symbol").orderBy("date")
        df = df.withColumn("prev_close", lag("close", 1).over(window))

        