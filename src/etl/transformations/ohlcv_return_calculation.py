from pyspark.sql import DataFrame
from pyspark.sql.functions import lag, col
from pyspark.sql.window import Window
from src.config.logger import get_logger

logger = get_logger("ohlcv_return_calculation")

class OHLCVCalculator:

    @staticmethod
    def calculate_daily_return(df: DataFrame) -> DataFrame:
        logger.info("Starting daily return per symbol calculation")

        window = Window.partitionBy("symbol").orderBy("date")
        df = df.withColumn("prev_close", lag("close", 1).over(window))
        df = df.withColumn("daily_return", (col("close") - col("prev_close")) / col("prev_close"))
        df = df.filter(col("daily_return").isNotNull())
        df = df.drop(col("prev_close"))

        logger.info("Daily return calculation per symbol completed")

        return df