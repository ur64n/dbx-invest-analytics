from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lead, lag
from pyspark.sql.window import Window
from src.config.logger import get_logger

logger = get_logger("ohlcv_return_calculation")

class OHLCVCalculator:
    """Calculates return metrics from OHLCV price data.

    - calculate_daily_return: day-over-day % change using lag(close).
    - calculate_forward_returns: T+1, T+2, T+5 forward returns using lead(close).

    Both methods operate within a Window partitioned by symbol, ordered by date.
    """
    
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
    
    @staticmethod
    def calculate_forward_returns(df: DataFrame) -> DataFrame:
        logger.info("Starting forward return calculation for T+1, T+2, T+5 horizons")

        window = Window.partitionBy("symbol").orderBy("date")
        horizons = [1, 2, 5]

        for h in horizons:
            df = df.withColumn(f"close_t{h}", lead("close", h).over(window))
            df = df.withColumn(f"return_t{h}", (col(f"close_t{h}") - col("close")) / col("close"))
            df = df.drop(f"close_t{h}")

        df = df.filter(col("return_t5").isNotNull())

        logger.info("Forward return calculation completed")

        return df