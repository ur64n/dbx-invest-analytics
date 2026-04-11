from pyspark.sql import DataFrame
from pyspark.sql.functions import col, first, last, date_format
from src.config.logger import get_logger

logger = get_logger("ohlcv_monthly_aggregation")

class OHLCVMonthlyAggregator:

    @staticmethod
    def calculate_monthly_return(df: DataFrame) -> DataFrame:
        logger.info("Calculating monthly return for QQQ")

        df = df.withColumn("year_month", date_format("date", "yyyy-MM"))
        df = df.orderBy("date")

        monthly_df = df.groupBy("year_month").agg(
            first("close").alias("first_close"),
            last("close").alias("last_close")
        )

        monthly_df = monthly_df.withColumn(
            "monthly_return",
            (col("last_close") - col("first_close")) / col("first_close")
        )

        monthly_df = monthly_df.drop("first_close", "last_close")

        return monthly_df