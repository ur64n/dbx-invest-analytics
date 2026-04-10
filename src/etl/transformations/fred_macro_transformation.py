from pyspark.sql import DataFrame
from pyspark.sql.functions import first, date_format, avg
from src.config.logger import get_logger

from src.etl.schema.macro_impact_schema import AVG_IND_COLUMNS

logger = get_logger("fred_macro_transformation")

class FredMacroTransformer:
    """Calculates monthly return as (last_close - first_close) / first_close.

    Groups by year_month derived from date column.
    """
    
    @staticmethod
    def pivot(df: DataFrame) -> DataFrame:
        logger.info("Starting pivot transformation on fred_macro_indicators dataset")

        macro_wide_df = df.groupBy("date").pivot("indicator_id").agg(first("value"))

        return macro_wide_df

    @staticmethod
    def aggregate_monthly(df: DataFrame) -> DataFrame:
        logger.info("Aggregating macro indicators to monthly granularity")

        df = df.withColumn("year_month", date_format("date", "yyyy-MM"))
        df = df.groupBy("year_month").agg(
            *[avg(c).alias(c) for c in AVG_IND_COLUMNS]
        )

        return df