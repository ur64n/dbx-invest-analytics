from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, trim

class OHLCVTransformer:

    @staticmethod
    def cast_column_types(df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("date", col("date").cast("date"))
            .withColumn("open", col("open").cast("double"))
            .withColumn("high", col("high").cast("double"))
            .withColumn("low", col("low").cast("double"))
            .withColumn("close", col("close").cast("double"))
            .withColumn("adj_close", col("adj_close").cast("double"))
            .withColumn("volume", col("volume").cast("long"))
        )

    @staticmethod
    def normalize_symbol(df: DataFrame) -> DataFrame:
        
        return df.withColumn(
            "symbol",
            lower(trim(col("symbol")))
        )