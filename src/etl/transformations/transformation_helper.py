from src.config.logger import get_logger
from src.config.config import qqq_delta_file_name, qqq_enriched_delta_file_name
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import DataFrame
import yfinance as yf

logger = get_logger("transformation_helper")

class TransformationHelper:

    def __init__(self, logger, spark):
        self.logger = logger
        self.spark = spark

    def extract_tickers(self, df) -> list[str]:

        self.logger.info(f"Start building list of tickers from{qqq_delta_file_name}")

        tickers = [r.Symbol for r in df.select("Symbol").distinct().collect()]
    
        self.logger.info(f"Building list of tickers from {qqq_delta_file_name} file, finished")

        return tickers

    def fetch_metadata(self, tickers: list[str]) -> DataFrame:

        self.logger.info(f"Start fetching categories for {len(tickers)} tickers")

        rows = []
        for symbol in tickers:
            try:
                info = yf.Ticker(symbol).info
                rows.append((symbol, info.get("sector"), info.get("industry")))
            except Exception:
                rows.append((symbol, None, None))

        self.logger.info(f"Fetching categories for {len(tickers)} tickers, finished")

        self.logger.info(f"Start creating enriched dataframe by categories and industries")
        
        schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("sector", StringType(), True),
            StructField("industry", StringType(), True),
        ])
        meta_df = self.spark.createDataFrame(rows, schema)
        
        self.logger.info(f"Creating dataframe by categories and industries, finished")

        return meta_df

    def enrich(self, df, meta_df) -> DataFrame:

        self.logger.info(f"Start combining qqq_entities_file with enriched dataframe by categories and industries")

        result_df = df.join(meta_df, on="symbol", how="left")

        self.logger.info(f"Combining qqq_entities_file with enriched dataframe by categories and industries, finished")

        return result_df

    def save_to_delta(self, result_df) -> None:

        self.logger.info(f"Start saving enriched dataframe to delta")

        result_df.write.format("delta").mode("overwrite").saveAsTable(qqq_enriched_delta_file_name)

        self.logger.info(f"Saving enriched dataframe to delta, finished")


