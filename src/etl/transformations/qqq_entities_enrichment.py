from src.config.logger import get_logger
from src.config.config import qqq_delta_file_name, qqq_enriched_delta_file_name
from src.config.config_loader import load_config
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import DataFrame
import yfinance as yf
from src.etl.validation.qqq_entities_validation import validation

logger = get_logger("enrichment")

class enrichment:

    def __init__(self, logger, spark, df):
        self.logger = logger
        self.spark = spark
        self.df = df
        self.validator = validation(load_config(), self.logger)

    def extract_tickers(self) -> list[str]:

        logger.info(f"Start building list of tickers from {qqq_delta_file_name}")

        tickers = [r.Symbol for r in self.df.select("Symbol").distinct().collect()]
    
        logger.info(f"Building list of tickers from {qqq_delta_file_name} file, finished")

        return tickers

    def fetch_metadata(self, tickers: list[str]) -> DataFrame:

        if not tickers:
            raise ValueError("Empty tickers list")

        logger.info(f"Start fetching categories from yfinance for {len(tickers)} tickers")

        rows = []
        for symbol in tickers:
            try:
                info = yf.Ticker(symbol).info
                rows.append((symbol, info.get("sector"), info.get("industry")))
            except Exception:
                rows.append((symbol, None, None))

        logger.info(f"Fetching categories for {len(tickers)} tickers, finished")

        logger.info(f"Start creating enriched dataframe with categories and industries")
        
        schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("sector", StringType(), True),
            StructField("industry", StringType(), True),
        ])
        meta_df = self.spark.createDataFrame(rows, schema)

        self.validator.validate_metadata_consistency(tickers, meta_df)
        
        logger.info(f"Creating dataframe with categories and industries, finished")

        return meta_df

    def enrich(self, meta_df) -> DataFrame:

        self.validator.validate_enrichment_cols(self.df, meta_df)

        logger.info(f"Start combining qqq_entities_file with enriched dataframe by categories and industries")

        result_df = self.df.join(meta_df, on="symbol", how="left")

        logger.info(f"Combining qqq_entities_file with enriched dataframe by categories and industries, finished")

        return result_df

    def save_to_delta(self, result_df) -> None:

        logger.info(f"Start saving enriched dataframe to delta")

        result_df.write.format("delta").mode("overwrite").saveAsTable(qqq_enriched_delta_file_name)

        logger.info(f"Saving enriched dataframe to delta, finished")

if __name__ == "__main__":

    df = spark.table(qqq_delta_file_name)
    enr = enrichment(logger, spark, df)
    tickers = enr.extract_tickers()
    meta_df = enr.fetch_metadata(tickers)
    result_df = enr.enrich(meta_df)
    enr.save_to_delta(result_df)
