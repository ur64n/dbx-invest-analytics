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
        self.validator = validation(load_config(), self.logger, self.spark)

    def extract_tickers(self) -> list[str]:

        logger.info("Start building list of tickers/symbols from qqq delta tabl")

        tickers = [r.Symbol for r in self.df.select("Symbol").distinct().collect()]
    
        logger.info("Building list of tickers/symbols from qqq delta table finished")

        return tickers

    def fetch_categories(self, tickers: list[str]) -> DataFrame:

        if not tickers:
            raise ValueError("Empty tickers list")

        logger.info(f"Start fetching categories from yfinance for {len(tickers)} tickers/symbols")

        rows = []
        for symbol in tickers:
            try:
                info = yf.Ticker(symbol).info
                rows.append((symbol, info.get("sector"), info.get("industry")))
            except Exception:
                rows.append((symbol, None, None))

        logger.info(f"Fetching categories for {len(tickers)} tickers/symbols from yfinance, finished")

        logger.info(f"Start combining base delta table with category dataframe")
        
        schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("sector", StringType(), True),
            StructField("industry", StringType(), True),
        ])
        category_df = self.spark.createDataFrame(rows, schema)

        self.validator.validate_missing_categories(tickers, category_df)
        
        logger.info(f"Combining base delta file with category dataframe finished")

        return category_df

    def enrich(self, category_df) -> DataFrame:

        self.validator.validate_qqq_cols_exist(self.df)

        self.validator.validate_enrichment_cols(self.df, category_df)

        logger.info(f"Start combining base delta table with category dataframe")

        result_df = self.df.join(category_df, on="symbol", how="left")

        logger.info(f"Combining base delta table with category dataframe finished")

        return result_df

    def save_to_delta(self, result_df) -> None:

        logger.info(f"Start saving enriched dataframe to delta silver layer")

        result_df.write.format("delta").mode("overwrite").saveAsTable(qqq_enriched_delta_file_name)

        logger.info(f"Saving enriched dataframe to delta silver layer finished")

if __name__ == "__main__":

    df = spark.table(qqq_delta_file_name)
    enr = enrichment(logger, spark, df)
    tickers = enr.extract_tickers()
    category_df = enr.fetch_categories(tickers)
    result_df = enr.enrich(category_df)
    enr.save_to_delta(result_df)
