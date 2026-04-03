import yfinance as yf
from pyspark.sql import SparkSession, DataFrame
from src.config.logger import get_logger

logger = get_logger("qqq_categories_extraction")

class QQQCategoriesExtractor:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def extract(self, symbols: list[str], schema: StructType) -> DataFrame:
        if not symbols:
            raise ValueError("Empty symbols list for category extraction")

        logger.info(f"Fetching categories from yfinance for {len(symbols)} symbols")

        rows = []
        for symbol in symbols:
            try:
                info = yf.Ticker(symbol).info
                rows.append(
                    (
                        symbol.lower(),
                        info.get("sector"),
                        info.get("industry"),
                    )
                )
            except Exception:
                rows.append((symbol.lower(), None, None))

        category_df = self.spark.createDataFrame(rows, schema)

        logger.info("Category extraction finished")

        return category_df