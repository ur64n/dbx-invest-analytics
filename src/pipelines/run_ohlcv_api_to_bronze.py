from pyspark.sql import SparkSession
from pyspark.sql.functions import max as spark_max

from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.extraction.yahoo_finance.yahoo_ohlcv_extractor import download_ohlcv
from src.etl.cleaning.ohlcv_cleaning import OHLCVCleaner
from src.etl.write.ohlcvWriter import OHLCVWriter

from datetime import datetime, timedelta, UTC

logger = get_logger("ohlcv_bronze_pipeline")

def run():
    logger.info("Starting OHLCV indicators pipeline")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    qqq_silver_entities = config["tables"]["silver_qqq"]
    ohlcv_bronze_table = config["tables"]["bronze_ohlcv"]
    ohlcv_table = config["tables"]["silver_ohlcv"]

    base_start_date = config["yfinance"]["start_date"]
    window_refresh_months = config["yfinance"]["window_refresh_months"]

    # ---------- load symbols ----------
    entities_df = spark.table(qqq_silver_entities)

    symbols = [
        row["symbol"]
        for row in entities_df.select("symbol").distinct().collect()
    ]

    logger.info(f"Number of symbols before cleaning: {len(symbols)}")

    # ---------- clean ----------
    symbols = OHLCVCleaner.clean_list(symbols)

    logger.info(f"Number of symbols after cleaning: {len(symbols)}")

    # ---------- date range ----------
    end_date = datetime.now(UTC).strftime("%Y-%m-%d")

    exists = spark.catalog.tableExists(ohlcv_table)

    has_data = (
        exists
        and spark.table(ohlcv_table).limit(1).count() > 0
    )

    if not has_data:
        logger.info("No data found in table -> FULL LOAD")
        start_date = base_start_date

    else:
        logger.info("Data found in table -> INCREMENTAL LOAD")

        max_date = (
            spark.table(ohlcv_table)
            .select(spark_max("date").alias("max_date"))
            .collect()[0]["max_date"]
        )

        if max_date is None:
            logger.info("Table exists but empty -> FULL LOAD")
            start_date = base_start_date
        else:
            refresh_window_days = window_refresh_months * 30

            start_date = (
                max_date - timedelta(days=refresh_window_days)
            ).strftime("%Y-%m-%d")

            logger.info(f"Max date in table: {max_date}")
            logger.info(f"Refresh start date: {start_date}")

    logger.info(f"Date range: {start_date} - {end_date}")

    # ---------- extraction ----------

    pdf = download_ohlcv(
        spark=spark,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date
    )

    logger.info(f"Extracted rows: {len(pdf)}")

    # ---------- convert ----------
    df = spark.createDataFrame(pdf)

    # ---------- write ----------
    ohlcv_writer = OHLCVWriter(
        spark=spark, 
        path=ohlcv_bronze_table
    )
    
    logger.info(f"Writing to table: {ohlcv_bronze_table}")

    ohlcv_writer.write_bronze_ohlcv(df)

    logger.info("OHLCV pipeline successfully")

if __name__ == "__main__":

    run()
