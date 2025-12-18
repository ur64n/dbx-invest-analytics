import os
import yfinance as yf
from src.config.config import (
    raw_csv_files_path, 
    qqq_entities_filename, 
    raw_qqq_delta_file_path, 
    qqq_delta_file_name
)
from src.config.logger import get_logger
from src.etl.validation.qqq_entities_validation import validation
from src.config.config_loader import load_config
from transformation_helper import TransformationHelper

#TODO: Zrobic commit wyslac na deva, a nastepnie utworzyc walidacje jezeli sa wskazane na tym etapie do nowych ficzerow, na koncu stworzyc modul cleaning.

class transformation:

    def __init__(self, spark, logger, spark_df):
        self.spark = spark
        self.logger = logger
        self.spark_df = spark_df
        self.config = load_config()
        self.validator = validation(self.config, logger)
        self.helper = TransformationHelper(logger, spark)

    def csv_to_delta(self):
        
        self.validator.validate_raw_csv_files_exist()

        self.logger.info("Starting transformation file csv to delta format")

        df = self.spark_df.withColumnRenamed("% Holding", "precent_holding")
        df.write.format("delta").mode("overwrite").saveAsTable(qqq_delta_file_name)

        self.logger.info("Transformation file csv to delta format completed")

        self.validator.validate_delta_table_exist(self.spark, qqq_delta_file_name)

        return df
    
    def qqq_entities_categorisation(self, df):
        
        self.logger.info("Starting qqq_entities categorisation with yfinance")

        tickers = self.helper.extract_tickers(df)
        meta_df = self.helper.fetch_metadata(tickers)
        final_df = self.helper.enrich(df, meta_df)
        self.helper.save_to_delta(final_df)

        self.logger.info("qqq_entities categorisation with yfinance completed")


if __name__ == "__main__":

    logger = get_logger("transformation")
    path = os.path.join(raw_csv_files_path, qqq_entities_filename)
    spark_df = spark.read.csv(path, header=True, inferSchema=True)
    run = transformation(spark, logger, spark_df)
    df = run.csv_to_delta()
    enrich = run.qqq_entities_categorisation(df)
    