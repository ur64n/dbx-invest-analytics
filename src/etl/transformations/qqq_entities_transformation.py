import pandas as pd
import os
import yfinance as yf
from src.config.config import raw_csv_files_path, qqq_entities_filename, raw_qqq_delta_file_path, qqq_delta_file_name
from src.config.logger import get_logger
from src.etl.validation.qqq_entities_validation import validation
from src.config.config_loader import load_config

logger = get_logger("transformation")

class transformation:
    def __init__(self, spark, spark_df):
        self.spark = spark
        self.logger = get_logger("transformation")
        self.spark_df = spark_df
        self.config = load_config()
        self.validator = validation(self.config)

    def csv_to_delta(self):
        
        self.validator.validate_raw_csv_files_exist()

        self.logger.info("Starting transformation file csv to delta format")

        df = self.spark_df.withColumnRenamed("% Holding", "precent_holding")
        df.write.format("delta").mode("overwrite").saveAsTable(qqq_delta_file_name)

        self.logger.info("Transformation file csv to delta format completed")

        self.validator.validate_delta_table_exist(self.spark, qqq_delta_file_name)

        return df
    
    def qqq_entities_categorisation(self):
        
        self.logger.info("Starting qqq_entities categorisation with yfinance")

        
   

if __name__ == "__main__":

    path = os.path.join(raw_csv_files_path, qqq_entities_filename)
    spark_df = spark.read.csv(path, header=True, inferSchema=True)
        ### launcher ###
    run = transformation(spark, spark_df)
    df = run.csv_to_delta()



#TODO: Kolejny etap transformacji to enrichment pliku qqq_entities delta o kolumne kategorii zaciagana z yfinance (kod zostal juz zdefiniowany w formie prototypu w sandbox.py, zweryfikowac biblioteki i rozkminic jak ten proces powiniene wygladac w seniorskim wydaniu.