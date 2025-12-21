import os
from src.config.config import (
    raw_csv_files_path, 
    qqq_entities_filename, 
    raw_qqq_delta_file_path, 
    qqq_delta_file_name
)
from src.config.logger import get_logger
from src.etl.validation.qqq_entities_validation import validation
from src.config.config_loader import load_config

#TODO: Usunac z pliku enriched wszystkie spolki, ktore nie sa technologiczne, utworzyc modul czyszczenia danych.

logger = get_logger("transformation")

class transformation:

    def __init__(self, spark, spark_df):
        self.spark = spark
        self.spark_df = spark_df
        self.validator = validation(load_config(), logger)

    def csv_to_delta(self):
        
        self.validator.validate_raw_csv_files_exist()

        logger.info("Starting transformation file csv to delta format")

        df = self.spark_df.withColumnRenamed("% Holding", "precent_holding")
        df.write.format("delta").mode("overwrite").saveAsTable(qqq_delta_file_name)

        logger.info("Transformation file csv to delta format completed")

        self.validator.validate_delta_table_exist(self.spark, qqq_delta_file_name)

if __name__ == "__main__":

    path = os.path.join(raw_csv_files_path, qqq_entities_filename)
    spark_df = spark.read.csv(path, header=True, inferSchema=True)
    run = transformation(spark, spark_df)
    df = run.csv_to_delta()
    