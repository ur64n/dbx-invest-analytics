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
from src.etl.schema.qqq_schema import qqq_schema

logger = get_logger("transformation")

class transformation:

    def __init__(self, spark):
        self.spark = spark
        self.raw_df = raw_df
        self.validator = validation(load_config(), logger, self.spark)

    def csv_to_delta(self):
        
        self.validator.validate_raw_csv_files_exist()
        raw_df = self.validator.validate_raw_csv_schema()
        self.validator.validate_raw_csv_non_empty(raw_df)

        logger.info("Starting transformation file csv to delta format")

        df = raw_df.withColumnRenamed("% Holding", "precent_holding")
        df = df.drop("Shares")
        self.validator.validate_qqq_col_values(df)
        self.validator.validate_id_uniqueness(df)
        df.write.format("delta").mode("overwrite").saveAsTable(qqq_delta_file_name)

        logger.info("Transformation file csv to delta format completed")

        self.validator.validate_delta_table_exist(self.spark, qqq_delta_file_name)

if __name__ == "__main__":

    path = os.path.join(raw_csv_files_path, qqq_entities_filename)
    raw_df = spark.read.schema(qqq_schema).option("header", True).csv(path)
    run = transformation(spark)
    df = run.csv_to_delta()