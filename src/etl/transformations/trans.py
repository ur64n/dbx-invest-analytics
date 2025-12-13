import pandas as pd
import os
from src.config.config import raw_csv_files_path, qqq_entities_filename, raw_delta_files_path
from src.config.logger import get_logger

logger = get_logger("transformation")

class transformation:
    def __init__(self, spark_df):
        self.logger = get_logger("transformation")
        self.spark_df = spark_df

    def csv_to_delta(self):

        self.logger.info("Starting transformation file csv to delta format")
    
        df = self.spark_df.withColumnRenamed("% Holding", "precent_holding")
        to_delta = df.write.format("delta").mode("overwrite").saveAsTable(raw_delta_files_path)

        self.logger.info(f"Transformation file csv to delta  format completed")

        return df

if __name__ == "__main__":

    path = os.path.join(raw_csv_files_path, qqq_entities_filename)
    spark_df = spark.read.csv(path, header=True, inferSchema=True)
        ### launcher ###
    run = transformation(spark_df)
    df = run.csv_to_delta()
        ### tester ###
    #spark_df.printSchema()
