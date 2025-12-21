from src.config.config import qqq_enriched_delta_file_name, qqq_silver_path
from src.config.logger import get_logger
from src.config.config_loader import load_config
from pyspark.sql import DataFrame


logger = get_logger("cleaning")

class cleaning:
    def __init__(self, spark, df):
        self.spark = spark
        self.df = df
        
    def clean_qqq_entities_col(self, df: DataFrame) -> DataFrame:

        logger.info("Rozpoczynamy pucowanie danych...")

        df = df.drop("shares")
        df = df.toDF(*[col.strip().lower() for col in df.columns])

        return df

if __name__ := "__main__":
    
    df = spark.table(qqq_enriched_delta_file_name)
    cleaning_run = cleaning(spark, df)
    run = cleaning_run.clean_qqq_entities(df)
    run.show(50)
    run.printSchema()
    