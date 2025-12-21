from src.config.config import qqq_enriched_delta_file_name, qqq_silver_path
from src.config.logger import get_logger
from src.config.config_loader import load_config
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, trim, regexp_replace


logger = get_logger("cleaning")

class cleaning:
    def __init__(self, spark, df):
        self.spark = spark
        self.df = df
        
    def clean_qqq_entities_col(self, df: DataFrame) -> DataFrame:

        logger.info("Start work with columns standards in qqq entities table")

        df = df.drop("shares")
        df = df.toDF(*[col.strip().lower() for col in df.columns])
        df = df.withColumn(
                "precent_holding",
                regexp_replace(col("precent_holding"), "%", "").cast("decimal(5,2)")
            )
        logger.info("Columns standards in qqq entities table are done")

        return df
    
    def clean_qqq_entities_rows(self, df: DataFrame) -> DataFrame:

        logger.info("Start cleaning rows standards in qqq entities table")

        text_cols = ['symbol', 'name', 'sector', 'industry']

        for c in text_cols:
            df = df.withColumn(
                c,
                lower(trim(regexp_replace(col(c), "\\s+", " ")))
            )
            
        logger.info("Rows standards in qqq entities table are done")
        
        df.write.format("delta").mode("overwrite").saveAsTable(qqq_silver_path)

        logger.info("Cleaned qqq entities table saved successfully in silver layer")

#TODO: Zastanowic sie nad walidacjami i przejsc do szukania kolejnych danych

if __name__ := "__main__":
    
    df = spark.table(qqq_enriched_delta_file_name)
    cleaning_run = cleaning(spark, df)
    run_col = cleaning_run.clean_qqq_entities_col(df)
    run_rows = cleaning_run.clean_qqq_entities_rows(run_col)
    