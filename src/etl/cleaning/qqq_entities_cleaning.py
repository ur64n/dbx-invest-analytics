from src.config.config import delta_qqq_enriched_filename, qqq_silver_filepath
from src.config.logger import get_logger
from src.config.config_loader import load_config
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, trim, regexp_replace
from src.etl.validation.qqq_entities_validation import validation

logger = get_logger("cleaning")

class cleaning:
    def __init__(self, spark, df, logger):
        self.spark = spark
        self.df = df
        self.logger = logger
        self.validator = validation(load_config(), self.logger, self.spark)

    def clean_qqq_entities_col(self, df: DataFrame) -> DataFrame:

        logger.info("Start columns standarization in qqq enriched table")

        df = df.toDF(*[col.strip().lower() for col in df.columns])
        df = df.withColumn(
                "precent_holding",
                regexp_replace(col("precent_holding"), "%", "").cast("decimal(5,2)")
            )
        logger.info("Columns standarization in qqq enriched table are done")

        return df
    
    def clean_qqq_entities_rows(self, df: DataFrame) -> DataFrame:

        logger.info("Start rows standardization in qqq enriched table")

        text_cols = ['symbol', 'name', 'sector', 'industry']

        for c in text_cols:
            df = df.withColumn(
                c,
                lower(trim(regexp_replace(col(c), "\\s+", " ")))
            )
            
        logger.info("Rows standardization in qqq enriched table are done")

        self.validator.validate_null_values(df)
        
        df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(qqq_silver_filepath)

        logger.info("Cleaned qqq enriched table, saved successfully in silver layer")

if __name__ := "__main__":
    
    df = spark.table(delta_qqq_enriched_filename)
    cleaning_run = cleaning(spark, df, logger)
    run_col = cleaning_run.clean_qqq_entities_col(df)
    run_rows = cleaning_run.clean_qqq_entities_rows(run_col)
    