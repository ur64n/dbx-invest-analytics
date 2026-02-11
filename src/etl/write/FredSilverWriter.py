from src.config.logger import get_logger
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

logger = get_logger("fred_writer")

class FredSilverWriter:
    def __init__(self, spark, table_name: str, metadata_table_name):
        self.spark = spark
        self.table_name = table_name
        self.metadata_table_name = metadata_table_name

    def write_bootstrap(self, cleaned_df):
        logger.info(f"Bootstrap SILVER table")

        cleaned_df.write.format("delta").mode("overwrite").saveAsTable(self.table_name)

    def write_refresh(self, cleaned_df):
        logger.info(f"Merging into SILVER fred_indicators")

        cleaned_df.createOrReplaceTempView("fred_source")

        self.spark.sql(f"""
            MERGE INTO {self.table_name} AS tgt
            USING fred_source AS src
            ON tgt.indicator_id = src.indicator_id
            AND tgt.date = src.date
            WHEN MATCHED THEN UPDATE SET
            tgt.value = src.value
            WHEN NOT MATCHED THEN INSERT (
                indicator_id,
                date,
                value,
                unit,
                frequency
            )
            VALUES (
                src.indicator_id,
                src.date,
                src.value,
                src.unit,
                src.frequency
                )
            """)
        
    def create_indicator_metadata_table(self, cleaned_df: DataFrame):
        logger.info(f"Creating indicator metadata table")
        
        metadata_df = (
            cleaned_df
            .select("indicator_id").distinct()
            .withColumn("unit", F.lit(None).cast("string"))
            .withColumn("frequency", F.lit(None).cast("string"))
            )
        
        metadata_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(self.metadata_table_name)

