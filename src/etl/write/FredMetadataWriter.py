from src.config.logger import get_logger
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

logger = get_logger("fred_metadata_writer")

class FredMetadataWriter:
    def __init__(self, spark, metadata_table_name: str):
        self.spark = spark
        self.metadata_table_name = metadata_table_name

    def merge(self, cleaned_metadata_df: DataFrame):
        logger.info(f"Inserting metadata indicators")
        
        cleaned_metadata_df.createOrReplaceTempView("metadata_source")

        self.spark.sql(f"""
            MERGE INTO {self.metadata_table_name} AS tgt
            USING metadata_source AS src
            ON tgt.indicator_id = src.indicator_id
            WHEN MATCHED THEN UPDATE SET
                tgt.unit = src.unit,
                tgt.frequency = src.frequency
            WHEN NOT MATCHED THEN INSERT (
                indicator_id,
                unit,
                frequency
            )
            VALUES (
                src.indicator_id,
                src.unit,
                src.frequency
                )
            """)