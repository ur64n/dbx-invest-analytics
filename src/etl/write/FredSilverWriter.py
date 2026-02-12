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

    def insert_metadata_indicators(self, metadata_rows: list[dict]):
        logger.info(f"Inserting metadata indicators")
        
        df = self.spark.createDataFrame(metadata_rows) \
            .withColumn("indicator_id", F.lower(F.col("indicator_id")))
        df.createOrReplaceTempView("metadata_source")

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

