from src.config.logger import get_logger
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

logger = get_logger("fred_writer")

class FredSilverWriter:
    def __init__(self, spark, table_name: str):
        self.spark = spark
        self.table_name = table_name

    def write_bootstrap(self, df: DataFrame):

        row_count = df.count()
        
        logger.info(f"Bootstrap SILVER table rows = {row_count}")

        df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(self.table_name)

    def write_refresh(self, df: DataFrame):
        """
        Upsert refreshed/revision data into the silver table from last year
        """

        row_count = df.count()

        logger.info(f"Refreshing SILVER table | merge_source_rows = {row_count}")

        df.createOrReplaceTempView("fred_source")

        self.spark.sql(f"""
            MERGE INTO {self.table_name} AS tgt
            USING fred_source AS src
            ON tgt.indicator_id = src.indicator_id
            AND tgt.date = src.date
            WHEN MATCHED THEN UPDATE SET
            tgt.value = src.value,
            tgt.unit = src.unit,
            tgt.frequency = src.frequency
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


