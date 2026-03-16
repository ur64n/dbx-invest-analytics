from src.config.logger import get_logger
from pyspark.sql import DataFrame
from delta.tables import DeltaTable

logger = get_logger("delta_table_writer")

class DeltaTableWriter:
    def __init__(self, table_name:str, spark):
        self.table_name = table_name
        self.spark = spark

    def overwrite(self, df:DataFrame) -> None:
        logger.info(f"Writing {df.count()} rows to {self.table_name}")

        df.write.format("delta").mode("overwrite").saveAsTable(self.table_name)

    def overwrite_schema(self, df:DataFrame) -> None:
        logger.info(f"Writing {df.count()} rows to {self.table_name} overwriteSchema = True")

        df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(self.table_name)

    def append(self, df:DataFrame) -> None:
        logger.info(f"Writing {df.count()} rows to {self.table_name} mode = append")

        df.write.format("delta").mode("append").saveAsTable(self.table_name)

    def upsert(self, df: DataFrame, merge_keys: list[str]) -> None:
        logger.info(f"Upserting {df.count()} rows into {self.table_name}")

        if not self.spark.catalog.tableExists(self.table_name):
            df.write.format("delta").mode("overwrite").saveAsTable(self.table_name)
            
            return

        delta_table = DeltaTable.forName(self.spark, self.table_name)

        merge_condition = " AND ".join(
            [f"target.{key} = source.{key}" for key in merge_keys]
        )

        delta_table.alias("target").merge(
            df.alias("source"),
            merge_condition
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()



