from pyspark.sql import SparkSession
from src.config.logger import get_logger

logger = get_logger("delta_vacuum")

class DeltaVacuum:
    """Runs Delta VACUUM on a table to remove old data files.

    Skips if table doesn't exist. Uses configurable retention period in hours.
    """

    @staticmethod
    def vacuum_table(spark: SparkSession, table_name: str, retention_hours: int) -> None:
        logger.info(f"Starting VACCUM on table: {table_name} with retention {retention_hours} h")

        if not spark.catalog.tableExists(table_name):
            logger.warning(f"Table: {table_name} doesn't exist, VACUUM skipping")
            return
        
        spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")

        logger.info(f"VACUUM completed on {table_name}")