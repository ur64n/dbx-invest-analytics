import os
from pyspark.sql import DataFrame, SparkSession
from src.config.logger import get_logger
from src.etl.extraction.file_utils import list_files, normalize_filename

logger = get_logger("qqq_csv_extraction")


class QQQCSVExtractor:

    def __init__(
        self, 
        spark: SparkSession, 
        raw_path: str, 
        expected_filename: str, 
        schema,
        ):

        self.spark = spark
        self.raw_path = raw_path
        self.expected_filename = expected_filename
        self.schema = schema

    def prepare_files(self) -> str:
        """
        Normalize filenames and return full path to expected raw qqq CSV.
        """
        files = list_files(self.raw_path)

        for f in files:
            normalize_filename(os.path.join(self.raw_path, f))

        expected_path = os.path.join(self.raw_path, self.expected_filename)

        if not os.path.exists(expected_path):
            raise FileNotFoundError(
                f"Expected CSV not found: {expected_path}"
            )

        return expected_path

    def read(self) -> DataFrame:
        logger.info("Reading raw QQQ CSV")

        csv_path = self.prepare_files()

        df = (
            self.spark.read
            .schema(self.schema)
            .option("header", True)
            .csv(csv_path)
        )

        logger.info("Raw QQQ CSV loaded")
        return df
