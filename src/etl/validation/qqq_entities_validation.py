import sys
project_root = "/Workspace/Users/jakub.kaczmarczyk443@gmail.com/dbx-invest-analytics/src"
sys.path.append(project_root)
from src.etl.utils.validation_helper import raw_csv_file_list, normalize_filename
from src.config.config_loader import load_config
from src.config.logger import get_logger
from pyspark.sql.utils import AnalysisException
from src.config.config import raw_qqq_delta_file_path, raw_csv_files_path, qqq_entities_filename
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
import os

#TODO: Na podstawie metod walidacji, wykonac sobie notatke (notion) jakie walidacje wykonuje sie zwykle na tabelach. (Opisac proces slownie zalaczyc kod oraz funkcje, ktorych sie uzywa.)

logger = get_logger("validation")

class validation:
    def __init__(self, config: dict, logger, spark):
        self.config = config
        self.spark = spark

    def validate_raw_csv_files_exist(self):
        
        logger.info("Start validating raw CSV file exists")

        for f in raw_csv_file_list():
            normalize_filename(f)

        normalized_csv_files = set(raw_csv_file_list())
        required_files = set(self.config["required_files"])

        missing_files = required_files - normalized_csv_files

        if missing_files:
            raise Exception(f"Missing required files: {missing_files}") 

        logger.info("All required files exist")

    def validate_raw_csv_schema(self) -> None:

        logger.info(f"Start validating required columns exist in raw qqq entities csv file")

        csv_path = os.path.join(raw_csv_files_path, qqq_entities_filename)
        df = self.spark.read.csv(csv_path, header=True, inferSchema=True)
        required_cols = {"Symbol", "Name", "% Holding"}

        missing_cols = required_cols - set(df.columns)

        if missing_cols:
            raise ValueError(f"Missing required column {missing_cols}")

        logger.info("All required columns exist in raw qqq entities csv file")

        return df

    def validate_raw_csv_non_empty(self, df: DataFrame) -> None:

        logger.info(f"Start validating raw csv file is not empty")

        row_count = df.count()
        if row_count == 0:
            raise ValueError(f"Raw csv file is empty")

        logger.info(f"Raw csv file contain {row_count} rows")

    def validate_delta_table_exist(self, spark, table_name: str):
        try:
            spark.table(table_name)
        except AnalysisException:
            raise Exception(f"Delta table {table_name} does not exist")

    def validate_qqq_cols_exist(self, df: DataFrame) -> None:
        
        logger.info(f"Start validating required columns exist in base qqq delta table")

        required_cols = {"Symbol", "Name", "precent_holding"}

        missing_cols = required_cols - set(df.columns)

        if missing_cols:
            raise ValueError(f"Missing required column {missing_cols}")

        logger.info("All required columns exist in base qqq delta table")

    def validate_qqq_col_values(self, df: DataFrame) -> None:
        
        logger.info("Start validating row values in bronze qqq delta table")

        cols = ["Symbol", "Name", "precent_holding"]

        for c in cols:
            count_empties = df.filter(col(c).isNull() | (col(c) == lit(""))).count() # lit tworzy fikcyjna stala pusta wartosc i filtruje puste wartosci na podstawie porownania w kolumnach.
            
            if count_empties > 0:

                if c.lower() == "symbol":
                    raise ValueError(f"Found empty values in qqq delta table column: {c}")

                logger.warning(f"Found: {count_empties} empty values in qqq delta table column: {c}")

        logger.info("All required columns have no empty values in qqq delta table")

    def validate_enrichment_cols(self, df: DataFrame, category_df: DataFrame) -> None:
        
        logger.info(f"Start validating required columns exist in qqq delta file and meta df")

        required_base_col = {"Symbol"}
        missing_base_col = required_base_col - set(df.columns)
        if missing_base_col:
            raise ValueError(f"Missing required column {missing_base_col}")

        required_meta_cols = {"symbol", "sector", "industry"}
        missing_meta_cols = required_meta_cols - set(category_df.columns)
        if missing_meta_cols:
            raise ValueError(f"Missing required column {missing_meta_cols}")

        logger.info("All required columns in base delta table and category df exists")

    def validate_missing_categories(self, tickers: list[str], category_df: DataFrame) -> None:
        
        logger.warning(f"Start validating missing categories in combined dataframe")

        total_tickers = len(tickers)

        not_null_sector = category_df.filter(category_df.sector.isNotNull()).count()
        not_null_industry = category_df.filter(category_df.industry.isNotNull()).count()

        difference_sector = total_tickers - not_null_sector
        difference_industry = total_tickers - not_null_industry
        
        logger.warning(f"Missing sector/category values: {difference_sector}, missing industry values: {difference_industry}")

        missing_sector_tickers = [row["symbol"] for row in category_df.filter(category_df.sector.isNull()).select("symbol").collect()]

        logger.warning(f"Missing sector/category values for tickers: {missing_sector_tickers}")

    def validate_null_values(self, df: DataFrame) -> None:

        logger.info(f"Start validating null values in enriched table decimal column")

        null_count = df.filter(df["precent_holding"].isNull()).count()
        if null_count > 0:
            logger.warning(f"Found {null_count} null values in decimal column")

        logger.info("No null values found in enriched table decimal column")

    def validate_id_uniqueness(self, df: DataFrame) -> None:

        logger.info("Start validating id symbol column uniqueness")

        count_all = df.count()
        count_distinct = df.select("Symbol").distinct().count()

        if count_all != count_distinct:
            raise ValueError(f"Found {count_all - count_distinct} duplicate values in id column")

        logger.info("All id values are unique")


if __name__ == "__main__":

    config = load_config()
    run = validation(config)
    run.validate_raw_csv_files_exist()
    