import sys
project_root = "/Workspace/Users/jakub.kaczmarczyk443@gmail.com/dbx-invest-analytics/src"
sys.path.append(project_root)
from src.etl.utils.validation_helper import raw_csv_file_list, normalize_filename
from src.config.config_loader import load_config
from src.config.logger import get_logger
from pyspark.sql.utils import AnalysisException
from src.config.config import raw_qqq_delta_file_path
from pyspark.sql import DataFrame

logger = get_logger("validation")

class validation:
    def __init__(self, config: dict, logger):
        self.config = config

    def validate_raw_csv_files_exist(self):
        
        logger.info("Start validating raw CSV file exists")

        for f in raw_csv_file_list():
            normalize_filename(f)

        normalized_csv_files = set(raw_csv_file_list()) # uruchamia zebranie nazw pliku do zbioru (set)
        required_files = set(self.config["required_files"]) # uruchamia config_loader odczytujacy plik yaml i zbiera z z niego liste wymaganych plikow i przeksztalca zbioru (set)

        missing_files = required_files - normalized_csv_files # odejmuje wartosci z dwoch list

        if missing_files: # Zwraca wartosc bool 
            raise Exception(f"Missing required files: {missing_files}") # jezeli jakas nazwa zostanie znaleziona to wyrzuca blad z nazwa pliku

        logger.info("All required files exist")

    def validate_delta_table_exist(self, spark, table_name: str):
        try:
            spark.table(table_name)
        except AnalysisException:
            raise Exception(f"Delta table {table_name} does not exist")

    def validate_enrichment_cols(self, df: DataFrame, meta_df: DataFrame) -> None:
        
        logger.info(f"Start validating required columns exist in base and meta df")

        required_base_col = {"Symbol"}
        missing_base_col = required_base_col - set(df.columns)
        if missing_base_col:
            raise ValueError(f"Missing required column {missing_base_col}")

        required_meta_cols = {"symbol", "sector", "industry"}
        missing_meta_cols = required_meta_cols - set(meta_df.columns)
        if missing_meta_cols:
            raise ValueError(f"Missing required column {missing_meta_cols}")

        logger.info("All required columns in base and meta df exists")

    def validate_metadata_consistency(self, tickers: list[str], meta_df: DataFrame) -> None:
        
        logger.warning(f"Start validating metadata consistency")

        total_tickers = len(tickers)

        not_null_sector = meta_df.filter(meta_df.sector.isNotNull()).count()
        not_null_industry = meta_df.filter(meta_df.industry.isNotNull()).count()

        difference_sector = total_tickers - not_null_sector
        difference_industry = total_tickers - not_null_industry
        
        logger.warning(f"Missing sector values: {difference_sector}, missing industry values: {difference_industry}")

        missing_sector_tickers = [row["symbol"] for row in meta_df.filter(meta_df.sector.isNull()).select("symbol").collect()]


        logger.warning(f"Missing sector values for tickers: {missing_sector_tickers}")

    def validate_null_values(self, df: DataFrame) -> None:

        logger.info(f"Start validating null values in decimal column")

        null_count = df.filter(df["precent_holding"].isNull()).count()
        if null_count > 0:
            logger.warning(f"Found {null_count} null values in decimal column")

        logger.info("No null values found in decimal column")

        
        

if __name__ == "__main__":

    config = load_config()
    run = validation(config)
    run.validate_raw_csv_files_exist()
    