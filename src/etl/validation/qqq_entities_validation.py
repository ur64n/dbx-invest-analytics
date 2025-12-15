import sys
project_root = "/Workspace/Users/jakub.kaczmarczyk443@gmail.com/dbx-invest-analytics/src"
sys.path.append(project_root)
from src.etl.utils.validation_helper import raw_csv_file_list, normalize_filename
from src.config.config_loader import load_config
from src.config.logger import get_logger
from pyspark.sql.utils import AnalysisException
from src.config.config import raw_qqq_delta_file_path

logger = get_logger("validation")

class validation:
    def __init__(self, config: dict):
        self.config = config
        self.logger = get_logger("validation")

    def validate_raw_csv_files_exist(self):
        
        self.logger.info("Start validating raw CSV file exists")

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

#TODO: Zdefiniowac nowa funkcje walidacji sprawdzenia obecnosci pliku delta w Unity Catalog.

if __name__ == "__main__":

    config = load_config()
    run = validation(config)
    run.validate_raw_csv_files_exist()
    