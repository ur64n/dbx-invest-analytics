import sys

project_root = "/Workspace/Users/jakub.kaczmarczyk443@gmail.com/dbx-invest-analytics/src"
sys.path.append(project_root)

from src.etl.utils.validation_helper import raw_csv_file_list, normalize_filename
from src.config.config_loader import load_config
from src.config.logger import get_logger

logger = get_logger("validation")

class validation:
    def __init__(self):
        pass

    def validate_raw_csv_files_exist(self):
        logger.info("Start validating raw CSV file exists") 

        raw_csv_files = raw_csv_file_list() # uruchamia zebranie listy nazw plikow
        present_files = {normalize_filename(f) for f in raw_csv_files} # uruchamia normalizacje nazw plikow na zebranej liscie zmienia na zbior (set)
        config = load_config() # urchamia skrypt otierajacy plik yaml
        required_files = set(config["required_files"]) # zbiera z pliku yaml liste wymaganych plikow do zbioru (set)

        missing_files = required_files - present_files # odejmuje wartosci z dwoch list

        if missing_files: # 
            raise Exception(f"Missing required files: {missing_files}") # jezeli jakas nazwa zostanie znaleziona to wyrzuca blad z nazwa pliku

        logger.info("All required files exist")

if __name__ == "__main__":

    run = validation()
    run.validate_raw_csv_files_exist()