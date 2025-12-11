import os
import re
from src.config.config import raw_csv_file_name, raw_csv_files_path
from src.config.logger import get_logger

logger = get_logger("validation_helper")

def raw_csv_file_list():
    # Pobiera listę plików z katalogu Volumes
    return [
        f for f in os.listdir(raw_csv_files_path)
        if os.path.isfile(os.path.join(raw_csv_files_path, f))
    ]

def normalize_filename(raw_csv_file_name:str) -> str:

    logger.info(f"qqq etf name normalization started for{raw_csv_file_name}")

    new_name = re.sub(r"[-_ ]?\d{2}[-\.]\d{2}[-\.]\d{4}", "", raw_csv_file_name)

    old_path = os.path.join(raw_csv_files_path, raw_csv_file_name)
    new_path = os.path.join(raw_csv_files_path, new_name)

    os.rename(old_path, new_path)
    
    return new_name

# if __name__ == "__main__":
#     new_name = normalize_filename(raw_csv_file_name)
#     print(new_name)