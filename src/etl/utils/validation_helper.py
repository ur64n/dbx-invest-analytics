import os
import re
from src.config.config import raw_csv_files_path, raw_qqq_delta_file_path
from src.config.logger import get_logger

logger = get_logger("validation_helper")

def raw_csv_file_list():
    # Pobiera listę plików z katalogu Volumes
    return [
        f for f in os.listdir(raw_csv_files_path)
        if os.path.isfile(os.path.join(raw_csv_files_path, f))
    ]

def normalize_filename(filename:str) -> str:

    logger.info(f"qqq_etf_entities filename normalization started for: {filename}")

    new_name = re.sub(r"[-_ ]?\d{2}[-\.]\d{2}[-\.]\d{4}", "", filename)

    old_path = os.path.join(raw_csv_files_path, filename)
    new_path = os.path.join(raw_csv_files_path, new_name)

    os.rename(old_path, new_path)
    
    logger.info(f"qqq_etf_entities filename normalization finished, new filename: {new_name}")

    return new_name