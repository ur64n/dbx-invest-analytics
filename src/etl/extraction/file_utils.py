import os
import re
from typing import List
from src.config.logger import get_logger

logger = get_logger("file_utils")

def list_files(path: str) -> List[str]:
    logger.info("Listing files")

    return [
        f for f in os.listdir(path)
        if os.path.isfile(os.path.join(path, f))
    ]

def normalize_filename(filename: str) -> str:
    logger.info(f"Start normalization filename: {filename}")

    new_name = re.sub(r"[-_ ]?\d{2}[-\.]\d{2}[-\.]\d{4}", "", filename)

    if new_name != filename:
        os.rename(filename, new_name)
        logger.info(f"Renamed file: {filename} -> {new_name}")

    return new_name
