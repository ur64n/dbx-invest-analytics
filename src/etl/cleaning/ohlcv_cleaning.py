from src.config.logger import get_logger
import re

logger = get_logger("ohlcv_cleaning")

class OHLCVCleaner:

    @staticmethod
    def clean_list(symbol_list: list) -> list:
        logger.info("Start cleaning list of symbols")

        pattern = re.compile(r"^[a-z]{1,5}$")

        cleaned = [
            s for s in symbol_list
            if isinstance(s, str) and pattern.match(s)
        ]

        logger.info(f"Symbols before cleaning: {len(symbol_list)} and after cleaning: {len(cleaned)}") 

        return cleaned