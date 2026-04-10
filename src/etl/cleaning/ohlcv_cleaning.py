from src.config.logger import get_logger
import re

logger = get_logger("ohlcv_cleaning")

class OHLCVCleaner:
    """Filters symbol lists to valid QQQ ticker format.

    Accepts only lowercase alpha strings, 1-5 chars (e.g. 'aapl', 'msft').
    Rejects uppercase, numeric, special chars, and non-string elements.
    Note: benchmark symbols like '^VIX' are intentionally excluded here
    and added separately in the pipeline.
    """
    
    @staticmethod
    def clean_list(symbol_list: list) -> list:
        logger.info("Start cleaning list of qqq_symbols")

        pattern = re.compile(r"^[a-z]{1,5}$")

        cleaned = [
            s for s in symbol_list
            if isinstance(s, str) and pattern.match(s)
        ]

        logger.info(f"QQQ Symbols before cleaning: {len(symbol_list)} and after cleaning: {len(cleaned)}") 

        return cleaned