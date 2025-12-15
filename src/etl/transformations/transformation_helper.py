import yfinance as yf
from src.config.logger import get_logger
from src.config.config import qqq_delta_file_name
# import sparka ?
# uruchomienie maszyny spark ? getorcreate cos tam ?

logger = get_logger("transformation_helper")


def qqq_tickers_list(df: pd.DataFrame) -> str:

    logger.info(f"Start preparing list with tickers based on {qqq_delta_file_name}") # Chce w {miec nazwe pliku na ktorym bazuje ten kod}

    tickers = (
        df.select("Symbol")
        .distinct()
        .rdd
        .map(lambda r: r[0])
        .collect()
    )

    return tickers

def qqq_entities_cat_enrichement(df: pd.DataFrame) -> pd.DataFrame:

    logger.info(f"Start enriching entities in category column in {qqq_delta_file_name}")

    rows = []

    for symbol in tickers:
        try:
            info = yf.Ticker(symbol).info
            rows.append((
                symbol,
                info.get("sector"),
                info.get("industry")
            ))
        except Exception:
            rows.append((symbol, None, None))





















