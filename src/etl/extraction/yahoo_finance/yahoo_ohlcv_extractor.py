import yfinance as yf
import pandas as pd
from src.config.logger import get_logger

logger = get_logger("yahoo_ohlcv_extraction")

def download_ohlcv(spark, symbols: list, start_date: str, end_date: str):

    logger.info("Start downloading OHLCV data")

    all_data = []

    expected_cols = [
        "date", "open", "high", "low",
        "close", "adj_close", "volume"
    ]

    logger.info(
        f"Downloading OHLCV data for {len(symbols)} symbols "
        f"from {start_date} to {end_date}"
    )

    for symbol in symbols:

        df = yf.download(
            symbol,
            start=start_date,
            end=end_date,
            interval="1d",
            auto_adjust=False,
            progress=False
        )

        empty_count = 0

        if df.empty:
            logger.warning(f"Empty dataframe for {symbol}")
            empty_count += 1
            continue

        # reset index (Date -> column)
        df = df.reset_index()

        # flatten MultiIndex if exists
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] for col in df.columns]

        # normalize column names
        df.columns = [
            col.strip().lower().replace(" ", "_")
            for col in df.columns
        ]

        # schema contract validation (compare sets)
        if not set(expected_cols).issubset(df.columns):
            raise ValueError(
                f"Expected columns {expected_cols} not found in dataframe"
            )

        # select only expected columns
        df = df[expected_cols]

        # add symbol column
        df["symbol"] = symbol

        # column order
        df = df[
            [
                "date", "symbol",
                "open", "high", "low",
                "close", "adj_close", "volume"
            ]
        ]

        all_data.append(df)

    if not all_data:
        raise ValueError("No data downloaded for any symbol")

    final_df = pd.concat(all_data, ignore_index=True)

    logger.info(
        f"Download rows: {len(final_df)} | empty symbols: {empty_count}"
    )

    return final_df