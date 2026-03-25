import pytest
from src.etl.cleaning.ohlcv_cleaning import OHLCVCleaner

# ---------- happy path ----------
def test_clean_list_valid_symbols():
    symbol_list = [
        "aapl",
        "msft",
        "csco"
    ]

    result = list(OHLCVCleaner.clean_list(symbol_list))

    assert len(result) == 3

# ---------- filtering ----------
def test_clean_list_filters_ivalid_symbols():

    symbol_list = [
        "AAPL",
        "ABCDEF",
        "abcdef",
        "aapl1",
        "aapl!",
    ]

    result = list(OHLCVCleaner.clean_list(symbol_list))

    assert len(result) == 0

# ---------- edge cases ----------

def test_clean_list_with_valid_invalid_symbols():

    symbol_list = [
        "aapl",
        "AAPL",
        "csco",
        "csco1"
    ]

    result = list(OHLCVCleaner.clean_list(symbol_list))

    assert len(result) == 2

def test_clean_list_filter_non_string_elements():

    symbol_list = [
        None,
        123,
        "aapl"
    ]

    result = list(OHLCVCleaner.clean_list(symbol_list))

    assert len(result) == 1