import pytest
from src.etl.transformations.ohlcv_return_calculation import OHLCVCalculator

def test_calculate_daily_return_expected_values():

    df = spark.createDataFrame(
        [
            ("aapl", "2024-01-01", 100.0),
            ("aapl", "2024-01-02", 110.0),
            ("aapl", "2024-01-03", 105.0),
        ],
        ["symbol","date","close"]
    )

    result = OHLCVCalculator.calculate_daily_return(df).collect()

    assert len(result) == 2
    assert result[0].daily_return == pytest.approx(0.1)
    assert result[1].daily_return == pytest.approx(-0.04545, abs=1e-4)

def test_calculate_daily_return_various_symbols_window():

    df = spark.createDataFrame(
        [
            ("aapl", "2024-01-01", 100.0),
            ("aapl", "2024-01-02", 110.0),
            ("msft", "2024-01-01", 200.0),
            ("msft", "2024-01-02", 220.0),
        ],
        ["symbol", "date", "close"]
    )

    result = OHLCVCalculator.calculate_daily_return(df).collect()
    result = {row.symbol: row.daily_return for row in result}

    assert result["aapl"] == pytest.approx(0.1)
    assert result["msft"] == pytest.approx(0.1)

def test_calculate_daily_forward_expected_values():

    df = spark.createDataFrame(
        [
            ("aapl", "2024-01-01", 100.0),
            ("aapl", "2024-01-02", 110.0),
            ("aapl", "2024-01-03", 110.0),
            ("aapl", "2024-01-04", 120.0),
            ("aapl", "2024-01-05", 110.0),
            ("aapl", "2024-01-06", 110.0),
        ],
        ["symbol", "date", "close"]
    )

    result = OHLCVCalculator.calculate_forward_returns(df).collect()

    assert len(result) == 1
    assert result[0].return_t1 == pytest.approx(0.1)
    assert result[0].return_t2 == pytest.approx(0.1)
    assert result[0].return_t5 == pytest.approx(0.1)
    assert "close_t1" not in result[0].__fields__
    assert "close_t2" not in result[0].__fields__
    assert "close_t5" not in result[0].__fields__