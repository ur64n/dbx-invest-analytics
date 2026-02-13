import pytest
from src.etl.extraction.fred.fred_client import FredClient

def test_build_series_metadata_url():
    config = {
        "fred": {"base_url": "https://api.stlouisfed.org/fred"},
        "paths": {"raw_macro": "/tmp"},
    }

    client = FredClient(config=config, api_key="TESTKEY")

    url = client._build_series_metadata_url("CPIAUCSL")

    assert "series_id=CPIAUCSL" in url
    assert "file_type=json" in url
    assert "api_key=TESTKEY" in url