import pytest
from src.etl.cleaning.fred_metadata_cleaning import FredMetadataCleaner

# ---------- happy path ----------
def test_clean_metadata_valid_values_df():

    df = spark.createDataFrame(
        [("CPI", "Percent", "Monthly")],
        ["indicator_id", "unit", "frequency"]
    )

    result = FredMetadataCleaner.clean_metadata(df).collect()

    assert result[0].indicator_id == "cpi"
    assert result[0].unit == "percent"
    assert result[0].frequency == "m"

# ---------- empty strings none ----------
def test_clean_metadata_empty_values_to_none():
    
    df = spark.createDataFrame(
        [("","","")],
        ["indicator_id","unit","frequency"]
    )

    result = FredMetadataCleaner.clean_metadata(df).collect()

    assert result[0].indicator_id is None
    assert result[0].unit is None
    assert result[0].frequency is None

# ---------- cannonical frequency  ----------
def test_clean_metadata_cannonical_form():

    df = spark.createDataFrame(
        [("cpi", "percent", "weekly")],
        ["indicator_id", "unit", "frequency"]
    )

    result = FredMetadataCleaner.clean_metadata(df).collect()

    assert result[0].frequency == "weekly"