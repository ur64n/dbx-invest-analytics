import pytest
from src.etl.cleaning.fred_cleaning import FredCleaner
from pyspark.sql.types import StructType, StructField, StringType
from datetime import date

def test_standardize_columns_types():

    df = spark.createDataFrame(
        [("CPI", "2024-01-01", "3.5")],
        ["indicator_id", "date", "value"]
    )

    result = FredCleaner.standardize_columns(df).collect()

    assert result[0].indicator_id == "cpi"
    assert result[0].date == date(2024, 1, 1)
    assert result[0].value == 3.5
    assert isinstance(result[0].date, date)
    assert isinstance(result[0].value, float)

def test_standardize_columns_lower_trim():

    schema = StructType([
        StructField("indicator_id", StringType()),
        StructField("date", StringType()),
        StructField("value", StringType())
    ])

    df = spark.createDataFrame(
        [(" CPI ", "2024-01-01", None)],
        schema=schema
    )

    result = FredCleaner.standardize_columns(df).collect()

    assert result[0].indicator_id == "cpi"
    assert result[0].date == date(2024, 1, 1)
    assert result[0].value is None