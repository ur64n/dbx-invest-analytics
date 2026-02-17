import pytest
from datetime import date
from src.etl.enrichment.fred_indicator_enrichment import FredIndicatorEnricher

def test_enrich(spark):

    # --- fake fact data ---
    indicators_data = [
        ("cpi", date(2024, 1, 1), 3.0),
        ("unrate", date(2024, 1, 1), 5.0),
    ]

    indicator_df = spark.createDataFrame(
        indicators_data,
        ["indicator_id", "date", "value"]
    )

    # --- fake metadata data ---
    metadata_data = [
        ("cpi", "percent", "m")
    ]

    metadata_df = spark.createDataFrame(
        metadata_data,
        ["indicator_id", "unit", "frequency"]
    )

    # --- run enrichment ---
    result_df = FredIndicatorEnricher.enrich(indicator_df, metadata_df)

    # --- assert schema contract ---
    assert result_df.columns == [
        "indicator_id",
        "date",
        "value",
        "unit",
        "frequency",
    ]

    result = {row.indicator_id: row for row in result_df.collect()}

    # --- matched case ---
    assert result["cpi"].unit == "percent"
    assert result["cpi"].frequency == "m"

    # --- unmatched case (left join) ---
    assert result["unrate"].unit is None
    assert result["unrate"].frequency is None
