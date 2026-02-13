import pytest
from src.etl.enrichment.fred_indicator_enrichment import FredIndicatorEnricher

def test_enrich(spark):

    # Fake values for indicator_df
    indicators_data = [
        ("cpi", "2024-01-01", 3.0),
        ("unrate", "2024-01-01", 5.0),
    ]
    
    # Creates df based on the above fake values
    indicator_df = spark.createDataFrame(
        indicators_data,
        ["indicator_id", "date", "value"]
    )

    # Fake metadata values for indicator_df
    metadata_data = [
        ("cpi", "percent", "monthly")
    ]
    
    # Creates df based on the above fake values
    metadata_df = spark.createDataFrame(
        metadata_data,
        ["indicator_id", "unit", "frequency"]
    )

    # run real enrich function on fake created dataframes above

    result_df = FredIndicatorEnricher.enrich(indicator_df, metadata_df)

    result = {row.indicator_id: row.unit for row in result_df.collect()}

    assert result["cpi"] == "percent"
    assert result["unrate"] is None

