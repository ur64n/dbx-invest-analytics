import pytest
from datetime import date
from src.etl.transformations.fred_xml_parser import FredXMLParser

# ---------- helpers ----------

def build_xml(*observations: str) -> bytes:

    obs_block = "\n".join(observations)
    
    return f"""<?xml version="1.0" encoding="utf-8" ?>
<observations>
{obs_block}
</observations>""".encode("utf-8")

# ---------- happy path ----------

def test_parse_single_valid_observation():

    xml = build_xml('<observation date="2024-01-01" value="3.5"/>')

    result = list(FredXMLParser.parse(xml, indicator_id="cpi"))

    assert len(result) == 1
    assert result[0]["indicator_id"] == "cpi"
    assert result[0]["date"] == date(2024, 1, 1)
    assert result[0]["value"] == 3.5


def test_parse_multiple_observations():

    xml = build_xml(
        '<observation date="2024-01-01" value="3.5"/>',
        '<observation date="2024-02-01" value="4.0"/>',
        '<observation date="2024-03-01" value="2.1"/>',
    )

    result = list(FredXMLParser.parse(xml, indicator_id="unrate"))

    assert len(result) == 3
    assert result[0]["date"] == date(2024, 1, 1)
    assert result[2]["value"] == 2.1


# ---------- brak daty — parser pomija rekord ----------

def test_parse_missing_date_skips_record():

    xml = build_xml(
        '<observation value="3.5"/>',
        '<observation date="2024-01-01" value="4.0"/>',
    )

    result = list(FredXMLParser.parse(xml, indicator_id="cpi"))

    assert len(result) == 1
    assert result[0]["value"] == 4.0


def test_parse_empty_date_string_skips_record():

    xml = build_xml('<observation date="" value="3.5"/>')

    result = list(FredXMLParser.parse(xml, indicator_id="cpi"))

    assert len(result) == 0


# ---------- nieprawidłowy format daty ----------

def test_parse_invalid_date_format_skips_record():

    xml = build_xml(
        '<observation date="01-01-2024" value="3.5"/>',
        '<observation date="2024-02-01" value="4.0"/>',
    )

    result = list(FredXMLParser.parse(xml, indicator_id="cpi"))

    assert len(result) == 1
    assert result[0]["date"] == date(2024, 2, 1)


# ---------- value = "." (FRED convention for missing) ----------

def test_parse_dot_value_returns_none():

    xml = build_xml('<observation date="2024-01-01" value="."/>')

    result = list(FredXMLParser.parse(xml, indicator_id="cpi"))

    assert len(result) == 1
    assert result[0]["value"] is None
    assert result[0]["date"] == date(2024, 1, 1)


# ---------- value = "" (pusty string) ----------

def test_parse_empty_value_returns_none():

    xml = build_xml('<observation date="2024-01-01" value=""/>')

    result = list(FredXMLParser.parse(xml, indicator_id="cpi"))

    assert len(result) == 1
    assert result[0]["value"] is None


# ---------- brak atrybutu value ----------

def test_parse_missing_value_attribute_returns_none():

    xml = build_xml('<observation date="2024-01-01"/>')

    result = list(FredXMLParser.parse(xml, indicator_id="cpi"))

    assert len(result) == 1
    assert result[0]["value"] is None


# ---------- value nie-numeryczny ----------

def test_parse_non_numeric_value_returns_none():

    xml = build_xml('<observation date="2024-01-01" value="N/A"/>')

    result = list(FredXMLParser.parse(xml, indicator_id="cpi"))

    assert len(result) == 1
    assert result[0]["value"] is None


# ---------- wartość ujemna (poprawna) ----------

def test_parse_negative_value():

    xml = build_xml('<observation date="2024-01-01" value="-0.5"/>')

    result = list(FredXMLParser.parse(xml, indicator_id="cpi"))

    assert len(result) == 1
    assert result[0]["value"] == -0.5


# ---------- pusty XML (brak observations) ----------

def test_parse_empty_xml_returns_empty():

    xml = b'<?xml version="1.0" encoding="utf-8" ?><observations></observations>'

    result = list(FredXMLParser.parse(xml, indicator_id="cpi"))

    assert len(result) == 0