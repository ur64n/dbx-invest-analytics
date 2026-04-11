from typing import Iterable, Dict
from xml.etree import ElementTree as ET
from datetime import datetime

from src.config.logger import get_logger

logger = get_logger("fred_xml_parser")

class FredXMLParser:
    """Parses FRED XML observation files into dicts.

    Handles FRED conventions: value='.' means missing data,
    empty or non-numeric values become None.
    Yields one dict per observation: {indicator_id, date, value}.
    """
    @staticmethod
    def parse(xml_content: bytes, indicator_id: str) -> Iterable[Dict]:

        root = ET.fromstring(xml_content)

        for obs in root.findall(".//observation"):
            date_raw = obs.attrib.get("date")
            value_raw = obs.attrib.get("value")

            if not date_raw:
                continue

            try:
                date = datetime.strptime(date_raw, "%Y-%m-%d").date()
            except ValueError:
                continue

            value = None
            if value_raw not in (None, ".", ""):
                try:
                    value = float(value_raw)
                except ValueError:
                    value = None

            yield {
                "indicator_id": indicator_id,
                "date": date,
                "value": value,
            }
