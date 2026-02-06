from typing import Iterable, Dict
from xml.etree import ElementTree as ET
from datetime import datetime

from src.config.logger import get_logger

logger = get_logger("fred_xml_parser")

class FredXMLParser:
    """
    Pure parser.
    XML (bytes / str) -> iterable of dicts
    """

    @staticmethod
    def parse(
        xml_content: bytes,
        indicator_id: str,
        unit: str | None = None,
        frequency: str | None = None,
    ) -> Iterable[Dict]:
        """
        Returns rows:
        - indicator_id
        - date
        - value
        - unit
        - frequency
        """

        root = ET.fromstring(xml_content)

        for obs in root.findall(".//observation"):
            date_raw = obs.attrib.get("date")
            value_raw = obs.attrib.get("value")

            # skip structurally invalid rows
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
                "unit": unit,
                "frequency": frequency,
            }
