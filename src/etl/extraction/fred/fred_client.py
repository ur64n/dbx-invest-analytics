import os
import time
import requests
from xml.etree import ElementTree
from datetime import datetime, timedelta
from typing import Optional

from src.config.logger import get_logger

logger = get_logger("fred_extraction")

class FredClient:
    """
    Extraction-only client for FRED API.
    Responsible ONLY for:
    - calling API
    - computing refresh window
    - writing raw XML
    """

    def __init__(self, config: dict, api_key: str):
        self.base_url = config["fred"]["base_url"]
        self.api_key = api_key
        self.raw_path = config["paths"]["raw_macro"]
        self.rate_limit_per_sec = config["fred"].get("rate_limit_per_second", 2)

        os.makedirs(self.raw_path, exist_ok=True)

    # ---------- helpers ----------

    def _rate_limit(self):
        time.sleep(1.0 / self.rate_limit_per_sec)

    def _build_observations_url(self, series_id: str, observation_start: Optional[str]) -> str:
        params = {
            "series_id": series_id,
            "file_type": "xml",
            "api_key": self.api_key,
        }
        if observation_start:
            params["observation_start"] = observation_start

        query = "&".join(f"{key}={value}" for key, value in params.items())
        return f"{self.base_url}/series/observations?{query}"

    def _raw_xml_path(self, series_id: str) -> str:
        return os.path.join(self.raw_path, f"{series_id}.xml")
    
    # ---------- public API ----------

    def download_series(self, series_id: str, observation_start: Optional[str]) -> str:
        logger.info(f"Downloading FRED series {series_id}")

        url = self._build_observations_url(series_id, observation_start)

        self._rate_limit()
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        xml_path = self._raw_xml_path(series_id)
        with open(xml_path, "wb") as f:
            f.write(response.content)

        logger.info(f"Saved raw XML for {series_id} -> {xml_path}")
        return xml_path

