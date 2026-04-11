import os
import time
import requests
from xml.etree import ElementTree
from datetime import datetime, timedelta
from typing import Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config.logger import get_logger

logger = get_logger("fred_extraction")

class FredClient:
    """HTTP client for the FRED API (Federal Reserve Economic Data).

    Downloads time series observations as XML files to a local Volume path.
    Also fetches series metadata (unit, frequency) as JSON.
    Respects configurable rate limiting between requests.
    """
    
    def __init__(self, config: dict, api_key: str):
        self.base_url = config["fred"]["base_url"]
        self.api_key = api_key
        self.raw_path = config["paths"]["raw_macro"]
        self.rate_limit_per_sec = config["fred"].get("rate_limit_per_second", 2)

        os.makedirs(self.raw_path, exist_ok=True)

        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

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
    
    def _build_series_metadata_url(self, series_id: str) -> str:
        params = {
            "series_id": series_id,
            "file_type": "json",
            "api_key": self.api_key,
        }
        query = "&".join(f"{key}={value}" for key, value in params.items())
        return f"{self.base_url}/series?{query}"

    # ---------- public API ----------

    def download_series(self, series_id: str, observation_start: Optional[str]) -> str:
        logger.info(f"Downloading FRED series {series_id}")

        url = self._build_observations_url(series_id, observation_start)

        self._rate_limit()

        logger.info(f"Requesting URL: {url}")

        response = self.session.get(url, timeout=30)
        response.raise_for_status()

        xml_path = self._raw_xml_path(series_id)
        with open(xml_path, "wb") as f:
            f.write(response.content) # save xml files to bronze workspace

        logger.info(f"Saved raw XML for {series_id} -> {xml_path}")

        return xml_path

    def download_series_metadata(self, series_id: str) -> dict:
        logger.info(f"Downloading FRED series metadata for {series_id}")

        url = self._build_series_metadata_url(series_id)

        self._rate_limit()
        response = self.session.get(url, timeout=30)
        response.raise_for_status()

        macro_metadata = response.json()

        series_info = macro_metadata.get("seriess", [])
        if not series_info:
            logger.warning(f"No metadata returned for {series_id}")
            return None

        series_info = series_info[0]

        return {
            "indicator_id": series_info.get("id"),
            "unit": series_info.get("units"),
            "frequency": series_info.get("frequency")
        }
