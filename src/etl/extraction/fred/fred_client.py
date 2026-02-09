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

    def _build_observations_url(self, series_id: str, start_date: Optional[str]) -> str:
        params = {
            "series_id": series_id,
            "file_type": "xml",
            "api_key": self.api_key,
        }
        if start_date:
            params["observation_start"] = start_date

        query = "&".join(f"{key}={value}" for key, value in params.items())
        return f"{self.base_url}/series/observations?{query}"

    def _compute_start_date(self, refresh_window_months: int) -> str:
        window_start = datetime.utcnow() - timedelta(days=30 * refresh_window_months)
        return window_start.strftime("%Y-%m-%d")

    def _raw_xml_path(self, series_id: str) -> str:
        return os.path.join(self.raw_path, f"{series_id}.xml")
    
    def _last_loaded_date(self, series_id: str) -> str | None:
        path = self._raw_xml_path(series_id)

        if not os.path.exists(path):
            return None

        try:
            tree = ElementTree.parse(path) #parsowanie do drzewa
            root = tree.getroot() #pobranie korzenia

            dates = []
            for obs in root.findall(".//observation"):
                date_str = obs.attrib.get("date")
                if date_str:
                    dates.append(datetime.strptime(date_str, "%Y-%m-%d")) # dodaje daty do listy

            if not dates:
                return None

            return max(dates).strftime("%Y-%m-%d") # zwraca najnowsza date

        except Exception:
            return None

    # ---------- public API ----------

    def download_series(self, series_id: str, refresh_window_months: int) -> str:
        logger.info(f"Downloading FRED series {series_id}")

        last_date = self._last_loaded_date(series_id) #najnowsza data pliku xml

        if last_date: #jesli jest
            last_dt = datetime.strptime(last_date, "%Y-%m-%d")#zmien na datetime
            window_dt = datetime.utcnow() - timedelta(days=30 * refresh_window_months)#oblicz date 180 dni wstecz od dzis
            start_dt = max(last_dt, window_dt) #wybierz najnowszą date spośród (od dzis 180 dni wstecz vs ostatnia data z pliku)
            start_date = start_dt.strftime("%Y-%m-%d")
        else:
            start_date = None

        url = self._build_observations_url(series_id, start_date)#buduje url i dopisuje start_date

        self._rate_limit()
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        xml_path = self._raw_xml_path(series_id)
        with open(xml_path, "wb") as f:
            f.write(response.content)

        logger.info(f"Saved raw XML for {series_id} -> {xml_path}")
        return xml_path

