import os
import time
import requests
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

        query = "&".join(f"{k}={v}" for k, v in params.items())
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

        with open(path, "rb") as f:
            content = f.read().decode("utf-8")

        dates = []
        for part in content.split("<date>")[1:]:
            try:
                d = part.split("</date>")[0].strip()
                dates.append(datetime.strptime(d, "%Y-%m-%d"))
            except Exception:
                continue

        return max(dates).strftime("%Y-%m-%d") if dates else None

    # ---------- public API ----------

    def download_series(self, series_id: str, refresh_window_months: int) -> str:
        logger.info(f"Downloading FRED series {series_id}")

        last_date = self._last_loaded_date(series_id)

        if last_date:
            last_dt = datetime.strptime(last_date, "%Y-%m-%d")
            window_dt = datetime.utcnow() - timedelta(days=30 * refresh_window_months)
            start_dt = max(last_dt + timedelta(days=1), window_dt)
            start_date = start_dt.strftime("%Y-%m-%d")
        else:
            start_date = None  # FULL HISTORY

        url = self._build_observations_url(series_id, start_date)

        self._rate_limit()
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        xml_path = self._raw_xml_path(series_id)
        with open(xml_path, "wb") as f:
            f.write(response.content)

        logger.info(f"Saved raw XML for {series_id} -> {xml_path}")
        return xml_path

