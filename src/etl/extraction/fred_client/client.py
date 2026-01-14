import os
import time
import requests
from src.config.logger import get_logger
from datetime import datetime, timedelta
from pyspark.dbutils import DBUtils

from src.config.config import (
    api_key_fred,
    fred_base_url,
    raw_macro_filespath,
    series_id,
    rate_limit_per_second,
    window_refresh_months,
)
from src.etl.extraction.fred_client.endpoints import observations_url

logger = get_logger("extraction_fred")

class FredClient:
    def __init__(self):
        self.api_key = api_key_fred
        self.base_url = fred_base_url
        self.raw_folder = raw_macro_filespath

        # upewnij się, że katalog istnieje
        os.makedirs(self.raw_folder, exist_ok=True)

    def _call_api(self, endpoint: str) -> bytes:
        """
        Execut HTTP GET to FRED API, and return raw data bites.
        With rate simply limiting.
        """
        url = f"{self.base_url}/{endpoint}&api_key={self.api_key}"
        time.sleep(1.0 / rate_limit_per_second)

        response = requests.get(url)
        response.raise_for_status()

        logger.info("FRED API call successful")
        return response.content

    def _write_raw(self, series_id: str, data: bytes):
        """
        Write raw XML to file {series_id}.xml
        """
        filename = os.path.join(self.raw_folder, f"{series_id}.xml")
        with open(filename, "wb") as f:
            f.write(data)

        logger.info(f"Raw XML written to {filename}")

    def _last_loaded_date(self, series_id: str) -> str:

        """
        Return last date from raw file for prosecced series_id
        """

        path = os.path.join(self.raw_folder, f"{series_id}.xml")

        if not os.path.exists(path):
            logger.info(f"No existing XML for series={series_id}")
            return None

        logger.info(f"Reading existing XML for series={series_id}")

        with open(path, "rb") as f:
            content = f.read()

        matches = content.decode("utf-8").split("<date>")
        if len(matches) <= 1:
            logger.warning(f"No <date> tags found for series={series_id}")
            return None

        last = None
        for part in matches[1:]:
            date_str = part.split("</date>")[0].strip()
            try:
                d = datetime.strptime(date_str, "%Y-%m-%d")
                if not last or d > last:
                    last = d
            except Exception:
                continue

        if last:
            logger.info(f"Last loaded date for series={series_id}: {last.date()}")
            return last.strftime("%Y-%m-%d")

        return None

    def _compute_start_date(self, last_date: str) -> str:

        if last_date:
            last_dt = datetime.strptime(last_date, "%Y-%m-%d")

            window_dt = datetime.today() - timedelta(days=30 * window_refresh_months)

            start = max(last_dt + timedelta(days=1), window_dt)

            logger.info("No last_date found, full history will be downloaded")
            
            return start.strftime("%Y-%m-%d")

        return None

    def download_series(self, series_id: str):

        logger.info(f"Start extraction xml file for series = {series_id}")

        last_date = self._last_loaded_date(series_id)
        start_date = self._compute_start_date(last_date)

        endpoint = observations_url(series_id, start_date=start_date)
        raw = self._call_api(endpoint)
        self._write_raw(series_id, raw)

        logger.info(f"Download completed for series={series_id}")

        return series_id

    def download_all(self):

        logger.info("Start extraction xml files with macro indicators from fred_api")

        downloaded = []
        for sid in series_id:
            try:
                downloaded.append(self.download_series(sid))
            except Exception as e:
                logger.error(
                    f"Extraction failed for series={sid}",
                    exc_info=True
                )

        logger.info(f"Extraction finished. Success count={len(downloaded)}")
        return downloaded

if __name__ == "__main__":
    client = FredClient()
    done = client.download_all()

