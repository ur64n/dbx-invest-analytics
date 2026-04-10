import time
import requests
from typing import Optional
from src.config.logger import get_logger
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = get_logger("av_sentiment_client")

class AlphaVantageSentimentClient:
    """Client for Alpha Vantage news sentiment latest data.

    Fetches per-ticker news sentiment (score, label, relevance)
    sorted by most recent articles first.
    Handles API rate limiting and error responses (rate limit, error message).

    Free tier: 25 requests/day, 5 requests/min.
    """

    def __init__(self, config: dict, api_key: str):
        self.base_url = config["alpha_vantage"]["base_url"]
        self.api_key = api_key
        self.rate_limit_per_min = config["alpha_vantage"]["rate_limit_per_min"]
        self.articles_per_request = config["alpha_vantage"]["articles_per_request"]
        self.topics = ",".join(config["alpha_vantage"]["topics"])

        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

    # ---------- helpers ----------

    def _rate_limit(self):
        time.sleep(60.0 / self.rate_limit_per_min) # timesleep = 12 sec.

    def _build_url(self,ticker: str,time_from: Optional[str],sort: str = "LATEST") -> str:
        
        params = {
            "function": "NEWS_SENTIMENT",
            "tickers": ticker,
            "limit": self.articles_per_request,
            "sort": sort,
            "apikey": self.api_key,
            "topics": self.topics
        }
        if time_from:
            params["time_from"] = time_from

        query = "&".join(f"{k}={v}" for k, v in params.items())
        return (f"{self.base_url}?{query}")
    
    # ---------- public API ----------

    def fetch_sentiment(self,ticker: str,time_from: Optional[str]) -> dict:
        """Fetch latest sentiment articles for a given ticker.

        Returns raw API JSON with 'feed' key containing articles.
        On rate limit or error, returns dict with empty feed and status flag.
        """
        url = self._build_url(ticker, time_from)

        logger.info(f"Fetching sentiment data for {ticker}")

        self._rate_limit()

        response = self.session.get(url, timeout=30)
        response.raise_for_status()

        data = response.json()

        if "Note" in data or "Information" in data:
            msg = data.get("Note") or data.get("Information")
            logger.warning(f"API limit reached for {ticker}: {msg}")
            return {"feed": [], "_rate_limited": True}
 
        if "Error Message" in data:
            logger.error(f"API error for {ticker}: {data['Error Message']}")
            return {"feed": [], "_error": data["Error Message"]}
    
        feed = data.get("feed", [])

        logger.info(f"Fetched {len(feed)} articles for {ticker}")

        return data


