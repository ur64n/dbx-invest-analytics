import time
import requests
from typing import Optional
from src.config.logger import get_logger

logger = get_logger("av_sentiment_client")

class AlphaVantageSentimentClient:
    """ 
    Client for Alpha Vantage news sentiment.
    Extracts per ticker news sentiment (score, label, relevance)

    (Free tier) - 25 request/day, 5 requests/min.
    """

    def __init__(self, config: dict, api_key: str):
        self.base_url = config["alpha_vantage"]["base_url"]
        self.api_key = api_key
        self.limit = config["alpha_vantage"]["articles_per_request"]
        self.rate_limit_per_min = config["alpha_vantage"]["rate_limit_per_min"]
        self.articles_per_request = config["alpha_vantage"]["articles_per_request"]

    # ---------- helpers ----------

    def _rate_limit(self):
        time.sleep(60.0 / self.rate_limit_per_min) # timesleep = 12 sec.

    def _build_url(
        self,
        ticker: str,
        time_from: Optional[str] = None
        time_to: Optional[str] = None
        sort: str = "LATEST"
    ) -> str:
        
        params = {
            "funtion": "NEWS_SENTIMENT",
            "tickers": ticker,
            "limit": self.limit,
            "sort": sort,
            "apikey": self.api_key
        }
        if time_from:
            params["time_from"] = time_from
        if time_to:
            params["time_to"] = time_to

        query = "&".join(f"{k}={v}" for k, v in params.items())
        return f"{self.base_url}?{query}
    
    # ---------- public API ----------

    def fetch_sentiment(
        self,
        ticker: str,
        time_from: Optional[str] = None,
        time_to: Optional[str] = None,
        limit: int = 
    )