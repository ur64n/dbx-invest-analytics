import time
import requests
from typing import Optional
from src.config.logger import get_logger

logger = get_logger("av_sentiment_history_client")

class AVSentimentHistoryClient:
    """ 
    Client for Alpha Vantage news sentiment history data.
    Extracts per ticker news sentiment (score, label, relevance)

    (Free tier) - 25 request/day, 5 requests/min.
    """

    def __init__(self, config: dict, api_key: str):
        self.base_url = config["alpha_vantage"]["base_url"]
        self.api_key = api_key
        self.rate_limit_per_min = config["alpha_vantage"]["rate_limit_per_min"]
        self.articles_per_request = config["alpha_vantage"]["articles_per_request"]
        self.time_from = config["alpha_vantage"]["time_from"]
        self.topics = "," .join(config["alpha_vantage"]["topics"])

    # helpers

    def _rate_limit(self):
        time.sleep(60.0 / self.rate_limit_per_min) # timesleep = 12 sec.

    def _build_url(self, ticker: str, time_to: Optional[str], sort: str = "EARLIEST") -> str:

        params = {
            "function": "NEWS_SENTIMENT",
            "tickers": ticker,
            "limit": self.articles_per_request,
            "sort": sort,
            "time_from": self.time_from,
            "apikey": self.api_key,
            "topics": self.topics
        }

        if time_to:
            params["time_to"] = time_to

        query = "&".join(f"{k}={v}" for k, v in params.items())
        return (f"{self.base_url}?{query}")

    #api call

    def fetch_sentiment(self, ticker: str, time_to: Optional[str]) -> dict:

        url = self._build_url(ticker, time_to)
    

        logger.info(f"Fetching sentiment data for {ticker}")

        self._rate_limit()

        response = requests.get(url, timeout=30)
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

        logger.info(f"Fetched {len(feed)} articles for: {ticker}")

        return data