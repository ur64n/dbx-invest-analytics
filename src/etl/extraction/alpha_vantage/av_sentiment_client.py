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

    #TODO: Move to cfg... 
    BASE_URL = "https://www.alphavantage.co/query"
    SOURCE_API = "alpha_vantage" 

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.rate_limit_per_min = rate_limit_per_min
