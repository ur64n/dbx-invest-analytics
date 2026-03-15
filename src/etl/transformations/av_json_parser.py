from typing import Iterable, Dict
from datetime import datetime

from src.config.logger import get_logger

logger = get_logger("av_json_parser")

class AvJsonParser:

    @staticmethod
    def parse(feed: list[dict]) -> Iterable[dict]:
        logger.info("Parsing AV sentiment feed")

        for article in feed:
            for ticker_data in article["ticker_sentiment"]:
                yield {
                    "symbol": ticker_data["ticker"],
                    "published_at": article["time_published"],
                    "source": article["source"],
                    "title": article["title"],
                    "relevance_score": float(ticker_data["relevance_score"]),
                    "sentiment_score": float(ticker_data["ticker_sentiment_score"]),
                    "sentiment_label": ticker_data["ticker_sentiment_label"],
                    "overall_sentiment_score": article["overall_sentiment_score"],
                    "overall_sentiment_label": article["overall_sentiment_label"]
                }