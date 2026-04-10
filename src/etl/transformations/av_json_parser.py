from typing import Iterable, Dict
from datetime import datetime

from src.config.logger import get_logger

logger = get_logger("av_json_parser")

class AvJsonParser:
    """Parses Alpha Vantage news sentiment JSON feed into flat dicts.

    Filters articles to only include tickers present in valid_symbols.
    Yields one dict per (article, ticker) pair with sentiment scores.
    """
    
    @staticmethod
    def parse(feed: list[dict], valid_symbols: set) -> Iterable[dict]:
        logger.info("Parsing AV sentiment feed")

        for article in feed:
            for ticker_data in article["ticker_sentiment"]:
                if ticker_data["ticker"] not in valid_symbols: 
                    continue

                yield {
                    "symbol": ticker_data["ticker"],
                    "published_at": datetime.strptime(article["time_published"], "%Y%m%dT%H%M%S"),
                    "source": article["source"],
                    "title": article["title"],
                    "ticker_relevance_score": float(ticker_data["relevance_score"]),
                    "ticker_sentiment_score": float(ticker_data["ticker_sentiment_score"]),
                    "ticker_sentiment_label": ticker_data["ticker_sentiment_label"],
                    "article_overall_sentiment_score": article["overall_sentiment_score"],
                    "article_overall_sentiment_label": article["overall_sentiment_label"]
                }