import pytest
from datetime import date
from src.etl.transformations.av_json_parser import AvJsonParser

from datetime import datetime

# ---------- happy path ----------
def test_parse_single_valid_article():

    feed = [
        {
            "time_published": "20260314T134939",
            "source": "MLQ.ai",
            "title": "Some title",
            "overall_sentiment_score": 0.09,
            "overall_sentiment_label": "Neutral",
            "ticker_sentiment": [
                {
                    "ticker": "AAPL",
                    "relevance_score": "1.0",
                    "ticker_sentiment_score": "0.08",
                    "ticker_sentiment_label": "Neutral",
                }
            ]
        }
    ]

    valid_symbols = {
        "AAPL"
    }

    result = list(AvJsonParser.parse(feed, valid_symbols))

    assert len(result) == 1
    assert result[0]["published_at"] == datetime(2026, 3, 14, 13, 49, 39)
    assert result[0]["source"] == "MLQ.ai"
    assert result[0]["title"] == "Some title"
    assert result[0]["ticker_relevance_score"] == 1.0
    assert result[0]["ticker_sentiment_score"] == 0.08
    assert result[0]["ticker_sentiment_label"] == "Neutral"
    assert result[0]["article_overall_sentiment_score"] == 0.09
    assert result[0]["article_overall_sentiment_label"] == "Neutral"
    assert result[0]["symbol"] == "AAPL"

# ---------- filtrowanie tickerów ----------
def test_parse_ticker_not_in_valid_symbols_skips_record():

    feed = [
        {
            "time_published": "20260314T134939",
            "source": "MLQ.ai",
            "title": "Some title",
            "overall_sentiment_score": 0.09,
            "overall_sentiment_label": "Neutral",
            "ticker_sentiment": [
                {
                    "ticker": "MSFT",
                    "relevance_score": "1.0",
                    "ticker_sentiment_score": "0.08",
                    "ticker_sentiment_label": "Neutral",
                }
            ]
        }
    ]
    
    valid_symbols = {
        "AAPL"
    }
    
    result = list(AvJsonParser.parse(feed, valid_symbols))

    assert len(result) == 0

def test_parse_ticker_with_empty_feed():

    feed = []
    valid_symbols = {
        "AAPL"
    }

    result = list(AvJsonParser.parse(feed, valid_symbols))

    assert len(result) == 0

# ---------- edge cases ----------
def test_parse_ticker_for_multiple_symbols_in_article():

    feed = [
        {
            "time_published": "20260314T134939",
            "source": "MLQ.ai",
            "title": "Some title",
            "overall_sentiment_score": 0.09,
            "overall_sentiment_label": "Neutral",
            "ticker_sentiment": [
                {
                    "ticker": "MSFT",
                    "relevance_score": "1.0",
                    "ticker_sentiment_score": "0.08",
                    "ticker_sentiment_label": "Neutral",
                },
                {
                    "ticker": "AAPL",
                    "relevance_score": "1.0",
                    "ticker_sentiment_score": "0.08",
                    "ticker_sentiment_label": "Neutral",   
                },
                {
                    "ticker": "CSCO",
                    "relevance_score": "1.0",
                    "ticker_sentiment_score": "0.08",
                    "ticker_sentiment_label": "Neutral",     
                }
            ]
        }
    ]
    
    valid_symbols = {
        "AAPL",
        "MSFT"
    }

    result = list(AvJsonParser.parse(feed, valid_symbols))

    assert len(result) == 2