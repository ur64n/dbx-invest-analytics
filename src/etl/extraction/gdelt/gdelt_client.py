import requests
import hashlib
from typing import Optional
from src.config.logger import get_logger

logger = get_logger("gdelt_extraction")

class GdeltClient:

    def __init__(self, config: dict):
        self.base_url = config["gdelt"]["base_url"]
        self.max_records = config["gdelt"]["max_records"]
        self.timespan = config["gdelt"]["timespan"]
        self.language = config["gdelt"]["language"]

    def _build_url(self, keyword: str) -> str:

        query = f'"{keyword}" sourcelang:{self.language}'

        return (
            f"{self.base_url}?"
            f"query={query}"
            f"&mode=artlist"
            f"&maxrecords={self.max_records}"
            f"&format=json"
            f"&sort=datedesc"
            f"&timespan={self.timespan}"
        )

    @staticmethod
    def _generate_article_id(url: str) -> str:
        return hashlib.sha256(url.encode()).hexdigest()

    def fetch_articles(self, keyword: str) -> str:
        
        url = self._build_url(keyword)

        logger.info(f"fetching GDELT articles for keyword: {keyword}")

        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        articles = data.get("articles", [])

        results = []

        for article in articles:
            results.append({
                "article_id": self._generate_article_id(article.get("url")),
                "title": article.get("title"),
                "url": article.get("url"),
                "domain": article.get("domain"),
                "source_country": article.get("sourcecountry"),
                "language": article.get("language"),
                "published_at": article.get("seendate"),
                "search_keyword": keyword,
            })

        logger.info(f"Fetched {len(results)} articles for keyword {keyword}")

        return results
    
