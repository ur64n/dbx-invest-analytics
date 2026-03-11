from pyspark.sql import SparkSession
from src.config.config_loader import load_config
from src.config.logger import get_logger

from src.etl.extraction.gdelt.gdelt_client import GdeltClient
from src.etl.schema.gdelt_schema import gdelt_schema

logger = get_logger("gdelt_new_api_to_bronze")

def run():
    logger.info("Starting gdelt_new_api_to_bronze pipeline")
    
    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    # ---------- extractor ----------
    gdelt_client = GdeltClient(config)
    all_articles = []

    for kw in config["gdelt"]["keywords"]:
        articles = gdelt_client.fetch_articles(kw)
        all_articles.extend(articles)

    # ---------- deduplicate ----------
    seen = set()
    unique_articles = []

    for a in all_articles:
        if a["article_id"] not in seen:
            seen.add(a["article_id"])
            unique_articles.append(a)
    
    # ---------- create df ----------
    df = spark.createDataFrame(
        unique_articles,
        gdelt_schema
    )

    df.limit(10).display()

if __name__ == "__main__":
    run()
