from pyspark.sql.types import *

REQUIRED_COLUMNS = {
    "date",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume"
}

NOT_NULL_COLUMNS = {
    "symbol",
    "date",
    "open",
    "high",
    "low",
    "close",
}

UNIQUE_KEY = [
    "date",
    "symbol"
]

PRICE_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "adj_close",
]