from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src.config.logger import get_logger
from pyspark.sql.functions import current_date

logger = get_logger("fred_validation")

#TODO: Zmienić z .count() na .head(1)

class FredValidator:

    @staticmethod
    def validate_domain_rules(df: DataFrame) -> None:
        logger.info("Validating domain rules (indicator_id, date, value)")

        # indicator_id not null
        if df.filter(col("indicator_id").isNull()).count() > 0:
            raise ValueError("indicator_id contains NULLs")

        # date not in the future
        if df.filter(col("date") > current_date()).count() > 0:
            raise ValueError("date contains future values")

        # value >= 0 (NULL allowed)
        negatives = df.filter(col("value") < 0).count()
        if negatives > 0:
            raise ValueError(f"Found {negatives} negative indicator values")
