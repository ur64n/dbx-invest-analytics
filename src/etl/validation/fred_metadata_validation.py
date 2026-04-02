from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src.config.logger import get_logger

from src.etl.schema.fred_metadata_schema import REQUIRED_COLUMNS

logger = get_logger("fred_metadata_validation")

class FredMetadataValidator:

    @staticmethod
    def validate_canonical_frequency(df: DataFrame) -> None:
        logger.info("Validating fred metadata frequency cannonical standard")

        allowed = ["m", "d", "q", "a"]

        if df.filter(~col("frequency").isin(allowed)).count() > 0:
            raise ValueError(f"Invalid frequency in metadata")

            # ("~"col) ~ odwaraca warunek. Czyli (isin = not in)

