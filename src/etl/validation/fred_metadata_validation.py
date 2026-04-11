from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src.config.logger import get_logger

logger = get_logger("fred_metadata_validation")

class FredMetadataValidator:
    """Validates FRED metadata canonical standards.

    Ensures frequency values are in canonical set: {d, m, q, a}.
    """
    
    @staticmethod
    def validate_canonical_frequency(df: DataFrame) -> None:
        logger.info("Validating fred metadata frequency cannonical standard")

        allowed = ["m", "d", "q", "a"]

        if df.filter(~col("frequency").isin(allowed)).head(1):
            raise ValueError(f"Invalid frequency in metadata")

            # ("~"col) ~ odwaraca warunek. Czyli (isin = not in)
