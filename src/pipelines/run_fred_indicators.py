from datetime import datetime
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

from src.config.logger import get_logger
from src.config.config_loader import load_config

from src.etl.schema.fred_schema import fred_schema
from src.etl.extraction.fred.fred_client import FredClient
from src.etl.extraction.fred.fred_run_metadata import FredRunMetadataWriter
from src.etl.transformations.fred_xml_parser import FredXMLParser
from src.etl.cleaning.fred_cleaning import FredCleaner
from src.etl.validation.fred_validation import FredValidator

""" importy modulow uruchamiaja kod top-level, w importowanych modulach czyli wszystkie importy wszystko co jest poza definicją klasy """

logger = get_logger("fred_pipeline")


def run():
    logger.info("Starting FRED macro indicators pipeline")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    fred_api_key = dbutils.secrets.get("my-scope", "API_KEY_FRED")

    config = load_config()
    run_ts = datetime.now(ZoneInfo("Europe/Warsaw"))

    series_ids = config["fred"]["series_ids"]
    refresh_window_months = config["fred"]["window_refresh_months"]

    # ---------- extraction ----------
    fred_client = FredClient(config=config, api_key=fred_api_key) 
    metadata_writer = FredRunMetadataWriter(spark)

    raw_xml_paths: list[tuple[str, str]] = []

    for series_id in series_ids:
        try:
            logger.info(f"Extracting series {series_id}")

            xml_path = fred_client.download_series(
                series_id=series_id,
                refresh_window_months=refresh_window_months,
            )

            metadata_writer.write_success(
                series_id=series_id,
                run_ts=run_ts,
                xml_path=xml_path,
            )

            raw_xml_paths.append((series_id, xml_path))

        except Exception as e:
            logger.error(f"Extraction failed for {series_id}", exc_info=True)
            metadata_writer.write_failure(
                series_id=series_id,
                run_ts=run_ts,
                error_message=str(e),
            )

    # ---------- transform ----------
    parser = FredXMLParser()
    rows = []

    for series_id, xml_path in raw_xml_paths:
        with open(xml_path, "rb") as f:
            xml_bytes = f.read()

        rows.extend(
            parser.parse(
                xml_content=xml_bytes,
                indicator_id=series_id,
            )
        )

    if not rows:
        raise RuntimeError("No FRED data extracted - pipeline stopped")

    combined_df = spark.createDataFrame(rows, schema=fred_schema) # xml -> df

    # ---------- validation ----------
    FredValidator.validate_schema(combined_df)
    FredValidator.validate_not_empty(combined_df)
    FredValidator.validate_domain_rules(combined_df)
    FredValidator.validate_uniqueness(combined_df)

    # ---------- cleaning ----------
    cleaned_df = FredCleaner.clean(combined_df)

    # ---------- write SILVER ----------
    logger.info("Writing SILVER fred_indicators table")

    (
        cleaned_df.write.format("delta")
        .mode("overwrite")
        .saveAsTable("silver.fred_indicators")
    )

    logger.info("FRED pipeline finished successfully")


if __name__ == "__main__":
    run()
