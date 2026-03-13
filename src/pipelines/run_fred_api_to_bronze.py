from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pyspark.sql.functions import col

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

from src.config.logger import get_logger
from src.config.config_loader import load_config

from src.etl.schema.fred_schema import fred_schema
from src.etl.schema.fred_metadata_schema import fred_metadata_schema
from src.etl.extraction.fred.fred_client import FredClient
from src.etl.extraction.fred.fred_run_metadata import FredRunMetadataWriter
from src.etl.transformations.fred_xml_parser import FredXMLParser
from src.etl.validation.fred_validation import FredValidator
from src.etl.validation.fred_metadata_validation import FredMetadataValidator
from src.etl.write.delta_table_writer import DeltaTableWriter

logger = get_logger("fred_api_to_bronze")

def run():
    logger.info("Starting FRED macro indicators pipeline")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate()
    config = load_config()

    # ---------- secrets ----------
    dbutils = DBUtils(spark)
    fred_api_key = dbutils.secrets.get("my-scope", "API_KEY_FRED")

    # ---------- parameters ----------
    series_ids = config["fred"]["series_ids"]
    refresh_window_months = config["fred"]["window_refresh_months"]
    run_ts = datetime.now(ZoneInfo("Europe/Warsaw"))

    # ---------- tables ----------
    silver_macro_indicators = config["tables"]["silver_fred_macro_indicators"]
    silver_macro_indicator_metadata = config["tables"]["silver_fred_macro_indicator_metadata"]

    # ---------- window refresh logic ----------
    exists = spark.catalog.tableExists(silver_macro_indicators)

    has_data = (
        exists 
        and spark.table(silver_macro_indicators).limit(1).count() > 0
    )
    if not has_data:
        observation_start = None 
    else:
        observation_start = (
            datetime.utcnow() - timedelta(days=30 * refresh_window_months)
        ).strftime("%Y-%m-%d")

    logger.info(
        f"Extraction mode: {'BOOTSTRAP' if not has_data else 'REFRESH'} | "
        f"Observation start = {observation_start}"
    )

    # ---------- extraction ----------
    fred_client = FredClient(
        config=config, 
        api_key=fred_api_key
        )
    
    metadata_writer = FredRunMetadataWriter(spark)

    raw_xml_paths: list[tuple[str, str]] = []
    metadata_rows: list[dict[str, Optional[str]]] = []

    logger.info(f"Starting extraction for {len(series_ids)} series")
    
#TODO: Move iteration to extractor
    for series_id in series_ids: 
        try:
            logger.info(f"Extracting series {series_id}")

            xml_path = fred_client.download_series(
                series_id=series_id,
                observation_start=observation_start,
            )

            macro_metadata = fred_client.download_series_metadata(
                series_id=series_id
            )

            if macro_metadata is None:
                logger.warning(f"No metadata returned for {series_id} - skipping metadata append")
            else:
                metadata_rows.append(macro_metadata)

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

    logger.info(
    f"Extraction finished | "
    f"successful_series={len(raw_xml_paths)} | "
    f"metadata_rows={len(metadata_rows)}"
    )

    success_count = len(raw_xml_paths)
    failure_count = len(series_ids) - success_count

    logger.info(
        f"FRED extraction summary | "
        f"total={len(series_ids)} | "
        f"success={success_count} | "
        f"failure={failure_count}"
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

    # ---------- spark_df ---------- 
    fact_df = spark.createDataFrame(
        rows, 
        schema=fred_schema
        ) # dict -> df
    dim_df = spark.createDataFrame(
        metadata_rows, 
        schema=fred_metadata_schema
        ) # metadata unit frequency df 
    
    # ---------- validation ---------- 
    FredValidator.validate_schema(fact_df)
    FredValidator.validate_not_empty(fact_df)

    FredMetadataValidator.validate_metadata_schema(dim_df)
    FredMetadataValidator.validate_metadata_not_empty(dim_df)
    
    # ---------- write ---------- 
    DeltaTableWriter(
        spark=spark,
        table_name=config["tables"]["bronze_fred_macro_indicators"]
        ).overwrite_schema(fact_df)
    
    DeltaTableWriter(
        spark=spark,
        table_name=config["tables"]["bronze_fred_macro_indicator_metadata"]
        ).overwrite_schema(dim_df)
    
    logger.info("Fred pipeline api to bronze finished successfully")
    
if __name__ == "__main__":
    run()

