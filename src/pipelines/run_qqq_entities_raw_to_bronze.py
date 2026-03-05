from pyspark.sql import SparkSession
from src.config.logger import get_logger
from src.config.config_loader import load_config

from src.etl.schema.qqq_schema import qqq_schema 
from src.etl.extraction.qqq_entities_extraction import QQQCSVExtractor
from src.etl.cleaning.qqq_entities_cleaning import QQQEntitiesCleaner
from src.etl.validation.qqq_entities_validation import QQQEntitiesValidator
from src.etl.transformations.qqq_entities_transformation import QQQEntitiesTransformer
from src.etl.write.delta_table_writer import DeltaTableWriter

logger = get_logger("qqq_entities_raw_to_bronze_pipeline")

def run(env: str = "dev"):
    logger.info("Starting qqq_entities_raw_to_bronze_pipeline")

    # ---------- setup ----------
    spark = SparkSession.builder.getOrCreate() 
    cfg = load_config(env)

    # ---------- extraction ----------
    extractor = QQQCSVExtractor(
        spark=spark,
        raw_path=cfg["paths"]["raw_csv"],
        expected_filename=cfg["files"]["qqq_entities"],
        schema=qqq_schema,
    )

    # ---------- read data ----------
    raw_df = extractor.read()

    # ---------- cleaning ----------
    cleaned_df = QQQEntitiesCleaner.remove_invalid_rows(raw_df)

    # ---------- validation ----------
    QQQEntitiesValidator.validate_raw_csv_schema(cleaned_df)
    QQQEntitiesValidator.validate_raw_csv_not_empty(cleaned_df)
#TODO: consider move this valid to bronze_to_silver_pipeline
#QQQEntitiesValidator.validate_no_null_holdings(df)

    # ---------- transform ----------
    bronze_df = QQQEntitiesTransformer.transform_raw(cleaned_df)

    # ---------- validation ----------
    QQQEntitiesValidator.validate_column_values(bronze_df)
    QQQEntitiesValidator.validate_symbol_uniqueness(bronze_df)

    # ---------- write BRONZE ----------
    DeltaTableWriter(
        table_name=cfg["tables"]["bronze_qqq"],
        spark=spark
    ).overwrite(bronze_df)

#TODO: move enrichment to silver_to_gold pipeline


    # # ---------- enrichment ----------
    # symbols = [r.symbol for r in bronze_df.select("symbol").distinct().collect()]

    # category_df = QQQCategoriesExtractor(spark).extract(symbols)
    # enriched_df = QQQEntitiesEnricher.enrich(bronze_df, category_df)

    # enriched_df.write.format("delta").mode("overwrite").saveAsTable(
    #     cfg["tables"]["bronze_qqq_enriched"]
    # )

    # # ---------- cleaning ----------
    # silver_df = QQQEntitiesCleaner.clean_columns(enriched_df)
    # silver_df = QQQEntitiesCleaner.clean_rows(silver_df)

    # # ---------- validation ----------
    

    # # ---------- write SILVER ----------
    # silver_df.write.format("delta").mode("overwrite").saveAsTable(
    #     cfg["tables"]["silver_qqq"]
    # )

    # logger.info("Pipeline finished successfully")


if __name__ == "__main__":
    run()
