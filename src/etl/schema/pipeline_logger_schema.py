from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, DoubleType

PPL_LOGGER_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("pipeline_name", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("status", StringType(), False),
    StructField("started_at", TimestampType(), False),
    StructField("finished_at", TimestampType(), False),
    StructField("input_rows", LongType(), True),
    StructField("output_rows", LongType(), True),
    StructField("error_message", StringType(), True),
    StructField("duration_seconds", DoubleType(), True),
    StructField("source_table", StringType(), True),
    StructField("target_table", StringType(), True),
    StructField("rows_rejected", LongType(), True),
    StructField("config_params", StringType(), True)
])