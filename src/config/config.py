from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

settings_path = "/Workspace/Users/jakub.kaczmarczyk443@gmail.com/dbx-invest-analytics/src/config/settings.yaml"

### qqq_all_level_filepaths ###

raw_csv_filespath = "/Volumes/workspace/bronze/raw/"
raw_qqq_delta_file_path = "workspace.bronze.qqq_etf_constituents"
raw_qqq_entities_filename = "qqq-etf-constituents.csv"
delta_qqq_filepath = "bronze.qqq_etf_constituents"
delta_qqq_enriched_filename = "bronze.qqq_etf_entities_enriched"
qqq_silver_filepath = "silver.qqq_silver_entities"

### fred_client ###

#api
raw_macro_filespath = "/Volumes/workspace/bronze/raw/macro/"
api_key_fred = dbutils.secrets.get(scope="my-scope", key="API_KEY_FRED")
fred_base_url = "https://api.stlouisfed.org/fred"

#limits
rate_limit_per_second = 2
window_refresh_months = 6

#macro_indicators
series_id = [
    "CPIAUCSL",
    "CPILFESL",
    "FEDFUNDS",
    "DGS10",
    "DGS2",
    "UNRATE",
    "INDPRO",
    "T10YIE",
]