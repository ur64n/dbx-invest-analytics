from pyspark.sql import SparkSession
from src.etl.cleaning.fred_cleaning import FredCleaner
from src.etl.schema.fred_schema import fred_schema

spark = SparkSession.builder.getOrCreate()

df = spark.table("twoja_silver_lub_bronze_tabela")

cleaned_df = FredCleaner.clean(df)

cleaned_df.printSchema()
cleaned_df.show(20, False)
