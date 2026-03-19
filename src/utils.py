''' 
TODO

1. create_spark_session()
2. read_csv(path)
3. read_parquet(path)
4. write_parquet(df, path)
5. print_step(message)
6. print_success(message)
7. compare_schema(df, expected_schema)
8. check_missing(df)
9. check_duplicates(df)
10. check_invalid_amount(df)
11. get_current_timestamp()
12. add_ingestion_timestamp(df)
'''

import os
from pyspark.sql import SparkSession

# Project root = two levels up from src/utils.py
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Data layer paths
RAW_PATH = os.path.join(BASE_DIR, 'data', 'raw')
BRONZE_PATH = os.path.join(BASE_DIR, 'data', 'bronze')
SILVER_PATH = os.path.join(BASE_DIR, 'data', 'silver')
GOLD_PATH = os.path.join(BASE_DIR, 'data', 'gold')
DQ_PATH = os.path.join(BASE_DIR, 'data', 'dq_reports')

RAW_FILE = os.path.join(RAW_PATH, "creditcard.csv")
BRONZE_FILE = os.path.join(BRONZE_PATH, "transactions.parquet")

def create_spark_session(app_name="RealBankTransactions"):
    '''Create and return a SparkSession'''
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName(app_name) \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()
    return spark

