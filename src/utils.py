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
SILVER_FILE = os.path.join(SILVER_PATH, "transactions.parquet")
GOLD_FRAUD_STATS = os.path.join(GOLD_PATH, "fraud_stats.parquet")
GOLD_FRAUD_HOURLY = os.path.join(GOLD_PATH, "fraud_hourly.parquet")
GOLD_FRAUD_AMOUNT_BUCKETS = os.path.join(GOLD_PATH, "fraud_amount_buckets.parquet")
GOLD_AMOUNT_DIST = os.path.join(GOLD_PATH, "amount_dist.parquet")
GOLD_AMOUNT_DIST_BY_CLASS = os.path.join(GOLD_PATH, "amount_dist_by_class.parquet")
NULL_REPORT_FILE = os.path.join(DQ_PATH, "null_report.csv")
DUPLICATE_REPORT_FILE = os.path.join(DQ_PATH, "duplicate_report.csv")
RANGE_REPORT_FILE = os.path.join(DQ_PATH, "range_report.csv")
SCHEMA_REPORT_FILE = os.path.join(DQ_PATH, "schema_report.csv")
INVALID_CLASS_REPORT_FILE = os.path.join(DQ_PATH, "invalid_class_report.csv")
OUTLIER_REPORT_FILE = os.path.join(DQ_PATH, "outlier_report.csv")


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


def read_csv(spark, path):
    '''Read a CSV file into a Spark DataFrame'''
    return spark.read.csv(path, header=True, inferSchema=True)


def write_csv(df, path):
    '''Write a Spark DataFrame to CSV format'''
    df.write.mode("overwrite").option("header", "true").csv(path)


def read_parquet(spark, path):
    '''Read a Parquet file into a Spark DataFrame'''
    return spark.read.parquet(path)


def write_parquet(df, path):
    '''Write a Spark DataFrame to Parquet format'''
    df.write.mode("overwrite").parquet(path)


def print_step(message):
    print(f"[STEP] {message}")


def print_success(message):
    print(f"[SUCCESS] {message}")
    