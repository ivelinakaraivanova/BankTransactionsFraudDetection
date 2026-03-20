from utils import BRONZE_FILE, RAW_FILE, create_spark_session, print_step, print_success, read_csv, write_parquet
from pyspark.sql import functions as F


def run():
    spark = create_spark_session("ETL_Bronze")
    
    # Read raw CSV files
    print_step("Loading raw data…")
    df = read_csv(spark, RAW_FILE)
    
    # Add ingestion timestamp
    df = df.withColumn("ingestion_timestamp", F.current_timestamp()) \
           .withColumn("source_file", F.lit("creditcard.csv"))
    
    # Write to bronze layer in Parquet format
    print_step("Writing Bronze layer…")
    write_parquet(df, BRONZE_FILE)

    print_success("Bronze layer saved.")
    print(f"Bronze data written to: {BRONZE_FILE}")
    print(f"Bronze row count: {df.count()}")
    df.printSchema()


if __name__ == "__main__":
    run()