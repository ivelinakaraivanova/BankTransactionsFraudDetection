from utils import BRONZE_FILE, RAW_FILE, create_spark_session, RAW_PATH, BRONZE_PATH
from pyspark.sql import functions as F

def run():
    spark = create_spark_session("ETL_Bronze")
    
    # Read raw CSV files
    df = spark.read.csv(RAW_FILE, header=True, inferSchema=True)
    
    # Add ingestion timestamp
    df = df.withColumn("ingestion_timestamp", F.current_timestamp()) \
           .withColumn("source_file", F.lit("creditcard.csv"))
    
    # Write to bronze layer in Parquet format
    df.write.mode("overwrite").parquet(BRONZE_FILE)

    print(f"Bronze data written to: {BRONZE_FILE}")
    print(f"Bronze row count: {df.count()}")
    df.printSchema()


if __name__ == "__main__":
    run()