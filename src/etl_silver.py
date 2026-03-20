from utils import SILVER_FILE, BRONZE_FILE, create_spark_session, print_step, print_success, read_parquet, write_parquet
from pyspark.sql import functions as F


def run():
    spark = create_spark_session("ETL_Silver")

    # Read bronze Parquet files
    print_step("Loading bronze data…")
    df = read_parquet(spark, BRONZE_FILE)
    print(f"Bronze row count: {df.count()}")

    print_step("Running Silver transformations…")
    #TODO add cleaning, validation, deduplication steps here
    
    # Check duplicates
    duplicate_count = df.count() - df.dropDuplicates().count()
    print(f"Duplicate records found: {duplicate_count}")

    # Remove duplicates
    print("Removing duplicates…")
    df = df.dropDuplicates()
    print(f"Duplicates removed. Row count after deduplication: {df.count()}")

    #Check for NULLs
    null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
    print("Missing values per column:")
    null_counts.show()

    #Drop nulls
    print("Dropping records with missing values…")
    df = df.dropna()
    print(f"Records with missing values dropped. Row count after dropping nulls: {df.count()}")

    #Check for invalid amounts (negative or zero)
    print_step("Checking for invalid values…")

    invalid_amount = df.filter(F.col("Amount") < 0).count()
    invalid_time = df.filter(F.col("Time") < 0).count()
    invalid_class = df.filter(~F.col("Class").isin([0, 1])).count()

    print(f"Invalid Amount records: {invalid_amount}")
    print(f"Invalid Time records: {invalid_time}")
    print(f"Invalid Class records: {invalid_class}")

    #Filter out invalid records
    df = df.filter((F.col("Amount") >= 0) & (F.col("Time") >= 0) & (F.col("Class").isin([0, 1])))
    print(f"Invalid values removed. Records after removing invalid values: {df.count()}")

    #Cast correct types
    print("Casting columns to correct data types…")

    df = df.withColumn("Time", F.col("Time").cast("integer")) \
            .withColumn("Amount", F.col("Amount").cast("double")) \
            .withColumn("Class", F.col("Class").cast("integer"))
    for col_name in [f"V{i}" for i in range(1, 29)]:
        df = df.withColumn(col_name, F.col(col_name).cast("double"))

    print("Data types after casting:")
    df.printSchema()

    # Remove bronze metadata columns
    print_step("Removing Bronze metadata columns…")
    df = df.drop("ingestion_timestamp", "source_file")
    print("Bronze metadata removed.")

    # Add processing timestamp
    print("Adding processing timestamp…")
    df = df.withColumn("processing_timestamp", F.current_timestamp())
    print("Processing timestamp added.")

    # Write to silver layer
    print_step("Writing silver data…")
    write_parquet(df, SILVER_FILE)

    print_success("Silver data written successfully.")
    print(f"Silver data written to: {SILVER_FILE}")
    print(f"Silver row count: {df.count()}")
    df.printSchema()

if __name__ == "__main__":
    run()
