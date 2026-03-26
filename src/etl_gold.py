from utils import SILVER_FILE, GOLD_FRAUD_AMOUNT_BUCKETS, GOLD_FRAUD_HOURLY, GOLD_FRAUD_STATS, GOLD_AMOUNT_DIST, GOLD_AMOUNT_DIST_BY_CLASS, create_spark_session, print_step, print_success, read_parquet, write_parquet
from pyspark.sql import functions as F


def fraud_aggregations():
    return[
        F.count("*").alias("total_transactions"),
        F.sum("Class").alias("total_frauds"),
        (F.sum("Class") / F.count("*")).alias("fraud_rate"),

        F.sum("Amount").alias("total_amount"),
        F.sum(F.when(F.col("Class") == 1, F.col("Amount"))).alias("fraud_amount"),
        F.sum(F.when(F.col("Class") == 0, F.col("Amount"))).alias("nonfraud_amount"),

        F.avg("Amount").alias("avg_amount"),
        F.avg(F.when(F.col("Class") == 1, F.col("Amount"))).alias("avg_fraud_amount"),
        F.avg(F.when(F.col("Class") == 0, F.col("Amount"))).alias("avg_nonfraud_amount")
    ]


def amount_statistics():
    return [
        F.min("Amount").alias("min_amount"),
        F.max("Amount").alias("max_amount"),
        F.avg("Amount").alias("avg_amount"),
        F.stddev("Amount").alias("std_amount"),
        F.expr("percentile_approx(Amount, 0.5)").alias("median_amount"),
        F.expr("percentile_approx(Amount, 0.25)").alias("p25_amount"),
        F.expr("percentile_approx(Amount, 0.75)").alias("p75_amount"),
        F.expr("percentile_approx(Amount, 0.95)").alias("p95_amount")
    ]


def add_hour_column(df):
    return df.withColumn("hour", (F.col("Time") / 3600).cast("int"))


def add_amount_bucket_column(df):
    return df.withColumn(
        "amount_bucket",
        F.when(F.col("Amount") < 10, "0-10")
        .when((F.col("Amount") >= 10) & (F.col("Amount") < 50), "10-50")
        .when((F.col("Amount") >= 50) & (F.col("Amount") < 100), "50-100")
        .when((F.col("Amount") >= 100) & (F.col("Amount") < 500), "100-500")
        .when((F.col("Amount") >= 500) & (F.col("Amount") < 1000), "500-1000")
        .otherwise("1000+")
    )

def run():

    spark = create_spark_session("ETL_Gold")

    # Read silver data
    print_step("Loading silver data…")
    df = read_parquet(spark, SILVER_FILE)
    print(f"Silver row count: {df.count()}")

    print_step("Running Gold transformations…")

    # Fraud Statistics
    print_step("Calculating fraud statistics…")

    fraud_stats = df.agg(*fraud_aggregations())
    fraud_stats = fraud_stats.withColumn("Class", F.lit("ALL")).select("Class", *fraud_stats.columns)

    fraud_stats.show()

    write_parquet(fraud_stats, GOLD_FRAUD_STATS)
    print_success("Fraud statistics saved.")

    # Hourly Fraud Analysis
    print_step("Calculating hourly fraud analysis…")

    df = add_hour_column(df)

    hourly_fraud = (
        df.groupBy("hour")
        .agg(*fraud_aggregations())
        .orderBy("hour")
    )

    hourly_fraud.show(24)

    write_parquet(hourly_fraud, GOLD_FRAUD_HOURLY)
    print_success("Hourly fraud analysis saved.")

    # Amount buckets fraud analysis
    print_step("Calculating amount buckets fraud analysis…")

    print_step("Creating amount buckets…")

    df = add_amount_bucket_column(df)

    print_step("Calculating fraud rate by amount buckets…")

    amount_buckets = (
        df.groupBy("amount_bucket")
        .agg(*fraud_aggregations())
        .orderBy("amount_bucket")
    )

    amount_buckets.show()

    write_parquet(amount_buckets, GOLD_FRAUD_AMOUNT_BUCKETS)
    print_success("Amount buckets fraud analysis saved.")

    # Amount distribution statistics
    print_step("Calculating amount distribution statistics…")
    amount_stats = df.agg(*amount_statistics())
    amount_stats = amount_stats.withColumn("Class", F.lit("ALL")) \
                                .select("Class", *amount_stats.columns)

    amount_stats.show()

    write_parquet(amount_stats, GOLD_AMOUNT_DIST)
    print_success("Amount distribution statistics saved.")

    # Amount distribution by class
    print_step("Calculating amount distribution by class…")
    amount_dist = (
        df.groupBy("Class")
        .agg(*amount_statistics())
        .orderBy("Class")
    )

    amount_dist.show()

    write_parquet(amount_dist, GOLD_AMOUNT_DIST_BY_CLASS)
    print_success("Amount distribution by class saved.")

    print_success("All Gold tables created.")

if __name__ == "__main__":
    run()