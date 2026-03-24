from utils import BRONZE_FILE, DQ_PATH, DUPLICATE_REPORT_FILE, INVALID_CLASS_REPORT_FILE, NULL_REPORT_FILE, OUTLIER_REPORT_FILE, RANGE_REPORT_FILE, SCHEMA_REPORT_FILE, create_spark_session, print_step, print_success, read_parquet, write_csv
from pyspark.sql import functions as F


def check_nulls(df, spark):
    '''Check for NULL values in the DataFrame'''
    null_exprs = [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]
    null_row = df.select(null_exprs).first()
    null_data = [(c, null_row[c]) for c in df.columns]
    return spark.createDataFrame(null_data, ["column_name", "null_count"])


def check_duplicates(df, spark):
    '''Check for duplicate records in the DataFrame'''
    total_records = df.count()
    distinct_records = df.dropDuplicates().count()
    duplicate_count = total_records - distinct_records

    dup_rows = df.groupBy(df.columns).count().filter("count > 1").drop("count")

    summary = spark.createDataFrame([
        ("total_rows", str(total_records)),
        ("distinct_rows", str(distinct_records)),
        ("duplicate_rows", str(duplicate_count))
    ], ["metric", "value"])

    return dup_rows, summary


def check_ranges(df, spark):
    '''Check for invalid Amount and Time values'''
    invalid_amount_count = df.filter(F.col("Amount") < 0).count()
    invalid_time_count = df.filter(F.col("Time") < 0).count()

    results = spark.createDataFrame([
        ("invalid_amount", str(invalid_amount_count)),
        ("invalid_time", str(invalid_time_count))
    ], ["issue", "count"])

    return results


def check_schema(df, spark):
    """Schema validation for types"""
    expected = {
        "Time": "IntegerType()",
        "Amount": "DoubleType()",
        "Class": "IntegerType()"
    }

    for i in range(1, 29):
        expected[f"V{i}"] = "DoubleType()"

    actual = {field.name: str(field.dataType) for field in df.schema.fields}
    
    schema_comparison = []
    
    for col, exp_type in expected.items():
        act_type = actual.get(col, "MISSING")
        status = "OK" if act_type == exp_type else "MISMATCH"
        schema_comparison.append((col, exp_type, act_type, status))

    return spark.createDataFrame(schema_comparison, ["column_name", "expected_type", "actual_type", "status"])


def check_invalid_class(df, spark):
    '''Check for invalid Class values'''
    invalid_class_count = df.filter(~F.col("Class").isin([0, 1])).count()
    return spark.createDataFrame([("invalid_class", str(invalid_class_count))], ["issue", "count"])


def check_outliers(df, spark):
    '''Check for outliers in the Amount column'''
    quantiles = df.approxQuantile("Amount", [0.01, 0.99], 0.01)
    p01, p99 = quantiles
    outlier_count = df.filter((F.col("Amount") < p01) | (F.col("Amount") > p99)).count()
    
    return spark.createDataFrame([("amount_outliers", str(outlier_count))], ["issue", "count"])


def run():

    spark = create_spark_session("DQ_Checks")
    df = read_parquet(spark, BRONZE_FILE)

    print("Running data quality checks on Bronze layer…")

    print_step("Checking for NULL values…")
    null_report = check_nulls(df, spark)
    write_csv(null_report, NULL_REPORT_FILE)
    print_success("Null value report saved.")
    
    print_step("Checking for duplicate records…")
    duplicate_rows, duplicate_summary = check_duplicates(df, spark)
    write_csv(duplicate_summary, DUPLICATE_REPORT_FILE)
    print_success("Duplicate records report saved.")
    
    print_step("Checking for invalid ranges in Amount and Time…")
    range_report = check_ranges(df, spark)
    write_csv(range_report, RANGE_REPORT_FILE)
    print_success("Invalid ranges report saved.")

    print_step("Checking schema validation…")
    schema_report = check_schema(df, spark)
    write_csv(schema_report, SCHEMA_REPORT_FILE)
    print_success("Schema validation report saved.")
    
    print_step("Checking for invalid Class values…")
    invalid_class_report = check_invalid_class(df, spark)
    write_csv(invalid_class_report, INVALID_CLASS_REPORT_FILE)
    print_success("Invalid Class values report saved.")

    print_step("Checking for outliers in Amount…")
    outlier_report = check_outliers(df, spark)
    write_csv(outlier_report, OUTLIER_REPORT_FILE)
    print_success("Outliers report saved.")

    print("\nData Quality Check Results:")

    print("\nNull Value Report:")
    null_report.show()

    print("\nDuplicate Records Summary:")
    duplicate_summary.show()

    print("\nInvalid Ranges Report:")
    range_report.show()

    print("\nSchema Validation Report:")
    schema_report.show()

    print("\nInvalid Class Values Report:")
    invalid_class_report.show()

    print("\nOutliers in Amount Report:")
    outlier_report.show()

    print_success(f"All DQ checks complete. Reports saved to: {DQ_PATH}")


if __name__ == "__main__":
    run()
