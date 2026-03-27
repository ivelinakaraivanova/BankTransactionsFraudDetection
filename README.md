# Bank Transactions Fraud Detection — PySpark ETL Pipeline

End-to-end ETL pipeline built with **PySpark** on the [Kaggle Credit Card Fraud Detection](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud) dataset (284,807 real anonymized transactions).

## Architecture

Medallion architecture: **Bronze → Silver → Gold** with automated Data Quality checks.

```
data/
├── raw/            # Source CSV from Kaggle
├── bronze/         # Raw Parquet (as-is ingestion)
├── silver/         # Cleaned & validated Parquet
├── gold/           # Aggregated analytics Parquet
└── dq_reports/     # Data quality report CSVs

src/
├── utils.py        # Shared config, Spark session, I/O helpers
├── etl_bronze.py   # Raw CSV → Bronze Parquet
├── etl_silver.py   # Dedup, null/type cleanup, outlier removal → Silver Parquet
├── etl_gold.py     # Fraud stats, hourly analysis, amount buckets
└── dq_checks.py    # Automated DQ checks (nulls, dupes, schema, ranges, outliers)
```

## Pipeline Layers

| Layer | Input | Output | Key Operations |
|-------|-------|--------|----------------|
| **Bronze** | `creditcard.csv` | `transactions.parquet` | Add ingestion metadata, no transformations |
| **Silver** | Bronze Parquet | `transactions.parquet` | Remove nulls, duplicates & outliers; cast types; add processing timestamp |
| **Gold** | Silver Parquet | 5 analytical Parquets | Fraud stats, hourly fraud rates, amount buckets, amount distribution |
| **DQ** | Silver Parquet | 6 CSV reports | Null counts, duplicates, schema validation, range checks, invalid classes, outliers |

## Setup

**Requirements:** Docker (recommended) or local PySpark 4.x + JDK 17/21

1. Clone the repo and place `creditcard.csv` in `data/raw/`
2. Run via Docker:
   ```bash
   docker run -v "$(pwd):/home/jovyan/work" jupyter/pyspark-notebook:latest \
     bash -c "cd /home/jovyan/work/src && python etl_bronze.py && python etl_silver.py && python dq_checks.py && python etl_gold.py"
   ```

## Dataset

- **Source:** [Kaggle Credit Card Fraud Detection](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud)
- **Rows:** 284,807 transactions (492 frauds, 0.17% fraud rate)
- **Features:** Time, V1–V28 (PCA), Amount, Class (0 = legit, 1 = fraud)

## Tech Stack

- PySpark 4.x
- Docker (jupyter/pyspark-notebook)
- Parquet / CSV