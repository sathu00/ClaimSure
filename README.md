# ClaimSure – Insurance Claims Data Engineering Pipeline

An end-to-end insurance claims data engineering project built using Python, PySpark and SQL-oriented data processing concepts. The pipeline transforms raw synthetic insurance data into validated, analytics-ready datasets through Bronze, Silver and Gold layers.

## Project Overview

ClaimSure simulates an insurance claims data platform that processes claims, policies, claimants and payment records.

The pipeline performs data ingestion, cleansing, standardization, deduplication, validation, quarantine handling and reconciliation to produce reliable datasets for downstream analytics.



## Architecture

```text
Synthetic CSV Data
       |
       v
  Bronze Layer
  (Raw CSV Files)
       |
       v
  PySpark ETL Pipeline
       |
       +--------------------+
       |                    |
       v                    v
  Silver Layer         Quarantine Layer
  (Clean Parquet)      (Rejected Records)
       |
       v
   Gold Layer
   (Analytics-ready datasets)
       |
       v
  Audit & Reconciliation
```

**Planned AWS architecture:** Amazon S3 → AWS Glue (PySpark) → Amazon Athena.

## Technology Stack

- Python
- PySpark
- SQL
- Pandas
- Apache Spark
- AWS S3, AWS Glue and Amazon Athena (planned integration)
- Parquet
- Git and GitHub

## Dataset

The project uses four synthetic datasets:

| Dataset | Description |
|---|---|
| Claims | Insurance claim transactions |
| Policies | Policy master records |
| Claimants | Claimant information |
| Payments | Claim payment transactions |

All records are synthetic and contain deliberately introduced data-quality issues.

## Data Engineering Workflow

### 1. Bronze Layer – Data Ingestion
- Ingests raw CSV datasets.
- Preserves source records without modifying the original files.
- Organizes data by ingestion date.

### 2. Silver Layer – Data Transformation
- Cleans and standardizes data.
- Handles missing and invalid values.
- Identifies and removes duplicate claim records.
- Validates dates, amounts, statuses and relationships.
- Stores validated datasets in Parquet format.

### 3. Quarantine Layer – Data Quality
- Separates rejected records from valid records.
- Captures validation failure reasons.
- Handles invalid claim and payment records.

### 4. Gold Layer – Analytics
Generates six analytics-ready datasets:

- `claims_enriched`
- `claims_by_status`
- `claims_by_type`
- `monthly_claims`
- `monthly_payments`
- `claims_by_city`

### 5. Audit and Reconciliation
- Validates Bronze-to-Silver and Quarantine record counts.
- Checks duplicate keys and referential integrity.
- Performs aggregate-level reconciliation.
- Generates data-quality summaries.

## Local Execution Results

The pipeline was successfully executed locally using PySpark.

| Dataset | Bronze | Silver | Quarantine |
|---|---:|---:|---:|
| Claims | 1,212 | 1,153 | 59 |
| Policies | 850 | 850 | 0 |
| Claimants | 1,050 | 1,050 | 0 |
| Payments | 749 | 715 | 34 |

**Validation results:**
- 28 reconciliation checks passed.
- 0 reconciliation failures.
- 6 Gold datasets generated.
- Silver, Quarantine, Gold and Audit outputs written in Parquet format.

## Project Structure

```text
ClaimSure/
│
├── claimsure/
│   ├── __init__.py
│   ├── config.py
│   ├── rules.py
│   ├── gold.py
│   ├── io_utils.py
│   └── reconciliation.py
│
├── glue/
│   └── claimsure_etl_job.py
│
├── data/
│   ├── claims.csv
│   ├── policies.csv
│   ├── claimants.csv
│   └── payments.csv
│
├── .gitignore
└── README.md
```

## Run Locally

### Prerequisites
- Python 3.12
- Java 17
- PySpark 3.5.6

### Install dependencies

```bash
python3 -m pip install pyspark==3.5.6 pandas
```

### Execute the ETL pipeline

Run from the project root:

```bash
python3 glue/claimsure_etl_job.py \
  --base_path ./data \
  --ingest_date 2026-11-24
```

The pipeline generates Bronze, Silver, Quarantine, Gold and Audit outputs under the `data/` directory.

## Future Enhancements

- Integrate Amazon S3 for cloud-based data storage.
- Deploy the PySpark ETL job using AWS Glue.
- Create AWS Glue Data Catalog tables.
- Query Gold datasets using Amazon Athena.
- Add automated pipeline testing and monitoring.

## Disclaimer

This project uses entirely synthetic insurance data for educational and portfolio demonstration purposes. It does not contain real customer or policyholder information.