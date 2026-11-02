# STEDI Human Balance Analytics | AWS Data Lakehouse

## Project Overview

This project implements an end-to-end data lakehouse pipeline using **Amazon S3, AWS Glue, Amazon Athena, PySpark, and SQL** as part of the Udacity Data Engineering program.

The objective is to process customer, accelerometer, and step trainer data from STEDI, a fictional fitness technology company, and prepare a curated dataset for machine learning.

The pipeline applies customer research-consent requirements, integrates multiple data sources, and transforms raw JSON files into analytics-ready Parquet datasets.

**Final outcome:** Successfully processed three source datasets through five AWS Glue ETL jobs, producing a machine learning curated dataset containing **43,681 records**.

## Technology Stack

| Technology | Purpose |
|---|---|
| Amazon S3 | Data lake storage for raw, trusted, and curated datasets |
| AWS Glue | Serverless ETL processing and orchestration |
| AWS Glue Data Catalog | Centralized metadata and table definitions |
| Apache Spark / PySpark | Distributed data transformation |
| Amazon Athena | Serverless SQL querying and validation |
| SQL | Filtering, joining, and data quality checks |
| AWS IAM | Secure access control for Glue and S3 |
| Parquet | Columnar storage format for transformed datasets |

## Data Architecture

The solution follows a three-layer data lakehouse architecture:

### 1. Landing Layer

Stores raw JSON files ingested into Amazon S3.

- `customer_landing`: Customer profiles, registration information, and data-sharing preferences.
- `accelerometer_landing`: Accelerometer sensor readings associated with customer accounts.
- `step_trainer_landing`: Step trainer measurements collected from fitness devices.

### 2. Trusted Layer

Applies consent-based filtering and establishes relationships between datasets.

- `customer_trusted`: Customers who consented to share their information for research.
- `accelerometer_trusted`: Accelerometer readings associated with consenting customers.
- `step_trainer_trusted`: Step trainer readings associated with curated customer device serial numbers.

### 3. Curated Layer

Prepares integrated datasets for downstream analytics and machine learning.

- `customers_curated`: Consenting customers with accelerometer activity.
- `machine_learning_curated`: Integrated step trainer and accelerometer measurements aligned by sensor reading timestamps.

### Pipeline Flow

```text
                     RAW DATA (JSON)
                            |
                     Amazon S3 Landing
                            |
           +----------------+----------------+
           |                |                |
       Customer        Accelerometer     Step Trainer
        Landing           Landing          Landing
           |                |                |
           v                |                |
    Customer Trusted        |                |
           |                |                |
           +-------> Accelerometer Trusted   |
           |                |                |
           +----------------+                |
           |                                 |
           v                                 |
    Customers Curated                        |
           |                                 |
           +----------> Step Trainer Trusted |
                              ^              |
                              +--------------+
                              |
                              v
                     Machine Learning
                         Curated
                              ^
                              |
                     Accelerometer
                         Trusted
```

All trusted and curated outputs are stored in Amazon S3 using Parquet format and registered in the AWS Glue Data Catalog.

## ETL Pipeline Implementation

Five AWS Glue ETL jobs were developed using Glue Studio, SQL transformations, and PySpark.

### Job 1: Customer Landing to Trusted

**Job:** `customer_landing_to_trusted`

Filters customer records to retain only individuals who provided research consent.

```sql
SELECT *
FROM customers
WHERE sharewithresearchasofdate IS NOT NULL
  AND sharewithresearchasofdate > 0;
```

**Result:** 482 consenting customers.

### Job 2: Accelerometer Landing to Trusted

**Job:** `accelerometer_landing_to_trusted`

Joins raw accelerometer readings with the trusted customer dataset using customer email addresses.

This ensures that accelerometer measurements are retained only for customers who provided research consent.

**Result:** 40,981 accelerometer readings.

### Job 3: Customer Trusted to Curated

**Job:** `customer_trusted_to_curated`

Identifies consenting customers with matching accelerometer activity.

Uses a semi-join to prevent repeated accelerometer measurements from duplicating customer records.

**Result:** 482 curated customers.

### Job 4: Step Trainer Landing to Trusted

**Job:** `step_trainer_landing_to_trusted`

Filters step trainer records using serial numbers associated with curated customers.

The transformation uses customer-device relationships to determine which step trainer readings qualify for downstream processing.

**Result:** 14,460 step trainer readings.

### Job 5: Machine Learning Curated

**Job:** `step_trainer_to_machine_learning_curated`

Integrates trusted step trainer measurements with trusted accelerometer measurements using their respective timestamp fields.

The transformation joins:

- Step trainer: `sensorReadingTime`
- Accelerometer: `timestamp`

The output combines sensor readings and customer-associated accelerometer information into a dataset suitable for further machine learning preparation.

**Result:** 43,681 records.

## Data Validation Results

All eight datasets were validated using Amazon Athena.

| Dataset | Record Count | Layer |
|---|---:|---|
| `customer_landing` | 956 | Landing |
| `accelerometer_landing` | 81,273 | Landing |
| `step_trainer_landing` | 28,680 | Landing |
| `customer_trusted` | 482 | Trusted |
| `accelerometer_trusted` | 40,981 | Trusted |
| `customers_curated` | 482 | Curated |
| `step_trainer_trusted` | 14,460 | Trusted |
| `machine_learning_curated` | 43,681 | Curated |

The resulting record counts were verified through SQL queries executed in Athena.

Example validation query:

```sql
SELECT COUNT(*) AS total_records
FROM stedi.machine_learning_curated;
```

**Verified result: 43,681 records.**

## Data Quality and Privacy Considerations

The pipeline incorporates several data quality and privacy principles:

**Research consent filtering:** Customer data is filtered based on explicit research-sharing preferences before it is included in trusted datasets.

**Data integrity:** Relationships between customer, accelerometer, and step trainer datasets are established using the available email, serial number, and timestamp fields.

**Duplicate prevention:** Semi-joins are used where appropriate to avoid multiplying customer records when multiple sensor readings exist.

**Schema validation:** AWS Glue Data Catalog schemas and Amazon Athena queries are used to validate table structures and data accessibility.

**Storage separation:** Raw JSON files remain in the landing layer, while transformed Parquet datasets are written to separate trusted and curated S3 locations.

**Data modeling limitation:** Device serial numbers may be reused across records in the sample dataset, and timestamps may not uniquely identify sensor measurements. These relationships should be evaluated further before using the output for production machine learning.

## Repository Structure

```text
stedi-human-balance-analytics/
|
|-- glue_jobs/
|   |-- customer_landing_to_trusted.py
|   |-- accelerometer_landing_to_trusted.py
|   |-- customer_trusted_to_curated.py
|   |-- step_trainer_landing_to_trusted.py
|   |-- step_trainer_to_machine_learning_curated.py
|
|-- sql/
|   |-- customer_landing.sql
|   |-- accelerometer_landing.sql
|   |-- step_trainer_landing.sql
|
|-- screenshots/
|   |-- Athena query validation screenshots
|
|-- README.md
```

## How to Reproduce

1. Create an Amazon S3 bucket with separate landing, trusted, and curated prefixes.
2. Upload the STEDI JSON source datasets to their corresponding landing locations.
3. Create the `stedi` database in the AWS Glue Data Catalog.
4. Execute the SQL scripts in the `sql/` directory using Amazon Athena to register the landing tables.
5. Configure an IAM role with the permissions required for AWS Glue to access the S3 datasets and Data Catalog.
6. Create the five AWS Glue ETL jobs using the scripts provided in `glue_jobs/`.
7. Update the S3 paths, table configurations, and IAM role references to match the target AWS environment.
8. Execute the ETL jobs in dependency order.
9. Validate the resulting datasets using Athena.

**Note:** The SQL and Glue scripts may contain environment-specific S3 locations and configuration values that must be updated before reuse.

## Key Learning Outcomes

This project provided practical experience in:

- Designing and implementing a layered data lakehouse architecture on AWS.
- Ingesting and querying semi-structured JSON datasets.
- Building serverless ETL workflows with AWS Glue and Apache Spark.
- Applying SQL joins, filtering, and customer-consent logic.
- Transforming raw data into optimized Parquet datasets.
- Managing data schemas through the AWS Glue Data Catalog.
- Troubleshooting S3 permissions, table locations, and schema-mapping issues.
- Validating pipeline outputs through Amazon Athena.
- Preparing integrated sensor datasets for downstream machine learning.

## Future Enhancements

Potential improvements for a production implementation include:

- Automated data quality tests and schema validation.
- AWS Glue workflow orchestration and scheduling.
- Pipeline monitoring and alerting using Amazon CloudWatch.
- Incremental processing and partitioned Parquet storage.
- More robust device identity and timestamp matching.
- Automated deployment using infrastructure as code.
- Stronger data governance and access controls.

## Project Context

Developed as part of the **Udacity Data Engineering program — STEDI Human Balance Analytics project**.

The project uses the STEDI educational dataset and demonstrates an AWS-based approach to building trusted and curated analytical datasets.

**Reference:** [Udacity STEDI Project](https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/blob/main/project/README.md)

---

*AWS Data Engineering | Data Lakehouse Architecture | ETL | PySpark | SQL | Analytics*
