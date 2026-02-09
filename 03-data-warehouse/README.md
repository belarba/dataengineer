# Module 3 Homework: Data Warehousing & BigQuery

## Data

Yellow Taxi Trip Records for January 2024 - June 2024 from NYC TLC.

## Setup

### 1. Load data to GCS

Update `BUCKET_NAME` and `CREDENTIALS_FILE` in `load_data.py`, then run:

```bash
pip install google-cloud-storage
python load_data.py
```

### 2. Create tables in BigQuery

Run the setup queries from `big_query.sql` (replace `<project_id>` and `<dataset>` with your values):

```sql
-- External table
CREATE OR REPLACE EXTERNAL TABLE `<project_id>.<dataset>.yellow_taxi_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dezoomcamp_hw3_2025/yellow_tripdata_2024-*.parquet']
);

-- Materialized table (no partition, no cluster)
CREATE OR REPLACE TABLE `<project_id>.<dataset>.yellow_taxi_materialized` AS
SELECT * FROM `<project_id>.<dataset>.yellow_taxi_external`;
```

## Answers

### Question 1: Counting records
**20,332,093**

```sql
SELECT COUNT(*) FROM yellow_taxi_materialized;
```

### Question 2: Data read estimation
**0 MB for the External Table and 155.12 MB for the Materialized Table**

BigQuery cannot estimate bytes for external tables (shows 0 MB), while the materialized table shows the actual column size to be scanned.

### Question 3: Understanding columnar storage
**BigQuery is a columnar database, and it only scans the specific columns requested in the query.**

Querying two columns requires reading more data than one column.

### Question 4: Counting zero fare trips
**8,333**

```sql
SELECT COUNT(*) FROM yellow_taxi_materialized WHERE fare_amount = 0;
```

### Question 5: Partitioning and clustering
**Partition by tpep_dropoff_datetime and Cluster on VendorID**

Partitioning on the filter column and clustering on the order column is the optimal strategy.

```sql
CREATE OR REPLACE TABLE yellow_taxi_partitioned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM yellow_taxi_materialized;
```

### Question 6: Partition benefits
**310.24 MB for non-partitioned table and 26.84 MB for the partitioned table**

Partitioning allows BigQuery to skip scanning partitions outside the date range filter.

### Question 7: External table storage
**GCP Bucket**

External tables reference data stored in Google Cloud Storage, not inside BigQuery.

### Question 8: Clustering best practices
**False**

Clustering is not always beneficial. For small tables, the overhead is not worth it. It's most effective on large tables with frequent filtered queries on the clustered columns.

### Question 9: Understanding table scans
**0 bytes**

`COUNT(*)` reads 0 bytes because BigQuery stores row count in table metadata and doesn't need to scan any data to return the result.
