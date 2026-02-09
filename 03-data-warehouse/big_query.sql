-- =============================================================================
-- Module 3 Homework: Data Warehousing & BigQuery
-- Yellow Taxi Trip Records - January 2024 to June 2024
-- =============================================================================

-- =============================================================================
-- SETUP: Create External Table and Materialized Table
-- =============================================================================

-- Create an external table referencing the parquet files in GCS
CREATE OR REPLACE EXTERNAL TABLE `<project_id>.<dataset>.yellow_taxi_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dezoomcamp_hw3_2025/yellow_tripdata_2024-*.parquet']
);

-- Create a regular (materialized) table from the external table (no partition, no cluster)
CREATE OR REPLACE TABLE `<project_id>.<dataset>.yellow_taxi_materialized` AS
SELECT * FROM `<project_id>.<dataset>.yellow_taxi_external`;


-- =============================================================================
-- Question 1: Counting records
-- What is the count of records for the 2024 Yellow Taxi Data?
-- =============================================================================

SELECT COUNT(*) AS total_records
FROM `<project_id>.<dataset>.yellow_taxi_materialized`;

-- Answer: 20,332,093


-- =============================================================================
-- Question 2: Data read estimation
-- Count distinct PULocationIDs on both tables.
-- What is the estimated data read?
-- =============================================================================

-- On external table:
SELECT COUNT(DISTINCT PULocationID)
FROM `<project_id>.<dataset>.yellow_taxi_external`;

-- On materialized table:
SELECT COUNT(DISTINCT PULocationID)
FROM `<project_id>.<dataset>.yellow_taxi_materialized`;

-- Answer: 0 MB for the External Table and 155.12 MB for the Materialized Table
-- (External tables do not provide estimated bytes in the query plan)


-- =============================================================================
-- Question 3: Understanding columnar storage
-- Retrieve PULocationID vs PULocationID + DOLocationID from the materialized table.
-- Why are the estimated bytes different?
-- =============================================================================

-- Query 1: Single column
SELECT PULocationID
FROM `<project_id>.<dataset>.yellow_taxi_materialized`;

-- Query 2: Two columns
SELECT PULocationID, DOLocationID
FROM `<project_id>.<dataset>.yellow_taxi_materialized`;

-- Answer: BigQuery is a columnar database, and it only scans the specific columns
-- requested in the query. Querying two columns (PULocationID, DOLocationID) requires
-- reading more data than querying one column (PULocationID), leading to a higher
-- estimated number of bytes processed.


-- =============================================================================
-- Question 4: Counting zero fare trips
-- How many records have a fare_amount of 0?
-- =============================================================================

SELECT COUNT(*) AS zero_fare_count
FROM `<project_id>.<dataset>.yellow_taxi_materialized`
WHERE fare_amount = 0;

-- Answer: 8,333


-- =============================================================================
-- Question 5: Partitioning and clustering
-- Best strategy for filtering on tpep_dropoff_datetime and ordering by VendorID
-- =============================================================================

-- Create optimized table: partition by tpep_dropoff_datetime, cluster by VendorID
CREATE OR REPLACE TABLE `<project_id>.<dataset>.yellow_taxi_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `<project_id>.<dataset>.yellow_taxi_materialized`;

-- Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID


-- =============================================================================
-- Question 6: Partition benefits
-- Distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15
-- Compare estimated bytes for non-partitioned vs partitioned table
-- =============================================================================

-- On the non-partitioned (materialized) table:
SELECT DISTINCT VendorID
FROM `<project_id>.<dataset>.yellow_taxi_materialized`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

-- On the partitioned/clustered table:
SELECT DISTINCT VendorID
FROM `<project_id>.<dataset>.yellow_taxi_partitioned_clustered`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

-- Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table


-- =============================================================================
-- Question 7: External table storage
-- Where is the data stored in the External Table?
-- =============================================================================

-- Answer: GCP Bucket
-- External tables do not store data in BigQuery; they reference files in GCS.


-- =============================================================================
-- Question 8: Clustering best practices
-- Is it always best practice to cluster your data?
-- =============================================================================

-- Answer: False
-- Clustering is not always beneficial. For small tables, the overhead of clustering
-- may not provide any performance improvement. Clustering is most effective on
-- large tables (typically > 1 GB) where queries frequently filter or aggregate
-- on the clustered columns.


-- =============================================================================
-- Question 9: Understanding table scans
-- SELECT count(*) FROM the materialized table. How many bytes estimated? Why?
-- =============================================================================

SELECT COUNT(*)
FROM `<project_id>.<dataset>.yellow_taxi_materialized`;

-- Answer: 0 bytes.
-- BigQuery stores metadata about tables, including the row count. A COUNT(*)
-- query does not need to scan any actual data because BigQuery can return the
-- result directly from the table's metadata.
