"""
Create yellow_tripdata and green_tripdata tables in BigQuery
from parquet files stored in GCS (2019-2020).
Uses auto-detect schema with schema_update_options to handle
type conflicts (airport_fee INT32 vs DOUBLE, ehail_fee) between files.
Strategy: load each file as a separate temp table, then UNION ALL.
"""
from google.cloud import bigquery

CREDENTIALS_FILE = "gcs.json"
PROJECT_ID = "zoomcamp-487311"
DATASET_ID = "hw3"
BUCKET_NAME = "zoomcamp-487311-hw3"

client = bigquery.Client.from_service_account_json(CREDENTIALS_FILE, project=PROJECT_ID)


def create_table_via_query(table_name, taxi_type, years, months):
    """
    Create table by loading all parquet URIs via a SQL query
    that reads from the files directly and casts problematic columns.
    """
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    uris = []
    for year in years:
        for month in months:
            uri = f"gs://{BUCKET_NAME}/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            uris.append(uri)

    print(f"Creating {table_id} from {len(uris)} files...")

    # Use LOAD DATA statement which handles type coercion better
    uri_list = ", ".join([f"'{u}'" for u in uris])

    # Drop table first if exists
    client.query(f"DROP TABLE IF EXISTS `{table_id}`").result()

    # Use LOAD DATA INTO with allow_jagged_rows
    load_sql = f"""
    LOAD DATA OVERWRITE `{table_id}`
    FROM FILES (
        format = 'PARQUET',
        uris = [{uri_list}],
        enable_logical_types = true
    );
    """

    print("  Executing LOAD DATA statement...")
    try:
        job = client.query(load_sql)
        job.result()
        table = client.get_table(table_id)
        print(f"\nTable {table_id} created!")
        print(f"Total rows: {table.num_rows}")
        print(f"Total size: {table.num_bytes / (1024*1024):.2f} MB")
    except Exception as e:
        print(f"  LOAD DATA failed: {e}")
        print("  Falling back to column-by-column approach...")
        create_table_manual(table_name, taxi_type, years, months)


def create_table_manual(table_name, taxi_type, years, months):
    """Fallback: load each year separately and UNION ALL."""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    # For yellow taxi
    if taxi_type == "yellow":
        columns = """
            CAST(VendorID AS INT64) AS VendorID,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            CAST(passenger_count AS FLOAT64) AS passenger_count,
            CAST(trip_distance AS FLOAT64) AS trip_distance,
            CAST(RatecodeID AS FLOAT64) AS RatecodeID,
            store_and_fwd_flag,
            CAST(PULocationID AS INT64) AS PULocationID,
            CAST(DOLocationID AS INT64) AS DOLocationID,
            CAST(payment_type AS FLOAT64) AS payment_type,
            CAST(fare_amount AS FLOAT64) AS fare_amount,
            CAST(extra AS FLOAT64) AS extra,
            CAST(mta_tax AS FLOAT64) AS mta_tax,
            CAST(tip_amount AS FLOAT64) AS tip_amount,
            CAST(tolls_amount AS FLOAT64) AS tolls_amount,
            CAST(improvement_surcharge AS FLOAT64) AS improvement_surcharge,
            CAST(total_amount AS FLOAT64) AS total_amount,
            CAST(congestion_surcharge AS FLOAT64) AS congestion_surcharge,
            CAST(airport_fee AS FLOAT64) AS airport_fee
        """
    else:  # green taxi
        columns = """
            CAST(VendorID AS INT64) AS VendorID,
            lpep_pickup_datetime,
            lpep_dropoff_datetime,
            store_and_fwd_flag,
            CAST(RatecodeID AS FLOAT64) AS RatecodeID,
            CAST(PULocationID AS INT64) AS PULocationID,
            CAST(DOLocationID AS INT64) AS DOLocationID,
            CAST(passenger_count AS FLOAT64) AS passenger_count,
            CAST(trip_distance AS FLOAT64) AS trip_distance,
            CAST(fare_amount AS FLOAT64) AS fare_amount,
            CAST(extra AS FLOAT64) AS extra,
            CAST(mta_tax AS FLOAT64) AS mta_tax,
            CAST(tip_amount AS FLOAT64) AS tip_amount,
            CAST(tolls_amount AS FLOAT64) AS tolls_amount,
            CAST(ehail_fee AS FLOAT64) AS ehail_fee,
            CAST(improvement_surcharge AS FLOAT64) AS improvement_surcharge,
            CAST(total_amount AS FLOAT64) AS total_amount,
            CAST(payment_type AS FLOAT64) AS payment_type,
            CAST(trip_type AS FLOAT64) AS trip_type,
            CAST(congestion_surcharge AS FLOAT64) AS congestion_surcharge
        """

    # Create temp external tables per year and union them
    queries = []
    for year in years:
        for month in months:
            uri = f"gs://{BUCKET_NAME}/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            temp_ext = f"{PROJECT_ID}.{DATASET_ID}._temp_{taxi_type}_{year}_{month:02d}"

            # Create temp external table for this single file
            create_ext_sql = f"""
            CREATE OR REPLACE EXTERNAL TABLE `{temp_ext}`
            OPTIONS (format = 'PARQUET', uris = ['{uri}']);
            """
            print(f"  Creating temp table for {year}-{month:02d}...")
            client.query(create_ext_sql).result()

            queries.append(f"SELECT {columns} FROM `{temp_ext}`")

    # Now UNION ALL and create final table
    union_sql = f"""
    CREATE OR REPLACE TABLE `{table_id}` AS
    {' UNION ALL '.join(queries)};
    """

    print(f"  Creating final table with UNION ALL ({len(queries)} sources)...")
    job = client.query(union_sql)
    job.result()

    # Clean up temp tables
    for year in years:
        for month in months:
            temp_ext = f"{PROJECT_ID}.{DATASET_ID}._temp_{taxi_type}_{year}_{month:02d}"
            try:
                client.query(f"DROP TABLE IF EXISTS `{temp_ext}`").result()
            except Exception:
                pass

    table = client.get_table(table_id)
    print(f"\nTable {table_id} created!")
    print(f"Total rows: {table.num_rows}")
    print(f"Total size: {table.num_bytes / (1024*1024):.2f} MB")


if __name__ == "__main__":
    years = [2019, 2020]
    months = list(range(1, 13))

    print("=" * 60)
    print("Creating yellow_tripdata table...")
    print("=" * 60)
    create_table_via_query("yellow_tripdata", "yellow", years, months)

    print("\n" + "=" * 60)
    print("Creating green_tripdata table...")
    print("=" * 60)
    create_table_via_query("green_tripdata", "green", years, months)

    print("\nAll tables created successfully!")
