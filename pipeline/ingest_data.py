#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates_yellow = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

parse_dates_green = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime"
]


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2021, type=int, help='Year of the data')
@click.option('--month', default=1, type=int, help='Month of the data')
@click.option('--taxi-type', default='yellow', type=click.Choice(['yellow', 'green']), help='Taxi type (yellow or green)')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading data')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, taxi_type, target_table, chunksize):
    """Ingest NYC taxi data into PostgreSQL database."""

    # NYC TLC official data source (Parquet format)
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02d}.parquet'

    print(f"Downloading data from: {url}")

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    # Read parquet file
    print("Reading parquet file...")
    df = pd.read_parquet(url)

    # Convert datetime columns based on taxi type
    parse_dates = parse_dates_green if taxi_type == 'green' else parse_dates_yellow
    for col in parse_dates:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])

    print(f"Total records: {len(df)}")

    # Create table with schema
    print(f"Creating table {target_table}...")
    df.head(0).to_sql(
        name=target_table,
        con=engine,
        if_exists='replace',
        index=False
    )

    # Insert data in chunks
    total_chunks = (len(df) // chunksize) + 1
    print(f"Inserting data in {total_chunks} chunks...")

    for i in tqdm(range(0, len(df), chunksize)):
        chunk = df.iloc[i:i+chunksize]
        chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists='append',
            index=False
        )

    print(f"Done! Inserted {len(df)} records into {target_table}")


if __name__ == '__main__':
    run()
