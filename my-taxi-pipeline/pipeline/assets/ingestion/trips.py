"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: vendor_id
    type: integer
    description: "TPEP/LPEP provider code"
  - name: pickup_datetime
    type: timestamp
    description: "Trip pickup date and time"
  - name: dropoff_datetime
    type: timestamp
    description: "Trip dropoff date and time"
  - name: passenger_count
    type: float
    description: "Number of passengers"
  - name: trip_distance
    type: float
    description: "Trip distance in miles"
  - name: pickup_location_id
    type: integer
    description: "TLC Taxi Zone pickup location"
  - name: dropoff_location_id
    type: integer
    description: "TLC Taxi Zone dropoff location"
  - name: fare_amount
    type: float
    description: "Meter fare amount"
  - name: total_amount
    type: float
    description: "Total charged to passenger"
  - name: payment_type
    type: integer
    description: "Payment type code"
  - name: taxi_type
    type: string
    description: "Taxi type (yellow or green)"
  - name: extracted_at
    type: timestamp
    description: "Timestamp of data extraction"

@bruin"""

import os
import json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta


def materialize():
    start_date = os.environ.get("BRUIN_START_DATE", "2022-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2022-02-01")
    bruin_vars = json.loads(os.environ.get("BRUIN_VARS", '{"taxi_types": ["yellow", "green"]}'))
    taxi_types = bruin_vars.get("taxi_types", ["yellow", "green"])

    base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/"

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    all_dfs = []
    current = start.replace(day=1)

    while current < end:
        year_month = current.strftime("%Y-%m")
        for taxi_type in taxi_types:
            filename = f"{taxi_type}_tripdata_{year_month}.csv.gz"
            url = base_url.format(taxi_type=taxi_type) + filename
            print(f"Fetching {url}...")
            try:
                df = pd.read_csv(url, compression="gzip")

                # Normalize column names
                col_mapping = {}
                if taxi_type == "yellow":
                    col_mapping = {
                        "VendorID": "vendor_id",
                        "tpep_pickup_datetime": "pickup_datetime",
                        "tpep_dropoff_datetime": "dropoff_datetime",
                        "passenger_count": "passenger_count",
                        "trip_distance": "trip_distance",
                        "PULocationID": "pickup_location_id",
                        "DOLocationID": "dropoff_location_id",
                        "fare_amount": "fare_amount",
                        "total_amount": "total_amount",
                        "payment_type": "payment_type",
                    }
                else:
                    col_mapping = {
                        "VendorID": "vendor_id",
                        "lpep_pickup_datetime": "pickup_datetime",
                        "lpep_dropoff_datetime": "dropoff_datetime",
                        "passenger_count": "passenger_count",
                        "trip_distance": "trip_distance",
                        "PULocationID": "pickup_location_id",
                        "DOLocationID": "dropoff_location_id",
                        "fare_amount": "fare_amount",
                        "total_amount": "total_amount",
                        "payment_type": "payment_type",
                    }

                df = df.rename(columns=col_mapping)
                keep_cols = list(col_mapping.values())
                df = df[[c for c in keep_cols if c in df.columns]]
                df["taxi_type"] = taxi_type
                df["extracted_at"] = datetime.now()

                all_dfs.append(df)
                print(f"  Fetched {len(df)} rows for {taxi_type} {year_month}")
            except Exception as e:
                print(f"  Error fetching {url}: {e}")

        current += relativedelta(months=1)

    if all_dfs:
        result = pd.concat(all_dfs, ignore_index=True)
        print(f"Total rows: {len(result)}")
        return result
    else:
        return pd.DataFrame()
