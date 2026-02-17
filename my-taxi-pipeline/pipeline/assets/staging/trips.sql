/* @bruin
name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    description: "Trip pickup date and time"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "Trip dropoff date and time"
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: pickup_location_id
    type: integer
    description: "TLC Taxi Zone pickup location"
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_location_id
    type: integer
    description: "TLC Taxi Zone dropoff location"
    primary_key: true
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    description: "Meter fare amount"
    primary_key: true
    checks:
      - name: non_negative
  - name: payment_type_name
    type: string
    description: "Payment type description"
    checks:
      - name: not_null
  - name: total_amount
    type: float
    description: "Total charged to passenger"

custom_checks:
  - name: no_duplicate_trips
    description: "Ensure no duplicate trips after deduplication"
    query: |
      SELECT COUNT(*) - COUNT(DISTINCT pickup_datetime || dropoff_datetime || pickup_location_id || dropoff_location_id || fare_amount)
      FROM staging.trips
    value: 0

@bruin */

WITH deduplicated AS (
    SELECT
        t.*,
        ROW_NUMBER() OVER (
            PARTITION BY t.pickup_datetime, t.dropoff_datetime,
                         t.pickup_location_id, t.dropoff_location_id, t.fare_amount
            ORDER BY t.extracted_at DESC
        ) AS rn
    FROM ingestion.trips t
    WHERE t.pickup_datetime >= '{{ start_datetime }}'
      AND t.pickup_datetime < '{{ end_datetime }}'
      AND t.pickup_datetime IS NOT NULL
      AND t.fare_amount >= 0
)
SELECT
    d.vendor_id,
    d.pickup_datetime,
    d.dropoff_datetime,
    d.passenger_count,
    d.trip_distance,
    d.pickup_location_id,
    d.dropoff_location_id,
    d.fare_amount,
    d.total_amount,
    d.payment_type,
    COALESCE(p.payment_type_name, 'unknown') AS payment_type_name,
    d.taxi_type,
    d.extracted_at
FROM deduplicated d
LEFT JOIN ingestion.payment_lookup p
    ON CAST(d.payment_type AS INTEGER) = p.payment_type_id
WHERE d.rn = 1
