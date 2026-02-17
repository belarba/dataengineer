/* @bruin
name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_date
  time_granularity: date

columns:
  - name: pickup_date
    type: date
    description: "Trip date"
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: "Taxi type (yellow or green)"
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Payment type description"
    primary_key: true
  - name: total_trips
    type: bigint
    description: "Total number of trips"
    checks:
      - name: non_negative
  - name: total_revenue
    type: float
    description: "Total revenue from fares"
    checks:
      - name: non_negative
  - name: avg_trip_distance
    type: float
    description: "Average trip distance in miles"
  - name: avg_fare_amount
    type: float
    description: "Average fare per trip"

@bruin */

SELECT
    CAST(pickup_datetime AS DATE) AS pickup_date,
    taxi_type,
    payment_type_name,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(trip_distance) AS avg_trip_distance,
    AVG(fare_amount) AS avg_fare_amount
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
    CAST(pickup_datetime AS DATE),
    taxi_type,
    payment_type_name
