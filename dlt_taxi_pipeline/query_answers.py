import duckdb

conn = duckdb.connect("taxi_pipeline.duckdb", read_only=True)

# Question 1: Start and end date of the dataset
print("=== Question 1: Date range ===")
result = conn.sql("""
    SELECT
        MIN(trip_pickup_date_time)::DATE AS start_date,
        MAX(trip_pickup_date_time)::DATE AS end_date
    FROM ny_taxi_data.rides
""").fetchone()
print(f"Start date: {result[0]}")
print(f"End date: {result[1]}")

# Question 2: Proportion of trips paid with credit card
print("\n=== Question 2: Credit card proportion ===")
result = conn.sql("""
    SELECT
        ROUND(
            COUNT(CASE WHEN payment_type = 'Credit' THEN 1 END) * 100.0 / COUNT(*),
            2
        ) AS credit_pct
    FROM ny_taxi_data.rides
""").fetchone()
print(f"Credit card proportion: {result[0]}%")

# Question 3: Total tips
print("\n=== Question 3: Total tips ===")
result = conn.sql("""
    SELECT ROUND(SUM(tip_amt), 2) AS total_tips
    FROM ny_taxi_data.rides
""").fetchone()
print(f"Total tips: ${result[0]}")

# Extra info
print("\n=== Extra: Row count and payment types ===")
count = conn.sql("SELECT COUNT(*) FROM ny_taxi_data.rides").fetchone()
print(f"Total rows: {count[0]}")

payments = conn.sql("""
    SELECT payment_type, COUNT(*) as cnt
    FROM ny_taxi_data.rides
    GROUP BY payment_type
""").fetchall()
print(f"Payment types: {payments}")

conn.close()
