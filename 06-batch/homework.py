"""
Module 6 Homework - Batch Processing with PySpark
Yellow Taxi Trip Data - November 2025
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

# Create a local Spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Module6Homework") \
    .getOrCreate()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ============================================================
# Question 1: Spark Version
# ============================================================
print("=" * 60)
print("Question 1: Spark Version")
print("=" * 60)
print(f"spark.version = {spark.version}")
print()

# ============================================================
# Question 2: Repartition and Parquet file sizes
# ============================================================
print("=" * 60)
print("Question 2: Average Parquet file size after repartition to 4")
print("=" * 60)

df = spark.read.parquet(os.path.join(BASE_DIR, "yellow_tripdata_2025-11.parquet"))

output_path = os.path.join(BASE_DIR, "yellow_tripdata_2025-11_partitioned")
df.repartition(4).write.mode("overwrite").parquet(output_path)

# Calculate average size of .parquet files
parquet_files = [
    f for f in os.listdir(output_path)
    if f.endswith(".parquet")
]
total_size = sum(
    os.path.getsize(os.path.join(output_path, f))
    for f in parquet_files
)
avg_size_mb = (total_size / len(parquet_files)) / (1024 * 1024)
print(f"Number of parquet files: {len(parquet_files)}")
print(f"Average parquet file size: {avg_size_mb:.1f} MB")
print()

# ============================================================
# Question 3: Count trips on November 15th
# ============================================================
print("=" * 60)
print("Question 3: Taxi trips started on November 15th")
print("=" * 60)

count_nov15 = df.filter(
    F.to_date("tpep_pickup_datetime") == "2025-11-15"
).count()
print(f"Trips on Nov 15: {count_nov15:,}")
print()

# ============================================================
# Question 4: Longest trip in hours
# ============================================================
print("=" * 60)
print("Question 4: Longest trip duration in hours")
print("=" * 60)

df_with_duration = df.withColumn(
    "trip_duration_hours",
    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 3600
)

longest_trip = df_with_duration.agg(
    F.max("trip_duration_hours").alias("max_hours")
).collect()[0]["max_hours"]

print(f"Longest trip: {longest_trip:.1f} hours")
print()

# ============================================================
# Question 5: Spark UI Port
# ============================================================
print("=" * 60)
print("Question 5: Spark UI Port")
print("=" * 60)
print("Spark UI runs on port 4040 by default")
print()

# ============================================================
# Question 6: Least frequent pickup location zone
# ============================================================
print("=" * 60)
print("Question 6: Least frequent pickup location zone")
print("=" * 60)

zones = spark.read.csv(
    os.path.join(BASE_DIR, "taxi_zone_lookup.csv"),
    header=True,
    inferSchema=True
)

zones.createOrReplaceTempView("zones")
df.createOrReplaceTempView("trips")

least_frequent = spark.sql("""
    SELECT z.Zone, COUNT(*) as trip_count
    FROM trips t
    JOIN zones z ON t.PULocationID = z.LocationID
    GROUP BY z.Zone
    ORDER BY trip_count ASC
    LIMIT 5
""")

print("Top 5 least frequent pickup zones:")
least_frequent.show(truncate=False)

# ============================================================
# Summary
# ============================================================
print("=" * 60)
print("SUMMARY OF ANSWERS")
print("=" * 60)
print(f"Q1: Spark version = {spark.version}")
print(f"Q2: Avg parquet file size = {avg_size_mb:.1f} MB")
print(f"Q3: Trips on Nov 15 = {count_nov15:,}")
print(f"Q4: Longest trip = {longest_trip:.1f} hours")
print(f"Q5: Spark UI port = 4040")
print("Q6: See least frequent zone above")

spark.stop()
