"""
AWS Glue ETL Job — Transforms raw JSON telemetry from S3 into optimized Parquet.

This script is designed to run as an AWS Glue ETL Job (Serverless Spark).
It reads from the raw/ prefix in S3, processes the data, and writes
partitioned Parquet files to the processed/ prefix.

Deployment:
  1. Upload this script to S3 (e.g., s3://sim-telemetry-lake/scripts/glue_etl_job.py)
  2. Create an AWS Glue Job in the Console → ETL → Python Shell or Spark
  3. Point the Job's script location to this S3 path
  4. Set the IAM role with S3 read/write permissions
  5. Run on-demand or schedule via a Glue Trigger / CloudWatch

Note: This is a batch job, not a streaming job. It processes all raw data
that has accumulated since the last run. For near-real-time processing,
you can schedule this to run every 15-30 minutes via a Glue Trigger.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    IntegerType, BooleanType,
)

# ── Glue Job Init ──
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_BUCKET = args["S3_BUCKET"]
RAW_PATH = f"s3://{S3_BUCKET}/raw/"
PROCESSED_PATH = f"s3://{S3_BUCKET}/processed/"

# ── Schema (matches kafka_publisher.py / aws_publisher.py output) ──
schema = StructType([
    StructField("timestamp_wall", StringType()),
    StructField("timestamp_game", FloatType()),
    StructField("session_id", StringType()),
    StructField("car", StringType()),
    StructField("track", StringType()),
    StructField("session_type", StringType()),
    StructField("status", StringType()),
    StructField("completed_laps", IntegerType()),
    StructField("current_lap_time_ms", IntegerType()),
    StructField("last_lap_time_ms", IntegerType()),
    StructField("best_lap_time_ms", IntegerType()),
    StructField("distance_into_lap", FloatType()),
    StructField("normalized_position", FloatType()),
    StructField("current_sector", IntegerType()),
    StructField("last_sector_time_ms", IntegerType()),
    StructField("is_valid_lap", BooleanType()),
    StructField("is_in_pit", BooleanType()),
    StructField("is_in_pit_lane", BooleanType()),
    StructField("throttle", FloatType()),
    StructField("brake", FloatType()),
    StructField("steering", FloatType()),
    StructField("clutch", FloatType()),
    StructField("speed_kmh", FloatType()),
    StructField("gear", IntegerType()),
    StructField("rpm", IntegerType()),
    StructField("fuel_remaining", FloatType()),
    StructField("fuel_per_lap", FloatType()),
    StructField("g_lat", FloatType()),
    StructField("g_lon", FloatType()),
    StructField("g_vert", FloatType()),
    StructField("tyre_pressure_fl", FloatType()),
    StructField("tyre_pressure_fr", FloatType()),
    StructField("tyre_pressure_rl", FloatType()),
    StructField("tyre_pressure_rr", FloatType()),
    StructField("tyre_temp_fl", FloatType()),
    StructField("tyre_temp_fr", FloatType()),
    StructField("tyre_temp_rl", FloatType()),
    StructField("tyre_temp_rr", FloatType()),
    StructField("tc_level", IntegerType()),
    StructField("tc_cut", IntegerType()),
    StructField("abs_level", IntegerType()),
    StructField("engine_map", IntegerType()),
    StructField("brake_bias", FloatType()),
    StructField("air_temp", FloatType()),
    StructField("road_temp", FloatType()),
    StructField("track_grip", StringType()),
    StructField("rain_intensity", StringType()),
    StructField("wind_speed", FloatType()),
    StructField("wind_direction", FloatType()),
])

# ── Read raw JSON from S3 ──
print(f"📖 Reading raw JSON from: {RAW_PATH}")
raw_df = spark.read.schema(schema).json(RAW_PATH)

print(f"   Total records: {raw_df.count()}")

# ── Transform ──
# 1. Parse timestamp_wall into a proper timestamp column
transformed = raw_df.withColumn(
    "timestamp_parsed",
    F.to_timestamp("timestamp_wall")
)

# 2. Derive average tyre temp per sample (useful for analytics)
transformed = transformed.withColumn(
    "avg_tyre_temp",
    (F.col("tyre_temp_fl") + F.col("tyre_temp_fr") +
     F.col("tyre_temp_rl") + F.col("tyre_temp_rr")) / 4.0
)

# 3. Derive average tyre pressure
transformed = transformed.withColumn(
    "avg_tyre_pressure",
    (F.col("tyre_pressure_fl") + F.col("tyre_pressure_fr") +
     F.col("tyre_pressure_rl") + F.col("tyre_pressure_rr")) / 4.0
)

# 4. Flag heavy braking zones (brake > 0.8)
transformed = transformed.withColumn(
    "is_heavy_braking",
    F.col("brake") > 0.8
)

# 5. Flag full throttle moments
transformed = transformed.withColumn(
    "is_full_throttle",
    F.col("throttle") > 0.95
)

# 6. Drop rows where status is not 'live' (replays, pauses)
transformed = transformed.filter(F.col("status") == "live")

# 7. Drop pit lane samples
transformed = transformed.filter(F.col("is_in_pit") == False)

print(f"   Records after filtering: {transformed.count()}")

# ── Write partitioned Parquet to processed/ ──
# Partition by session_id and completed_laps for efficient querying
print(f"📝 Writing optimized Parquet to: {PROCESSED_PATH}")
transformed.write \
    .mode("overwrite") \
    .partitionBy("session_id", "completed_laps") \
    .parquet(PROCESSED_PATH + "telemetry/")

# ── Create a lap summary aggregate table ──
print("📊 Creating lap summary aggregate...")
lap_summary = transformed.groupBy("session_id", "completed_laps", "car", "track") \
    .agg(
        F.max("speed_kmh").alias("max_speed_kmh"),
        F.avg("speed_kmh").alias("avg_speed_kmh"),
        F.avg("avg_tyre_temp").alias("avg_tyre_temp"),
        F.avg("avg_tyre_pressure").alias("avg_tyre_pressure"),
        F.avg("fuel_remaining").alias("avg_fuel"),
        F.min("fuel_remaining").alias("min_fuel"),
        F.sum(F.when(F.col("is_heavy_braking"), 1).otherwise(0)).alias("heavy_braking_count"),
        F.sum(F.when(F.col("is_full_throttle"), 1).otherwise(0)).alias("full_throttle_count"),
        F.count("*").alias("sample_count"),
        F.min("timestamp_parsed").alias("lap_start_time"),
        F.max("timestamp_parsed").alias("lap_end_time"),
    )

lap_summary.write \
    .mode("overwrite") \
    .parquet(PROCESSED_PATH + "lap_summary/")

print(f"✅ ETL complete. Lap summaries: {lap_summary.count()}")

# ── Commit Job ──
job.commit()
