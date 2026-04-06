from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, IntegerType, BooleanType

spark = SparkSession.builder \
    .appName("Simmetry") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

kafka_brokers = "localhost:9092"
kafka_topic = "telemetry_raw"

# Lazy evaluation: blueprint for connecting to Kafka
raw_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", kafka_topic) \
    .load()

# Define the flattened schema matching kafka_publisher.py
schema = StructType([
    StructField("timestamp_wall", StringType()), # keeping as string to parse later if needed
    StructField("timestamp_game", FloatType()),
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
    
    # Flattened Wheel FL
    StructField("tyre_pressure_fl", FloatType()),
    StructField("tyre_temp_fl", FloatType()),
    StructField("tyre_inner_temp_fl", FloatType()),
    StructField("tyre_middle_temp_fl", FloatType()),
    StructField("tyre_outer_temp_fl", FloatType()),
    StructField("tyre_slip_fl", FloatType()),
    StructField("suspension_travel_fl", FloatType()),
    StructField("brake_temp_fl", FloatType()),
    StructField("tyre_wear_fl", FloatType()),

    # Flattened Wheel FR
    StructField("tyre_pressure_fr", FloatType()),
    StructField("tyre_temp_fr", FloatType()),
    StructField("tyre_inner_temp_fr", FloatType()),
    StructField("tyre_middle_temp_fr", FloatType()),
    StructField("tyre_outer_temp_fr", FloatType()),
    StructField("tyre_slip_fr", FloatType()),
    StructField("suspension_travel_fr", FloatType()),
    StructField("brake_temp_fr", FloatType()),
    StructField("tyre_wear_fr", FloatType()),

    # Flattened Wheel RL
    StructField("tyre_pressure_rl", FloatType()),
    StructField("tyre_temp_rl", FloatType()),
    StructField("tyre_inner_temp_rl", FloatType()),
    StructField("tyre_middle_temp_rl", FloatType()),
    StructField("tyre_outer_temp_rl", FloatType()),
    StructField("tyre_slip_rl", FloatType()),
    StructField("suspension_travel_rl", FloatType()),
    StructField("brake_temp_rl", FloatType()),
    StructField("tyre_wear_rl", FloatType()),

    # Flattened Wheel RR
    StructField("tyre_pressure_rr", FloatType()),
    StructField("tyre_temp_rr", FloatType()),
    StructField("tyre_inner_temp_rr", FloatType()),
    StructField("tyre_middle_temp_rr", FloatType()),
    StructField("tyre_outer_temp_rr", FloatType()),
    StructField("tyre_slip_rr", FloatType()),
    StructField("suspension_travel_rr", FloatType()),
    StructField("brake_temp_rr", FloatType()),
    StructField("tyre_wear_rr", FloatType()),
    
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

# Parse the JSON payload and unpack columns using data.*
parsed_stream = raw_stream \
    .selectExpr("CAST(key AS STRING) as session_key", "CAST(value AS STRING) as json_payload") \
    .select("session_key", from_json(col("json_payload"), schema).alias("data")) \
    .select("session_key", "data.*")

# Add a timestamp column representing when Spark processed the record
from pyspark.sql.functions import current_timestamp
processed_stream = parsed_stream.withColumn("processed_at", current_timestamp())

import os
# Checkpoints are required for State and Exactly-Once Fault Tolerance in Spark Streaming
checkpoint_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "checkpoints", "telemetry"))
output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "telemetry", "parquet_stream"))

# 1. Output to Console (for visualizing the stream live)
console_query = processed_stream \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# 2. Output to Parquet Files (Partitioned by session & lap)
parquet_query = processed_stream \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_dir) \
    .option("checkpointLocation", checkpoint_dir) \
    .partitionBy("session_key", "completed_laps") \
    .start()

print("🚀 Spark Structured Streaming engine started. Listening to Kafka topic: telemetry_raw")
print(f"📁 Writing Parquet files to: {output_dir}")

# Wait for termination
spark.streams.awaitAnyTermination()