"""
Athena Query Layer — Replaces DuckDB for AWS deployment mode.

Provides helper functions to:
  1. Run SQL queries against S3 data via Amazon Athena
  2. Create Athena external tables over the S3 data lake
  3. Return results as Pandas DataFrames for Streamlit dashboards

Prerequisites:
  pip install awswrangler boto3

Usage:
    from src.storage.athena_client import AthenaClient

    client = AthenaClient()
    client.create_tables()    # One-time setup
    df = client.query("SELECT AVG(speed_kmh) FROM telemetry_raw WHERE track='Monza'")
"""

import boto3
import pandas as pd
from loguru import logger
from typing import Optional

try:
    import awswrangler as wr
    HAS_WRANGLER = True
except ImportError:
    HAS_WRANGLER = False

from config.settings import (
    AWS_REGION,
    S3_BUCKET,
    S3_RAW_PREFIX,
    S3_PROCESSED_PREFIX,
    ATHENA_DATABASE,
    ATHENA_RESULTS_BUCKET,
)


class AthenaClient:
    """
    Thin wrapper around Amazon Athena for querying the S3 data lake.
    Uses awswrangler for seamless Athena → Pandas DataFrame conversion.
    """

    def __init__(self, database: str = None, region: str = None):
        self._database = database or ATHENA_DATABASE
        self._region = region or AWS_REGION
        self._results_location = ATHENA_RESULTS_BUCKET

        if not HAS_WRANGLER:
            raise ImportError(
                "awswrangler is required for Athena queries. "
                "Install it with: pip install awswrangler"
            )

        logger.info(f"AthenaClient initialized | DB: {self._database} | Region: {self._region}")

    def _run_ddl(self, sql: str):
        """Execute a DDL statement (CREATE, DROP, MSCK REPAIR) and wait for completion."""
        query_id = wr.athena.start_query_execution(
            sql=sql,
            database=self._database,
            s3_output=self._results_location,
            wait=True,
        )

    def create_database(self):
        """Create the Athena database if it doesn't exist."""
        # For CREATE DATABASE, we don't pass a database param (it doesn't exist yet)
        query_id = wr.athena.start_query_execution(
            sql=f"CREATE DATABASE IF NOT EXISTS {self._database}",
            s3_output=self._results_location,
            wait=True,
        )
        logger.info(f"✅ Database '{self._database}' ready")

    def create_tables(self):
        """
        Create Athena external tables pointing to S3 locations.
        Run this once after setting up the S3 bucket and Firehose.
        """
        self.create_database()

        # ── Raw telemetry table (JSON from Firehose) ──
        raw_ddl = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {self._database}.telemetry_raw (
            timestamp_wall STRING,
            timestamp_game FLOAT,
            session_id STRING,
            car STRING,
            track STRING,
            session_type STRING,
            status STRING,
            completed_laps INT,
            current_lap_time_ms INT,
            last_lap_time_ms INT,
            best_lap_time_ms INT,
            distance_into_lap FLOAT,
            normalized_position FLOAT,
            current_sector INT,
            last_sector_time_ms INT,
            is_valid_lap BOOLEAN,
            is_in_pit BOOLEAN,
            is_in_pit_lane BOOLEAN,
            throttle FLOAT,
            brake FLOAT,
            steering FLOAT,
            clutch FLOAT,
            speed_kmh FLOAT,
            gear INT,
            rpm INT,
            fuel_remaining FLOAT,
            fuel_per_lap FLOAT,
            g_lat FLOAT,
            g_lon FLOAT,
            g_vert FLOAT,
            tyre_pressure_fl FLOAT,
            tyre_pressure_fr FLOAT,
            tyre_pressure_rl FLOAT,
            tyre_pressure_rr FLOAT,
            tyre_temp_fl FLOAT,
            tyre_temp_fr FLOAT,
            tyre_temp_rl FLOAT,
            tyre_temp_rr FLOAT,
            tyre_inner_temp_fl FLOAT,
            tyre_inner_temp_fr FLOAT,
            tyre_inner_temp_rl FLOAT,
            tyre_inner_temp_rr FLOAT,
            tyre_middle_temp_fl FLOAT,
            tyre_middle_temp_fr FLOAT,
            tyre_middle_temp_rl FLOAT,
            tyre_middle_temp_rr FLOAT,
            tyre_outer_temp_fl FLOAT,
            tyre_outer_temp_fr FLOAT,
            tyre_outer_temp_rl FLOAT,
            tyre_outer_temp_rr FLOAT,
            tyre_slip_fl FLOAT,
            tyre_slip_fr FLOAT,
            tyre_slip_rl FLOAT,
            tyre_slip_rr FLOAT,
            suspension_travel_fl FLOAT,
            suspension_travel_fr FLOAT,
            suspension_travel_rl FLOAT,
            suspension_travel_rr FLOAT,
            brake_temp_fl FLOAT,
            brake_temp_fr FLOAT,
            brake_temp_rl FLOAT,
            brake_temp_rr FLOAT,
            tyre_wear_fl FLOAT,
            tyre_wear_fr FLOAT,
            tyre_wear_rl FLOAT,
            tyre_wear_rr FLOAT,
            tc_level INT,
            tc_cut INT,
            abs_level INT,
            engine_map INT,
            brake_bias FLOAT,
            air_temp FLOAT,
            road_temp FLOAT,
            track_grip STRING,
            rain_intensity STRING,
            wind_speed FLOAT,
            wind_direction FLOAT
        )
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
        LOCATION 's3://{S3_BUCKET}/{S3_RAW_PREFIX}'
        """
        self._run_ddl(raw_ddl)
        logger.info("✅ Table 'telemetry_raw' created")

        # ── Processed telemetry table (Parquet from Glue ETL) ──
        processed_ddl = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {self._database}.telemetry_processed (
            timestamp_wall STRING,
            timestamp_game FLOAT,
            car STRING,
            track STRING,
            session_type STRING,
            status STRING,
            current_lap_time_ms INT,
            last_lap_time_ms INT,
            best_lap_time_ms INT,
            distance_into_lap FLOAT,
            normalized_position FLOAT,
            current_sector INT,
            throttle FLOAT,
            brake FLOAT,
            steering FLOAT,
            speed_kmh FLOAT,
            gear INT,
            rpm INT,
            fuel_remaining FLOAT,
            g_lat FLOAT,
            g_lon FLOAT,
            tyre_temp_fl FLOAT,
            tyre_temp_fr FLOAT,
            tyre_temp_rl FLOAT,
            tyre_temp_rr FLOAT,
            avg_tyre_temp FLOAT,
            avg_tyre_pressure FLOAT,
            is_heavy_braking BOOLEAN,
            is_full_throttle BOOLEAN,
            timestamp_parsed TIMESTAMP
        )
        PARTITIONED BY (session_id STRING, completed_laps INT)
        STORED AS PARQUET
        LOCATION 's3://{S3_BUCKET}/{S3_PROCESSED_PREFIX}telemetry/'
        """
        self._run_ddl(processed_ddl)
        logger.info("✅ Table 'telemetry_processed' created")

        # ── Lap summary table ──
        summary_ddl = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {self._database}.lap_summary (
            session_id STRING,
            completed_laps INT,
            car STRING,
            track STRING,
            max_speed_kmh FLOAT,
            avg_speed_kmh FLOAT,
            avg_tyre_temp FLOAT,
            avg_tyre_pressure FLOAT,
            avg_fuel FLOAT,
            min_fuel FLOAT,
            heavy_braking_count BIGINT,
            full_throttle_count BIGINT,
            sample_count BIGINT,
            lap_start_time TIMESTAMP,
            lap_end_time TIMESTAMP
        )
        STORED AS PARQUET
        LOCATION 's3://{S3_BUCKET}/{S3_PROCESSED_PREFIX}lap_summary/'
        """
        self._run_ddl(summary_ddl)
        logger.info("✅ Table 'lap_summary' created")

    def query(self, sql: str) -> pd.DataFrame:
        """
        Run an SQL query against Athena and return results as a Pandas DataFrame.

        Example:
            df = client.query("SELECT * FROM telemetry_raw LIMIT 100")
            df = client.query("SELECT AVG(speed_kmh), track FROM lap_summary GROUP BY track")
        """
        logger.debug(f"Athena query: {sql[:100]}...")
        df = wr.athena.read_sql_query(
            sql=sql,
            database=self._database,
            s3_output=self._results_location,
        )
        logger.debug(f"  → {len(df)} rows returned")
        return df

    def repair_table(self, table_name: str):
        """Run MSCK REPAIR TABLE to discover new partitions (for partitioned tables)."""
        self._run_ddl(f"MSCK REPAIR TABLE {self._database}.{table_name}")
        logger.info(f"✅ Repaired partitions for '{table_name}'")

    def get_sessions(self) -> pd.DataFrame:
        """Get a list of all sessions with basic stats."""
        return self.query("""
            SELECT 
                session_id,
                car,
                track,
                session_type,
                MAX(completed_laps) as total_laps,
                MIN(timestamp_wall) as session_start,
                MAX(timestamp_wall) as session_end,
                AVG(speed_kmh) as avg_speed
            FROM telemetry_raw
            WHERE status = 'live'
            GROUP BY session_id, car, track, session_type
            ORDER BY session_start DESC
        """)

    def get_lap_data(self, session_id: str, lap_number: int) -> pd.DataFrame:
        """Get full telemetry for a specific lap."""
        return self.query(f"""
            SELECT * 
            FROM telemetry_raw
            WHERE session_id = '{session_id}'
              AND completed_laps = {lap_number}
              AND status = 'live'
            ORDER BY timestamp_wall
        """)

    def get_lap_summary(self, session_id: Optional[str] = None) -> pd.DataFrame:
        """Get lap summary statistics."""
        where = f"WHERE session_id = '{session_id}'" if session_id else ""
        return self.query(f"""
            SELECT * FROM lap_summary
            {where}
            ORDER BY session_id, completed_laps
        """)
