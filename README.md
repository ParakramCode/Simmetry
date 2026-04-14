# 🏎️ Sim Racing Telemetry Intelligence Platform

> A real-time telemetry analytics platform for sim racing that ingests live ACC session data at 30Hz, identifies exactly where and why you're losing lap time, and gives setup recommendations grounded in your own driving data — with a natural language interface to query your session history.

---

## Architecture

The platform supports **two deployment modes** controlled by a single environment variable:

### Local Mode (`DEPLOYMENT_MODE=local`)
```
ACC Shared Memory → Telemetry Listener (30Hz) → Kafka → Spark Structured Streaming
                                                            ↓
                                              Parquet (raw per-lap traces)
                                              PostgreSQL (sessions, laps, setups)
                                                            ↓
                                              Analytics Layer → AI Agent → Streamlit
```

### Cloud Mode (`DEPLOYMENT_MODE=aws`)
```
ACC Shared Memory → Telemetry Listener (30Hz) → Kinesis Data Firehose → Amazon S3 (Data Lake)
                                                                              ↓
                                                                    AWS Glue ETL (Spark)
                                                                              ↓
                                                                S3 Processed (Parquet) + Lap Summary
                                                                              ↓
                                                                    Amazon Athena (SQL)
                                                                              ↓
                                                                    Streamlit Dashboard (EC2)
```

**Two-layer storage:** Raw 30Hz telemetry goes to Parquet (columnar, efficient for time-series). Derived/aggregated metrics go to PostgreSQL (local) or Athena lap_summary tables (cloud). DuckDB (local) or Athena (cloud) bridges both for unified queries.

---

## AWS Cloud Architecture

| Layer | Service | Purpose |
|---|---|---|
| **Edge** | Local PC (Windows) | Runs ACC + Telemetry Listener |
| **Ingestion** | Kinesis Data Firehose | Buffers & batches JSON telemetry into S3 |
| **Data Lake (Raw)** | Amazon S3 (`raw/`) | Stores raw JSON telemetry from Firehose |
| **Processing** | AWS Glue (Serverless Spark) | Transforms JSON → partitioned Parquet + analytics |
| **Data Lake (Processed)** | Amazon S3 (`processed/`) | Stores optimized Parquet files + lap summaries |
| **Query Engine** | Amazon Athena | Serverless SQL queries over S3 Parquet |
| **Dashboard** | Streamlit on EC2 (t2.micro) | Interactive visualizations via Athena |

**Estimated cost:** Under $1/month within AWS free tier.

---

## Quick Start

### Prerequisites
- Python 3.11+
- ACC installed (for live telemetry — sample data available for offline testing)
- Docker Desktop (for local Kafka + PostgreSQL)
- AWS CLI + credentials (for cloud mode)

### Setup
```bash
python -m venv venv
.\venv\Scripts\activate          # Windows
pip install -r requirements.txt
```

### Generate Sample Data (no ACC needed)
```bash
python scripts/generate_sample_data.py
```

### Run Tests
```bash
python -m pytest tests/ -v
```

### Run Live Listener (requires ACC running)
```bash
# Local mode (Kafka)
python -m src.listener.main

# AWS mode
set DEPLOYMENT_MODE=aws
python -m src.listener.main
```

---

## AWS Setup (Cloud Mode)

### 1. Configure AWS credentials
```bash
aws configure
# Enter your Access Key, Secret Key, and Region (us-east-1)
```

### 2. Provision infrastructure
```bash
python scripts/setup_aws.py
```
This creates: S3 bucket, IAM role, Kinesis Data Firehose stream.

### 3. Update .env
```ini
DEPLOYMENT_MODE=aws
S3_BUCKET=sim-telemetry-lake
FIREHOSE_STREAM_NAME=telemetry-firehose
```

### 4. Upload sample data & create Athena tables
```bash
python scripts/generate_sample_data.py
python scripts/upload_to_s3.py
python -c "from src.storage.athena_client import AthenaClient; AthenaClient().create_tables()"
```

### 5. Query with Athena
```python
from src.storage.athena_client import AthenaClient
client = AthenaClient()
df = client.query("SELECT AVG(speed_kmh), track FROM telemetry_raw GROUP BY track")
```

---

## Project Structure

| Directory | Purpose |
|---|---|
| `src/listener/` | ACC shared memory reader, session management, data publisher |
| `src/listener/aws_publisher.py` | ☁️ AWS Firehose + S3 publisher (cloud mode) |
| `src/listener/kafka_publisher.py` | 🖥️ Kafka publisher (local mode) |
| `src/streaming/` | Spark Structured Streaming (local) / AWS Glue ETL (cloud) |
| `src/storage/` | PostgreSQL models (local) / Athena client (cloud) |
| `src/analytics/` | Lap decomposition, braking analysis, tyre analysis |
| `src/agent/` | Claude API agent with 7 data-grounded tools |
| `src/dashboard/` | Streamlit UI |
| `config/` | Settings, track definitions (sector boundaries, corners) |
| `scripts/` | Data generation, S3 upload, AWS setup, Kafka topic creation |
| `tests/` | Schema validation, unit tests |

## Tech Stack

| Layer | Local Mode | Cloud Mode |
|---|---|---|
| Telemetry | Python (`mmap` + `ctypes`) | Same |
| Ingestion | Apache Kafka | Kinesis Data Firehose |
| Raw Storage | Local Parquet | Amazon S3 |
| Processing | Spark Structured Streaming | AWS Glue (Serverless Spark) |
| Serving DB | PostgreSQL | Amazon Athena |
| Query Bridge | DuckDB | awswrangler |
| AI Agent | Claude API (tool calling) | Same |
| Dashboard | Streamlit (local) | Streamlit (EC2) |

## License

MIT
