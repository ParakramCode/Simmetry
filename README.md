# 🏎️ Sim Racing Telemetry Intelligence Platform

> A real-time telemetry analytics platform for sim racing that ingests live ACC session data at 60Hz, identifies exactly where and why you're losing lap time, and gives setup recommendations grounded in your own driving data — with a natural language interface to query your session history.

---

## Architecture

```
ACC Shared Memory → Telemetry Listener (60Hz) → Kafka → Spark Structured Streaming
                                                            ↓
                                              Parquet (raw per-lap traces)
                                              PostgreSQL (sessions, laps, setups)
                                                            ↓
                                              Analytics Layer → AI Agent → Streamlit
```

**Two-layer storage:** Raw 60Hz telemetry goes to Parquet (columnar, efficient for time-series). Derived/aggregated metrics go to PostgreSQL. DuckDB bridges both for unified queries.

## Quick Start

### Prerequisites
- Python 3.11+
- ACC installed (for live telemetry — sample data available for offline testing)
- Docker Desktop (for Kafka + PostgreSQL in Week 2+)

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

### Run Live Listener (requires ACC to be running)
```bash
python -m src.listener.main
```

## Project Structure

| Directory | Purpose |
|---|---|
| `src/listener/` | ACC shared memory reader, session management, data publisher |
| `src/streaming/` | Spark Structured Streaming — lap detection, sector alignment |
| `src/storage/` | PostgreSQL models, DuckDB bridge, setup delta computation |
| `src/analytics/` | Lap decomposition, braking analysis, tyre analysis |
| `src/agent/` | Claude API agent with 7 data-grounded tools |
| `src/dashboard/` | Streamlit UI |
| `config/` | Settings, track definitions (sector boundaries, corners) |
| `scripts/` | Data generation, Kafka topic creation, session replay |
| `tests/` | Schema validation, unit tests |

## Tech Stack

| Layer | Technology |
|---|---|
| Telemetry | Python (`mmap` + `ctypes`) |
| Streaming | Apache Kafka → Spark Structured Streaming |
| Raw Storage | Parquet (per-lap files) |
| Serving DB | PostgreSQL |
| Query Bridge | DuckDB |
| AI Agent | Claude API (tool calling) |
| Dashboard | Streamlit |

## License

MIT
