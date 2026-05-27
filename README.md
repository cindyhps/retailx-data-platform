# RetailX Data Platform

> Real-time streaming ETL pipeline — dari raw transaksi retail hingga dashboard analitik

## Architecture

CSV (Olist) → Kafka → DuckDB (raw) → dbt (transform) → Metabase (dashboard)
→ Grafana (monitoring)
Airflow mengorkestrasikan seluruh pipeline

## Tech Stack
| Layer | Tools |
|---|---|
| Ingestion | Apache Kafka, Python |
| Storage | DuckDB |
| Transform | dbt |
| Orchestration | Apache Airflow |
| Visualization | Metabase, Grafana |
| Infrastructure | Docker, Docker Compose |

## Quick Start
```bash
# 1. Clone repo
git clone https://github.com/USERNAME/retailx-data-platform.git
cd retailx-data-platform

# 2. Setup environment
cp .env.example .env

# 3. Jalankan semua services
docker-compose up -d

# 4. Buat Kafka topics
powershell -ExecutionPolicy Bypass -File scripts/init_kafka_topics.ps1
```

## Services
| Service | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| Airflow | http://localhost:8081 |
| Grafana | http://localhost:3000 |
| Metabase | http://localhost:3001 |

## Status
🚧 Week 1 of 4 — Infrastructure & Streaming Pipeline