# RetailX Data Platform

> Real-time streaming ETL pipeline — dari raw transaksi retail hingga dashboard analitik

## Dataset

Using Brazilian E-Commerce Public Dataset by Olist (Kaggle)
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?resource=download

### Dataset Explorer

| Dataset | Attributes |
|----------|-----------|
| Customer | Cust ID, Unique ID, ZIP Code, City, State |
| Geolocation | Zip Code, Latitude, Longitude, City, State |
| Order Items | Order ID, Order Item ID, Product ID, Seller ID, Shipping Limit Date, Price, Freight Value |
| Order Payments | Order ID, Payment Sequential, Payment Type, Payment Installments, Payment Value |
| Product Reviews | Review ID, Order ID, Review Score, Review Comment Title, Review Comment Message, Review Creation Date, Review Answer Timestamp |
| Orders History | Order ID, Customer ID, Order Status, Order Purchase Timestamp, Order Approved Timestamp, Order Delivered Carrier Date, Order Delivered Customer Date, Order Estimated Delivery Date |
| Products | Product ID, Product Category Name, Product Name Length, Product Description Length, Product Photos Quantity, Product Weight Gram, Product Length CM, Product Height CM, Product Width CM |
| Sellers | Seller ID, Seller ZIP Code Prefix, Seller City, Seller State |
| Category Translation | Product Category Name, Product Category Name English |

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
| dbt Docs | http://localhost:8085 |

## Status
🚧 Week 1 of 3 — Infrastructure & Streaming Pipeline

## dbt Documentation

Generate documentation:

```bash
dbt docs generate
```

Serve docs locally:

```bash
dbt docs serve --port 8085
```

Open in browser:

```text
http://localhost:8085
```