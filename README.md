# RetailX Data Platform

> Real-time streaming ETL pipeline — dari raw transaksi retail hingga dashboard analitik

## Dataset

Using Brazilian E-Commerce Public Dataset by Olist (Kaggle)
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?resource=download

### Data Explorer

1. Data Customers 
| Cust ID | Unique ID | ZIP Code | City | State |
2. Geolocation 
| Zip Code | Latitude | Longitude | City | State |
3. Order Item List 
| Order ID | Order Item ID | Product ID | Seller ID | Shipping Limit Date | Price | Freight Value
4. Order Payments List 
| Order ID | Payment Sequential | Payment Type | Payment Installments | Payment Value |
5. Product Review
| Review ID | Order ID | Review Score | Review Comment Title | Review Comment Message | Review Creation Date | Review Answer Timestamp |
6. History Order Dataset
| Order ID | Customer ID | Order Status | Order Purchase Timestamp | Order Approved Timestamp | Order Delivered Carrier Date | Order Delivered Customer Date | Order Estimated Delivery Date |
7. Product List
| Product ID | Product Category Name | Product Name Lenght | Product Description Lenght | Product Photos Quuantity | Product Weight Gram | Product Length CM | Product Height CM | Product Width CM | 
8. Seller 
| Seller ID | Seller ZIP Code Prefix | Seller's City | Seller's State |
9. Translation
| Product Category Name | Product Category Name English |

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