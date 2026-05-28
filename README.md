# RetailX Data Platform

> Real-time streaming ETL pipeline — dari raw transaksi retail hingga dashboard analitik

## Dataset

Using Brazilian E-Commerce Public Dataset by Olist (Kaggle)
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?resource=download

### Data Explorer

##### 1. Customer Dataset

| Column Name | Description |
|---|---|
| Cust ID | Unique identifier for each customer |
| Unique ID | Unique customer reference ID |
| ZIP Code | Customer ZIP code prefix |
| City | Customer city |
| State | Customer state |

---

##### 2. Geolocation 

| Column Name | Description |
|---|---|
| Zip Code | ZIP code prefix |
| Latitude | Geographic latitude |
| Longitude | Geographic longitude |
| City | City name |
| State | State abbreviation |

---

##### 3. Order Item List 

| Column Name | Description |
|---|---|
| Order ID | Unique identifier for each order |
| Order Item ID | Item sequence number within an order |
| Product ID | Product identifier |
| Seller ID | Seller identifier |
| Shipping Limit Date | Shipping deadline date |
| Price | Product price |
| Freight Value | Shipping/freight cost |

---

##### 4. Order Payments Dataset

| Column Name | Description |
|---|---|
| Order ID | Unique order identifier |
| Payment Sequential | Payment sequence number |
| Payment Type | Type of payment method |
| Payment Installments | Number of installments |
| Payment Value | Total payment value |

---

##### 5. Product Review Dataset

| Column Name | Description |
|---|---|
| Review ID | Unique review identifier |
| Order ID | Related order identifier |
| Review Score | Customer review score |
| Review Comment Title | Review title |
| Review Comment Message | Review message content |
| Review Creation Date | Date review was created |
| Review Answer Timestamp | Timestamp of review response |

---

##### 6. Orders History Dataset

| Column Name | Description |
|---|---|
| Order ID | Unique order identifier |
| Customer ID | Customer identifier |
| Order Status | Current order status |
| Order Purchase Timestamp | Purchase timestamp |
| Order Approved Timestamp | Order approval timestamp |
| Order Delivered Carrier Date | Carrier delivery date |
| Order Delivered Customer Date | Customer delivery date |
| Order Estimated Delivery Date | Estimated delivery date |

---

##### 7. Product Dataset

| Column Name | Description |
|---|---|
| Product ID | Unique product identifier |
| Product Category Name | Product category |
| Product Name Length | Length of product name |
| Product Description Length | Length of product description |
| Product Photos Quantity | Number of product photos |
| Product Weight Gram | Product weight in grams |
| Product Length CM | Product length in centimeters |
| Product Height CM | Product height in centimeters |
| Product Width CM | Product width in centimeters |

---

##### 8. Seller Dataset

| Column Name | Description |
|---|---|
| Seller ID | Unique seller identifier |
| Seller ZIP Code Prefix | Seller ZIP code prefix |
| Seller City | Seller city |
| Seller State | Seller state |

---

##### 9. Translation Dataset

| Column Name | Description |
|---|---|
| Product Category Name | Original product category name |
| Product Category Name English | English-translated category name |

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