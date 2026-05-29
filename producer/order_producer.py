import csv
import json
import time
import os
from datetime import datetime
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

# Config
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
RATE_LIMIT = 600  # events per detik
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')

# Init producer
producer = Producer({
    'bootstrap.servers': KAFKA_BOOTSTRAP
})

def delivery_report(err, msg):
    
    
    if err is not None:
        print(f"  ERROR deliver: {err}")

def read_csv(filename):
    filepath = os.path.join(DATA_DIR, filename)
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return list(reader)

def produce_orders():
    print("Membaca olist_orders_dataset.csv...")
    orders = read_csv('olist_orders_dataset.csv')
    print(f"Total orders: {len(orders)}")

    for i, row in enumerate(orders):
        event = {
            "event_type": "order_placed",
            "order_id": row["order_id"],
            "customer_id": row["customer_id"],
            "order_status": row["order_status"],
            "order_purchase_timestamp": row["order_purchase_timestamp"],
            "order_approved_at": row["order_approved_at"],
            "order_delivered_carrier_date": row["order_delivered_carrier_date"],
            "order_delivered_customer_date": row["order_delivered_customer_date"],
            "order_estimated_delivery_date": row["order_estimated_delivery_date"],
            "ingested_at": datetime.utcnow().isoformat()
        }

        producer.produce(
            topic="retail.orders",
            key=row["order_id"],
            value=json.dumps(event).encode('utf-8'),
            callback=delivery_report
        )

        producer.poll(0)

        if (i + 1) % 100 == 0:
            print(f"  Orders sent: {i + 1}/{len(orders)}")

        time.sleep(1 / RATE_LIMIT)

    producer.flush()
    print("Selesai produce orders.")

def produce_order_items():
    print("Membaca olist_order_items_dataset.csv...")
    items = read_csv('olist_order_items_dataset.csv')
    print(f"Total order items: {len(items)}")

    for i, row in enumerate(items):
        event = {
            "event_type": "order_item",
            "order_id": row["order_id"],
            "order_item_id": row["order_item_id"],
            "product_id": row["product_id"],
            "seller_id": row["seller_id"],
            "shipping_limit_date": row["shipping_limit_date"],
            "price": float(row["price"]),
            "freight_value": float(row["freight_value"]),
            "ingested_at": datetime.utcnow().isoformat()
        }

        producer.produce(
            topic="retail.order_items",
            key=row["order_id"],
            value=json.dumps(event).encode('utf-8'),
            callback=delivery_report
        )

        producer.poll(0)

        if (i + 1) % 100 == 0:
            print(f"  Items sent: {i + 1}/{len(items)}")

        time.sleep(1 / RATE_LIMIT)

    producer.flush()
    print("Selesai produce order items.")

def produce_payments():
    print("Membaca olist_order_payments_dataset.csv...")
    payments = read_csv('olist_order_payments_dataset.csv')
    print(f"Total payments: {len(payments)}")

    for i, row in enumerate(payments):
        event = {
            "event_type": "payment_made",
            "order_id": row["order_id"],
            "payment_sequential": int(row["payment_sequential"]),
            "payment_type": row["payment_type"],
            "payment_installments": int(row["payment_installments"]),
            "payment_value": float(row["payment_value"]),
            "ingested_at": datetime.utcnow().isoformat()
        }

        producer.produce(
            topic="retail.payments",
            key=row["order_id"],
            value=json.dumps(event).encode('utf-8'),
            callback=delivery_report
        )

        producer.poll(0)

        if (i + 1) % 100 == 0:
            print(f"  Payments sent: {i + 1}/{len(payments)}")

        time.sleep(1 / RATE_LIMIT)

    producer.flush()
    print("Selesai produce payments.")

if __name__ == "__main__":
    print("=== RetailX Kafka Producer ===")
    print(f"Connecting to Kafka: {KAFKA_BOOTSTRAP}")
    produce_orders()
    produce_order_items()
    produce_payments()
    print("=== Semua data berhasil di-produce ===")