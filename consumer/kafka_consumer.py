import json
import os
import time
import duckdb
from datetime import datetime
from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv

load_dotenv()

# Config
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
BATCH_SIZE = 100 # row
FLUSH_INTERVAL = 3 # detik
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'retailx.duckdb')
DLQ_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'dlq', 'failed_messages.jsonl')

os.makedirs(os.path.dirname(DLQ_PATH), exist_ok=True)

conn = duckdb.connect(DB_PATH)

def init_schema():
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw.orders (
            order_id VARCHAR,
            customer_id VARCHAR,
            order_status VARCHAR,
            order_purchase_timestamp VARCHAR,
            order_approved_at VARCHAR,
            order_delivered_carrier_date VARCHAR,
            order_delivered_customer_date VARCHAR,
            order_estimated_delivery_date VARCHAR,
            ingested_at VARCHAR,
            _ingested_at TIMESTAMP DEFAULT current_timestamp,
            _kafka_offset BIGINT,
            _kafka_partition INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw.order_items (
            order_id VARCHAR,
            order_item_id VARCHAR,
            product_id VARCHAR,
            seller_id VARCHAR,
            shipping_limit_date VARCHAR,
            price DOUBLE,
            freight_value DOUBLE,
            ingested_at VARCHAR,
            _ingested_at TIMESTAMP DEFAULT current_timestamp,
            _kafka_offset BIGINT,
            _kafka_partition INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw.payments (
            order_id VARCHAR,
            payment_sequential INTEGER,
            payment_type VARCHAR,
            payment_installments INTEGER,
            payment_value DOUBLE,
            ingested_at VARCHAR,
            _ingested_at TIMESTAMP DEFAULT current_timestamp,
            _kafka_offset BIGINT,
            _kafka_partition INTEGER
        )
    """)
    print("Schema raw siap.")

def write_to_dlq(topic, partition, offset, raw_value, error):
    with open(DLQ_PATH, 'a', encoding='utf-8') as f:
        entry = {
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "raw_value": raw_value,
            "error": str(error),
            "failed_at": datetime.utcnow().isoformat()
        }
        f.write(json.dumps(entry) + '\n')

def flush_batch(topic, batch):
    if not batch:
        return
    try:
        if topic == "retail.orders":
            conn.executemany("""
                INSERT INTO raw.orders VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp, ?, ?
                )
            """, [(
                r["order_id"], r["customer_id"], r["order_status"],
                r["order_purchase_timestamp"], r["order_approved_at"],
                r["order_delivered_carrier_date"], r["order_delivered_customer_date"],
                r["order_estimated_delivery_date"], r["ingested_at"],
                r["_offset"], r["_partition"]
            ) for r in batch])

        elif topic == "retail.order_items":
            conn.executemany("""
                INSERT INTO raw.order_items VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp, ?, ?
                )
            """, [(
                r["order_id"], r["order_item_id"], r["product_id"],
                r["seller_id"], r["shipping_limit_date"],
                r["price"], r["freight_value"], r["ingested_at"],
                r["_offset"], r["_partition"]
            ) for r in batch])

        elif topic == "retail.payments":
            conn.executemany("""
                INSERT INTO raw.payments VALUES (
                    ?, ?, ?, ?, ?, ?, current_timestamp, ?, ?
                )
            """, [(
                r["order_id"], r["payment_sequential"], r["payment_type"],
                r["payment_installments"], r["payment_value"], r["ingested_at"],
                r["_offset"], r["_partition"]
            ) for r in batch])

        print(f"  Flushed {len(batch)} rows → {topic} [{datetime.utcnow().isoformat()}]")

    except Exception as e:
        print(f"  ERROR flush {topic}: {e}")

def run_consumer():
    print("=== RetailX Kafka Consumer ===")
    print(f"Connecting to Kafka: {KAFKA_BOOTSTRAP}")

    init_schema()

    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': 'retailx-consumer-group-v3',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    })

    consumer.subscribe(["retail.orders", "retail.order_items", "retail.payments"])

    buffers = {
        "retail.orders": [],
        "retail.order_items": [],
        "retail.payments": []
    }

    last_flush = time.time()

    print("Mendengarkan messages... (Ctrl+C untuk stop)")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Tidak ada message baru — cek apakah perlu flush timeout
                if (time.time() - last_flush) >= FLUSH_INTERVAL:
                    for t, batch in buffers.items():
                        flush_batch(t, batch)
                        buffers[t] = []
                    last_flush = time.time()
                continue

            if msg.error():
                raise KafkaException(msg.error())

            topic = msg.topic()

            try:
                data = json.loads(msg.value().decode('utf-8'))
                data["_offset"] = msg.offset()
                data["_partition"] = msg.partition()
                buffers[topic].append(data)

            except Exception as e:
                write_to_dlq(topic, msg.partition(), msg.offset(), str(msg.value()), e)
                print(f"  DLQ: {topic} offset {msg.offset()} — {e}")
                continue

            if (len(buffers[topic]) >= BATCH_SIZE or
                    (time.time() - last_flush) >= FLUSH_INTERVAL):
                for t, batch in buffers.items():
                    flush_batch(t, batch)
                    buffers[t] = []
                last_flush = time.time()

    except KeyboardInterrupt:
        print("\nStopping consumer...")
        for t, batch in buffers.items():
            flush_batch(t, batch)
    finally:
        consumer.close()
        conn.close()
        print("Consumer stopped.")

if __name__ == "__main__":
    run_consumer()