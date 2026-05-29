import json
import os
import time
import duckdb
from datetime import datetime
from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv

load_dotenv(override=False)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
GROUP_ID        = "retailx-consumer-group-v3"
TOPICS          = ["retail.orders", "retail.order_items", "retail.payments"]

BATCH_SIZE      = 10000   # rows sebelum flush
FLUSH_INTERVAL  = 30     # detik sebelum flush by timeout
MAX_EMPTY_POLLS = 15    # jumlah poll kosong berturut-turut sebelum consumer berhenti (asumsi sudah tidak ada data baru)

DB_PATH  = os.getenv("DB_PATH", os.path.join(os.path.dirname(__file__), '..', 'data', 'retailx.duckdb'))
DLQ_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'dlq', 'failed_messages.jsonl')

os.makedirs(os.path.dirname(DLQ_PATH), exist_ok=True)


# ─────────────────────────────────────────────
# DB CONNECTION
# ─────────────────────────────────────────────
conn = duckdb.connect(DB_PATH)


# ─────────────────────────────────────────────
# SCHEMA INIT
# ─────────────────────────────────────────────
def init_schema():
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw.orders (
            order_id                        VARCHAR,
            customer_id                     VARCHAR,
            order_status                    VARCHAR,
            order_purchase_timestamp        VARCHAR,
            order_approved_at               VARCHAR,
            order_delivered_carrier_date    VARCHAR,
            order_delivered_customer_date   VARCHAR,
            order_estimated_delivery_date   VARCHAR,
            ingested_at                     VARCHAR,
            _ingested_at                    TIMESTAMP DEFAULT current_timestamp,
            _kafka_offset                   BIGINT,
            _kafka_partition                INTEGER
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw.order_items (
            order_id            VARCHAR,
            order_item_id       VARCHAR,
            product_id          VARCHAR,
            seller_id           VARCHAR,
            shipping_limit_date VARCHAR,
            price               DOUBLE,
            freight_value       DOUBLE,
            ingested_at         VARCHAR,
            _ingested_at        TIMESTAMP DEFAULT current_timestamp,
            _kafka_offset       BIGINT,
            _kafka_partition    INTEGER
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw.payments (
            order_id                VARCHAR,
            payment_sequential      INTEGER,
            payment_type            VARCHAR,
            payment_installments    INTEGER,
            payment_value           DOUBLE,
            ingested_at             VARCHAR,
            _ingested_at            TIMESTAMP DEFAULT current_timestamp,
            _kafka_offset           BIGINT,
            _kafka_partition        INTEGER
        )
    """)

    print("[schema] raw.orders, raw.order_items, raw.payments siap.")


# ─────────────────────────────────────────────
# DLQ
# ─────────────────────────────────────────────
def write_to_dlq(topic: str, partition: int, offset: int, raw_value: str, error: Exception):
    entry = {
        "topic":     topic,
        "partition": partition,
        "offset":    offset,
        "raw_value": raw_value,
        "error":     str(error),
        "failed_at": datetime.utcnow().isoformat(),
    }
    with open(DLQ_PATH, 'a', encoding='utf-8') as f:
        f.write(json.dumps(entry) + '\n')
    print(f"  [DLQ] {topic} | partition={partition} offset={offset} — {error}")


# ─────────────────────────────────────────────
# FLUSH
# ─────────────────────────────────────────────
def flush_batch(topic: str, batch: list):
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

        print(f"  [flush] {len(batch):>4} rows → {topic} [{datetime.utcnow().isoformat()}]")

    except Exception as e:
        print(f"  [ERROR] flush gagal untuk {topic}: {e}")


def flush_all(buffers: dict):
    for topic, batch in buffers.items():
        flush_batch(topic, batch)
        buffers[topic] = []


# ─────────────────────────────────────────────
# MAIN CONSUMER
# ─────────────────────────────────────────────
def run_consumer():
    print("=" * 45)
    print("  RetailX Kafka Consumer")
    print(f"  bootstrap : {KAFKA_BOOTSTRAP}")
    print(f"  group     : {GROUP_ID}")
    print(f"  topics    : {', '.join(TOPICS)}")
    print("=" * 45)

    init_schema()

    consumer = Consumer({
    'bootstrap.servers':        KAFKA_BOOTSTRAP,
    'group.id':                 GROUP_ID,
    'auto.offset.reset':        'earliest',
    'enable.auto.commit':       True,
    'fetch.min.bytes':          1048576,
    'fetch.wait.max.ms':        500,
    'max.partition.fetch.bytes': 5242880,
    })
    consumer.subscribe(TOPICS)

    buffers        = {t: [] for t in TOPICS}
    last_flush     = time.time()
    empty_polls    = 0
    total_consumed = 0

    print("[consumer] Mulai polling...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            # ── Tidak ada message ──────────────────────
            if msg is None:
                empty_polls += 1

                if (time.time() - last_flush) >= FLUSH_INTERVAL:
                    flush_all(buffers)
                    last_flush = time.time()

                if empty_polls >= MAX_EMPTY_POLLS:
                    print(f"[consumer] Idle {MAX_EMPTY_POLLS} poll berturut. Selesai.")
                    break

                continue

            # ── Error dari Kafka ───────────────────────
            if msg.error():
                raise KafkaException(msg.error())

            # ── Proses message ─────────────────────────
            empty_polls = 0  # reset idle counter
            topic       = msg.topic()

            try:
                data               = json.loads(msg.value().decode('utf-8'))
                data["_offset"]    = msg.offset()
                data["_partition"] = msg.partition()
                buffers[topic].append(data)
                total_consumed += 1

            except Exception as e:
                write_to_dlq(topic, msg.partition(), msg.offset(), str(msg.value()), e)
                continue

            # ── Flush jika batch penuh atau timeout ───
            if (len(buffers[topic]) >= BATCH_SIZE or
                    (time.time() - last_flush) >= FLUSH_INTERVAL):
                flush_all(buffers)
                last_flush = time.time()

    except KeyboardInterrupt:
        print("\n[consumer] Dihentikan manual (Ctrl+C).")

    finally:
        flush_all(buffers)  # pastikan sisa buffer tidak hilang
        consumer.close()
        conn.close()
        print(f"[consumer] Selesai. Total dikonsumsi: {total_consumed} messages.")


# ─────────────────────────────────────────────
if __name__ == "__main__":
    run_consumer()