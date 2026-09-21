import csv
import io
import json
import os
import time
from datetime import datetime, timezone

import boto3
import psycopg
from botocore.config import Config
from confluent_kafka import Consumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
EMPLOYEES_TOPIC = os.getenv("EMPLOYEES_TOPIC", "sport_data.raw.employees")
SPORTS_TOPIC = os.getenv("EMPLOYEE_SPORTS_TOPIC", "sport_data.raw.employee_sports")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://sport_user:SportData2026Local@db:5432/sport_data")
SOURCE_BUCKET = os.getenv("S3_REFERENCE_SOURCE_BUCKET", "reference-source")
CURRENT_BUCKET = os.getenv("S3_REFERENCE_BUCKET", "references")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin123")
DEBOUNCE_SECONDS = float(os.getenv("REFERENCE_SNAPSHOT_DEBOUNCE_SECONDS", "2"))

TABLES = {
    EMPLOYEES_TOPIC: {
        "table": "raw.employees",
        "order_by": "employee_id",
        "archive_prefix": "company/employees",
        "current_key": "company/employees.csv",
    },
    SPORTS_TOPIC: {
        "table": "raw.employee_sports",
        "order_by": "employee_id",
        "archive_prefix": "sports/employee_sports",
        "current_key": "sports/employee_sports.csv",
    },
}

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}),
)


def ensure_bucket(name: str) -> None:
    try:
        s3.head_bucket(Bucket=name)
    except Exception:
        s3.create_bucket(Bucket=name)


def snapshot_reference(cfg: dict) -> str:
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {cfg['table']} ORDER BY {cfg['order_by']}")
            columns = [d.name for d in cur.description]
            rows = cur.fetchall()

    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(columns)
    writer.writerows(rows)
    payload = output.getvalue().encode("utf-8")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    archive_key = f"{cfg['archive_prefix']}_{stamp}.csv"

    # Historique immuable d'abord, puis publication de la version courante.
    s3.put_object(Bucket=SOURCE_BUCKET, Key=archive_key, Body=payload, ContentType="text/csv; charset=utf-8")
    s3.put_object(Bucket=CURRENT_BUCKET, Key=cfg["current_key"], Body=payload, ContentType="text/csv; charset=utf-8")
    print(f"[reference-snapshot] {cfg['table']} -> s3://{SOURCE_BUCKET}/{archive_key}", flush=True)
    print(f"[reference-current]  {cfg['table']} -> s3://{CURRENT_BUCKET}/{cfg['current_key']}", flush=True)
    return archive_key


def main() -> None:
    ensure_bucket(SOURCE_BUCKET)
    ensure_bucket(CURRENT_BUCKET)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "reference-snapshotter-v1",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(list(TABLES))
    print(f"[reference-watcher] écoute {', '.join(TABLES)}", flush=True)

    dirty = set()
    last_change = None
    try:
        while True:
            msg = consumer.poll(0.5)
            if msg is not None:
                if msg.error():
                    print(f"[reference-watcher] Kafka: {msg.error()}", flush=True)
                elif msg.topic() in TABLES:
                    # Le contenu CDC n'est pas utilisé pour fabriquer le CSV :
                    # Debezium sert de signal, puis on relit la table complète dans PostgreSQL.
                    try:
                        json.loads(msg.value().decode("utf-8")) if msg.value() else None
                    except Exception:
                        pass
                    dirty.add(msg.topic())
                    last_change = time.monotonic()

            if dirty and last_change is not None and time.monotonic() - last_change >= DEBOUNCE_SECONDS:
                for topic in sorted(dirty):
                    snapshot_reference(TABLES[topic])
                consumer.commit(asynchronous=False)
                dirty.clear()
                last_change = None
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
