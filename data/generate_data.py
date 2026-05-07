"""
Gera dados sintéticos no tema UberEats e sobe pro MinIO.

Substitui o bucket público owshq-shadow-traffic-uber-eats que a demo
original do curso usa. Você roda isso uma vez (após `make up`) e tem
os mesmos datasets das demos do curso disponíveis no seu MinIO local.

Estrutura criada no bucket s3://raw-data/:
    raw-data/mysql/restaurants/restaurants.json
    raw-data/postgres/drivers/drivers.json
    raw-data/kafka/orders/orders.json
    raw-data/mongodb/users/users.json

Os "subsistemas" (mysql, postgres, kafka, mongodb) são fictícios mas
seguem o padrão da UberEats real do curso: cada fonte de dados é tratada
como se viesse de um sistema de origem diferente.

Uso (do host, fora dos containers):
    pip install boto3
    python data/generate_data.py
"""

import json
import random
import io
from datetime import datetime, timedelta

import boto3
from botocore.client import Config


# ─── Config MinIO (vista do HOST, não de dentro do container) ──
# Do host, MinIO está em localhost:9000. De dentro de um container
# Spark, está em minio:9000. Aqui rodamos do host.
MINIO_ENDPOINT = "http://127.0.0.1:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET = "raw-data"

random.seed(42)  # reproduzível


# ─── Geradores de dados ────────────────────────────────────────

def gen_restaurants(n=50):
    cuisines = ["Italian", "Japanese", "Brazilian", "Mexican", "Indian", "Chinese", "American"]
    cities = ["Sao Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba", "Porto Alegre"]
    return [
        {
            "restaurant_id": i,
            "name": f"Restaurant_{i}",
            "cuisine_type": random.choice(cuisines),
            "city": random.choice(cities),
            "average_rating": round(random.uniform(3.0, 5.0), 2),
            "num_reviews": random.randint(10, 5000),
            "phone_number": f"+55119{random.randint(10000000, 99999999)}",
            "dt_current_timestamp": datetime.utcnow().isoformat(),
        }
        for i in range(1, n + 1)
    ]


def gen_drivers(n=80):
    vehicle_types = ["car", "motorcycle", "bicycle"]
    makes = ["Toyota", "Honda", "Ford", "Yamaha", "Volkswagen"]
    cities = ["Sao Paulo", "Rio de Janeiro", "Belo Horizonte"]
    return [
        {
            "driver_id": i,
            "first_name": f"FirstName_{i}",
            "last_name": f"LastName_{i}",
            "license_number": f"LIC{random.randint(100000, 999999)}",
            "vehicle_type": random.choice(vehicle_types),
            "vehicle_make": random.choice(makes),
            "vehicle_model": f"Model_{random.randint(1, 20)}",
            "vehicle_year": random.randint(2010, 2024),
            "city": random.choice(cities),
            "country": "Brazil",
            "date_birth": (datetime(1980, 1, 1) + timedelta(days=random.randint(0, 365 * 25))).strftime("%Y-%m-%d"),
            "phone_number": f"+55119{random.randint(10000000, 99999999)}",
            "uuid": f"uuid-{i:06d}",
            "dt_current_timestamp": datetime.utcnow().isoformat(),
        }
        for i in range(1, n + 1)
    ]


def gen_orders(n=200):
    statuses = ["pending", "confirmed", "delivered", "cancelled"]
    payments = ["credit_card", "pix", "cash"]
    return [
        {
            "order_id": i,
            "user_id": random.randint(1, 100),
            "restaurant_id": random.randint(1, 50),
            "driver_id": random.randint(1, 80),
            "total_amount": round(random.uniform(20.0, 250.0), 2),
            "order_status": random.choice(statuses),
            "order_date": (datetime(2025, 1, 1) + timedelta(days=random.randint(0, 300))).isoformat(),
            "payment_method": random.choice(payments),
        }
        for i in range(1, n + 1)
    ]


def gen_users(n=100):
    cities = ["Sao Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba"]
    return [
        {
            "user_id": i,
            "email": f"user_{i}@uber.com",
            "first_name": f"User_{i}",
            "last_name": f"Last_{i}",
            "city": random.choice(cities),
            "country": "Brazil",
            "phone_number": f"+55119{random.randint(10000000, 99999999)}",
            "registration_date": (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 700))).isoformat(),
        }
        for i in range(1, n + 1)
    ]


# ─── Upload pro MinIO ──────────────────────────────────────────

def to_jsonl(rows):
    """Serializa lista de dicts como JSON Lines (1 objeto por linha).
    Esse é o formato que o Spark lê com spark.read.json()."""
    return "\n".join(json.dumps(r) for r in rows).encode("utf-8")


def main():
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    # Garante que o bucket existe (o mc-init já cria, mas é defensivo)
    try:
        s3.head_bucket(Bucket=BUCKET)
    except Exception:
        print(f"Bucket {BUCKET} não encontrado. Subiu o ambiente com 'make up'?")
        raise

    datasets = [
        ("mysql/restaurants/restaurants.json", gen_restaurants()),
        ("postgres/drivers/drivers.json", gen_drivers()),
        ("kafka/orders/orders.json", gen_orders()),
        ("mongodb/users/users.json", gen_users()),
    ]

    for key, rows in datasets:
        body = to_jsonl(rows)
        s3.put_object(Bucket=BUCKET, Key=key, Body=body)
        print(f"  s3a://{BUCKET}/{key}  ({len(rows)} rows, {len(body)} bytes)")

    print("\nDados sintéticos prontos. Pode rodar as demos.")


if __name__ == "__main__":
    main()
