"""Productor: lee el dataset limpio y publica cada fila como un evento JSON en Kafka."""
import argparse
import json
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_PATH = BASE_DIR / "data" / "processed" / "happiness_unified.csv"

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "happiness-events"


def build_event(index: int, row: pd.Series) -> dict:
    """Convierte una fila del dataset en un evento que cumple el contrato."""
    return {
        "event_id": f"E{index:04d}",
        "country": str(row["country"]),
        "year": int(row["year"]),
        "gdp": float(row["gdp"]),
        "family": float(row["family"]),
        "health": float(row["health"]),
        "freedom": float(row["freedom"]),
        "generosity": float(row["generosity"]),
        "corruption": float(row["corruption"]),
        "actual_happiness_score": float(row["score"]),
        "event_time": datetime.now().isoformat(timespec="seconds"),
    }


delivered = 0
failed = 0


def delivery_report(err, msg) -> None:
    """Se llama cuando Kafka confirma (o rechaza) cada mensaje."""
    global delivered, failed
    if err is not None:
        failed += 1
        print(f"ERROR al entregar: {err}")
    else:
        delivered += 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Productor de eventos de felicidad")
    parser.add_argument("--delay", type=float, default=0.5, help="segundos entre mensajes (default 0.5)")
    parser.add_argument("--limit", type=int, default=None, help="enviar solo los primeros N eventos")
    parser.add_argument("--dry-run", action="store_true", help="imprimir los eventos sin enviarlos a Kafka")
    args = parser.parse_args()

    df = pd.read_csv(DATA_PATH)
    if args.limit:
        df = df.head(args.limit)

    producer = None
    if not args.dry_run:
        from confluent_kafka import Producer
        producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    for index, (_, row) in enumerate(df.iterrows(), start=1):
        event = build_event(index, row)
        key = event["country"]
        value = json.dumps(event)

        if producer is None:
            print(f"[dry-run] key={key} | {value}")
        else:
            producer.produce(TOPIC, key=key, value=value, callback=delivery_report)
            producer.poll(0)
            print(f"En cola: key={key} | {value}")

        time.sleep(args.delay)

    if producer is not None:
        pendientes = producer.flush(15)
        print(f"Entregados: {delivered} | Fallidos: {failed} | Sin confirmar: {pendientes}")
        if pendientes or failed:
            print("AVISO: hay mensajes que no llegaron a Kafka. Revise que Kafka este corriendo.")
    print(f"Listo: {len(df)} eventos procesados.")


if __name__ == "__main__":
    main()