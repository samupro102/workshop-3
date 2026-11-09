"""Consumidor: recibe eventos de Kafka, guarda el crudo, valida el contrato, predice y guarda el resultado."""
import argparse
import json
import math
import os
from datetime import datetime
from pathlib import Path

import joblib
import pandas as pd
import psycopg2
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
MODEL_PATH = BASE_DIR / "models" / "model.pkl"

# Lee las credenciales de la base de datos desde el archivo .env de la raiz del proyecto
load_dotenv(BASE_DIR / ".env")

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "happiness-events"
GROUP_ID = "happiness-ml-team"

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "happiness_db"),
    "user": os.getenv("DB_USER", "happiness_user"),
    "password": os.getenv("DB_PASSWORD"),
}

# Mismo orden que en el entrenamiento (model_training.ipynb)
FEATURES = ["gdp", "family", "health", "freedom", "generosity", "corruption"]
REQUIRED_FIELDS = ["event_id", "country", "year", *FEATURES, "actual_happiness_score", "event_time"]


# ---------------------------------------------------------------- validación
def is_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate_event(event):
    """Valida el evento contra el contrato. Devuelve (estado, motivo)."""
    if not isinstance(event, dict):
        return "INVALID_SCHEMA", "El mensaje no es un objeto JSON"

    # 1) Esquema: campos presentes, no vacíos y con el tipo correcto
    for field in REQUIRED_FIELDS:
        if field not in event:
            return "INVALID_SCHEMA", f"Falta el campo: {field}"
        if event[field] is None or (isinstance(event[field], str) and not event[field].strip()):
            return "INVALID_SCHEMA", f"Campo vacio: {field}"

    for field in ("event_id", "country", "event_time"):
        if not isinstance(event[field], str):
            return "INVALID_SCHEMA", f"{field} debe ser texto"

    if not isinstance(event["year"], int) or isinstance(event["year"], bool):
        return "INVALID_SCHEMA", "year debe ser un entero"

    for field in (*FEATURES, "actual_happiness_score"):
        if not is_number(event[field]):
            return "INVALID_SCHEMA", f"{field} debe ser numerico"

    # 2) Valores: rangos y formato
    if not 1900 <= event["year"] <= 2100:
        return "INVALID_VALUES", f"year fuera de rango: {event['year']}"

    for field in FEATURES:
        value = event[field]
        if not math.isfinite(value) or value < 0:
            return "INVALID_VALUES", f"{field} debe ser un numero finito >= 0 (llego {value})"

    score = event["actual_happiness_score"]
    if not math.isfinite(score) or not 0 <= score <= 10:
        return "INVALID_VALUES", f"actual_happiness_score debe estar entre 0 y 10 (llego {score})"

    try:
        datetime.fromisoformat(event["event_time"])
    except ValueError:
        return "INVALID_VALUES", "event_time no es un timestamp ISO-8601 valido"

    return "VALID", "OK"


# ---------------------------------------------------------------- predicción
def predict_score(model, event) -> float:
    """Arma las features en el mismo orden del entrenamiento y predice."""
    X = pd.DataFrame([[event[f] for f in FEATURES]], columns=FEATURES)
    return float(model.predict(X)[0])


# ---------------------------------------------------------------- base de datos
def set_status(conn, raw_event_id, status, reason=None, event_id=None):
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE raw_happiness_events
               SET processing_status = %s, error_message = %s, event_id = COALESCE(%s, event_id)
               WHERE raw_event_id = %s""",
            (status, reason, event_id, raw_event_id),
        )
    conn.commit()


def store_prediction(conn, raw_event_id, event, predicted):
    """Guarda país, fecha y la predicción; marca el evento crudo como VALID (una sola transacción)."""
    event_dt = datetime.fromisoformat(event["event_time"])
    date_id = int(event_dt.strftime("%Y%m%d"))
    actual = event["actual_happiness_score"]

    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO dim_country (country_name) VALUES (%s)
               ON CONFLICT (country_name) DO UPDATE SET country_name = EXCLUDED.country_name
               RETURNING country_id""",
            (event["country"],),
        )
        country_id = cur.fetchone()[0]

        cur.execute(
            """INSERT INTO dim_date (date_id, full_date, year, month, day)
               VALUES (%s, %s, %s, %s, %s) ON CONFLICT (date_id) DO NOTHING""",
            (date_id, event_dt.date(), event_dt.year, event_dt.month, event_dt.day),
        )

        cur.execute(
            """INSERT INTO fact_predictions
               (raw_event_id, country_id, date_id, year, actual_score, predicted_score,
                prediction_error, event_time)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (raw_event_id, country_id, date_id, event["year"], actual, predicted,
             predicted - actual, event_dt),
        )

        cur.execute(
            """UPDATE raw_happiness_events
               SET processing_status = 'VALID', error_message = NULL, event_id = %s
               WHERE raw_event_id = %s""",
            (event["event_id"], raw_event_id),
        )
    conn.commit()


# ---------------------------------------------------------------- procesamiento
def process_message(conn, model, topic, partition, offset, key, raw_bytes):
    """Procesa un mensaje. Nunca lanza excepciones por datos malos. Devuelve (estado, detalle)."""
    raw_text = raw_bytes.decode("utf-8", errors="replace").replace("\x00", "�")

    # 1) Guardar el mensaje original ANTES de validar o transformar
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO raw_happiness_events
               (kafka_topic, kafka_partition, kafka_offset, kafka_key, raw_payload)
               VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
               RETURNING raw_event_id""",
            (topic, partition, offset, key, raw_text),
        )
        row = cur.fetchone()
        if row is None:  # el mensaje ya se habia guardado (reproceso)
            cur.execute(
                """SELECT raw_event_id, processing_status FROM raw_happiness_events
                   WHERE kafka_topic = %s AND kafka_partition = %s AND kafka_offset = %s""",
                (topic, partition, offset),
            )
            raw_event_id, status = cur.fetchone()
            conn.commit()
            if status != "RECEIVED":
                return status, "ya procesado antes (se omite)"
        else:
            raw_event_id = row[0]
            conn.commit()

    # 2) Leer el JSON
    try:
        event = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        set_status(conn, raw_event_id, "INVALID_SCHEMA", f"JSON invalido: {exc}")
        return "INVALID_SCHEMA", f"JSON invalido: {exc}"

    # 3) Validar el contrato
    status, reason = validate_event(event)
    if status != "VALID":
        event_id = event.get("event_id") if isinstance(event, dict) and isinstance(event.get("event_id"), str) else None
        set_status(conn, raw_event_id, status, reason, event_id)
        return status, reason

    # 4) Predecir y guardar
    try:
        predicted = predict_score(model, event)
        store_prediction(conn, raw_event_id, event, predicted)
    except Exception as exc:  # noqa: BLE001 - un fallo aqui no debe tumbar el pipeline
        conn.rollback()
        set_status(conn, raw_event_id, "PREDICTION_ERROR", str(exc), event["event_id"])
        return "PREDICTION_ERROR", str(exc)

    actual = event["actual_happiness_score"]
    return "VALID", (f"{event['country']} {event['year']} | real={actual:.3f} "
                     f"predicho={predicted:.3f} error={predicted - actual:+.3f}")


# ---------------------------------------------------------------- bucle principal
def main() -> None:
    parser = argparse.ArgumentParser(description="Consumidor de eventos de felicidad")
    parser.add_argument("--max-messages", type=int, default=None, help="detenerse tras procesar N mensajes")
    args = parser.parse_args()

    from confluent_kafka import Consumer

    model = joblib.load(MODEL_PATH)
    conn = psycopg2.connect(**DB_CONFIG)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # se confirma el offset solo despues de guardar
    })
    consumer.subscribe([TOPIC])

    counts = {}
    processed = 0
    print(f"Esperando eventos en '{TOPIC}' (grupo {GROUP_ID})... Ctrl+C para detener.")

    try:
        while args.max_messages is None or processed < args.max_messages:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                print(f"Error del consumidor: {message.error()}")
                continue

            key = message.key().decode("utf-8", errors="replace") if message.key() else None
            status, detail = process_message(
                conn, model, message.topic(), message.partition(), message.offset(), key, message.value() or b""
            )
            consumer.commit(message=message, asynchronous=False)

            processed += 1
            counts[status] = counts.get(status, 0) + 1
            print(f"[{status}] p{message.partition()} o{message.offset()} | {detail}")
    except KeyboardInterrupt:
        print("\nConsumidor detenido.")
    finally:
        consumer.close()
        conn.close()
        print(f"Resumen: {processed} mensajes | {counts}")


if __name__ == "__main__":
    main()