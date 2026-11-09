"""
Workshop-3 — Step 6: Kafka Consumer & Real-Time ML Inference
Consumes streaming events from 'happiness-events' (consumer group 'happiness-ml-team'):
1. Stores the raw incoming Kafka message in 'raw_happiness_events' BEFORE transformation/prediction
   with idempotency on (kafka_topic, kafka_partition, kafka_offset).
2. Validates the incoming event against the defined Event Contract.
3. Applies the exact feature ordering and preprocessing pipeline ('models/model.pkl').
4. Atomically updates raw status to 'VALID' and upserts prediction into 'fact_predictions'
   (idempotent on 'event_id') within a single database transaction, wrapped in try/except.
5. Manually commits Kafka offsets only AFTER database persistence succeeds.
"""

import argparse
import json
import math
import os
import pickle
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg2
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

TOPIC_NAME = "happiness-events"
CONSUMER_GROUP = "happiness-ml-team"
NUM_PARTITIONS = 3
REPLICATION_FACTOR = 1

BASE_DIR = Path(__file__).resolve().parent.parent
MODEL_PATH = BASE_DIR / "models" / "model.pkl"
SCHEMA_SQL_PATH = BASE_DIR / "sql" / "create_tables.sql"

MODEL_FEATURES = ["gdp", "family", "health", "freedom", "generosity", "corruption"]
REQUIRED_CONTRACT_FIELDS = [
    "event_id",
    "country",
    "year",
    *MODEL_FEATURES,
    "actual_happiness_score",
    "event_time",
]


def get_pg_connection():
    """Connect to the PostgreSQL analytical database."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "happiness_db"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
    )


def initialize_database(conn) -> None:
    """Ensure all required tables, constraints, and views exist before consuming events."""
    if SCHEMA_SQL_PATH.exists():
        sql_script = SCHEMA_SQL_PATH.read_text(encoding="utf-8")
        with conn.cursor() as cur:
            cur.execute(sql_script)
        conn.commit()
        print("[Consumer] Verified PostgreSQL schema ('raw_happiness_events', 'dim_country', 'dim_date', 'fact_predictions').")


def load_serialized_model(model_path: Path):
    """Load the pre-trained scikit-learn Pipeline (Imputer + StandardScaler + Regressor)."""
    if not model_path.exists():
        raise FileNotFoundError(f"Serialized model not found at {model_path}")
    with open(model_path, "rb") as f:
        pipeline = pickle.load(f)
    print(f"[Consumer] Loaded serialized ML pipeline from {model_path}")
    return pipeline


def validate_event_contract(raw_payload: str) -> tuple[str, dict[str, Any] | None, str | None]:
    """
    Validate incoming raw JSON payload against the Event Contract.
    Returns: (status, parsed_event_or_None, error_description_or_None)
    Possible statuses: 'VALID', 'INVALID_SCHEMA', 'INVALID_VALUES'
    """
    # 1. Parse JSON
    try:
        payload = json.loads(raw_payload)
    except Exception as exc:
        return "INVALID_SCHEMA", None, f"Malformed JSON payload: {exc}"

    if not isinstance(payload, dict):
        return "INVALID_SCHEMA", None, "Payload root must be a JSON object."

    # 2. Check missing contract fields & missing model features
    missing_fields = [f for f in REQUIRED_CONTRACT_FIELDS if f not in payload]
    if missing_fields:
        missing_features = [f for f in MODEL_FEATURES if f not in payload]
        detail = f"Missing required fields: {missing_fields}"
        if missing_features:
            detail += f" (Missing model features: {missing_features})"
        return "INVALID_SCHEMA", payload, detail

    # 3. Validate data types
    if not isinstance(payload["event_id"], str):
        return "INVALID_SCHEMA", payload, "Field 'event_id' must be a string."
    if not isinstance(payload["country"], str):
        return "INVALID_SCHEMA", payload, "Field 'country' must be a string."
    if isinstance(payload["year"], bool) or not isinstance(payload["year"], int):
        return "INVALID_SCHEMA", payload, "Field 'year' must be an integer."
    if not isinstance(payload["event_time"], str):
        return "INVALID_SCHEMA", payload, "Field 'event_time' must be an ISO-8601 string."

    for num_field in [*MODEL_FEATURES, "actual_happiness_score"]:
        if isinstance(payload[num_field], bool) or not isinstance(payload[num_field], (int, float)):
            return (
                "INVALID_SCHEMA",
                payload,
                f"Field '{num_field}' has invalid data type ({type(payload[num_field]).__name__}); expected numeric.",
            )

    # 4. Validate business values and numerical bounds
    if not payload["event_id"].strip():
        return "INVALID_VALUES", payload, "Field 'event_id' must be a non-empty string."
    if not payload["country"].strip():
        return "INVALID_VALUES", payload, "Field 'country' must be a non-empty string."

    year_val = int(payload["year"])
    if year_val < 1900 or year_val > 2100:
        return "INVALID_VALUES", payload, f"Field 'year' ({year_val}) is outside valid range [1900, 2100]."

    for feat in MODEL_FEATURES:
        val = float(payload[feat])
        if math.isnan(val) or math.isinf(val):
            return "INVALID_VALUES", payload, f"Feature '{feat}' contains NaN or Infinity."
        if val < 0.0 or val > 10.0:
            return "INVALID_VALUES", payload, f"Feature '{feat}' ({val}) is outside valid range [0.0, 10.0]."

    actual_score = float(payload["actual_happiness_score"])
    if math.isnan(actual_score) or math.isinf(actual_score) or actual_score <= 0.0 or actual_score > 10.0:
        return (
            "INVALID_VALUES",
            payload,
            f"Field 'actual_happiness_score' ({actual_score}) is outside valid range (0.0, 10.0].",
        )

    try:
        datetime.fromisoformat(payload["event_time"].replace("Z", "+00:00"))
    except ValueError as exc:
        return "INVALID_VALUES", payload, f"Field 'event_time' is not a valid ISO-8601 timestamp: {exc}"

    return "VALID", payload, None


def insert_raw_event_before_processing(
    conn,
    topic: str,
    partition: int,
    offset: int,
    event_key: str | None,
    raw_payload: str,
) -> int:
    """
    Step 6.2 Requirement: Store the original incoming Kafka message into
    'raw_happiness_events' BEFORE applying any transformation or prediction.
    Idempotent on (kafka_topic, kafka_partition, kafka_offset).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw_happiness_events (
                kafka_topic, kafka_partition, kafka_offset, event_key,
                raw_payload, processing_status
            )
            VALUES (%s, %s, %s, %s, %s, 'RECEIVED')
            ON CONFLICT (kafka_topic, kafka_partition, kafka_offset)
            DO UPDATE SET
                raw_payload = EXCLUDED.raw_payload,
                ingested_at = CURRENT_TIMESTAMP
            RETURNING raw_event_id;
            """,
            (topic, partition, offset, event_key, raw_payload),
        )
        raw_event_id = cur.fetchone()[0]
    conn.commit()
    return raw_event_id


def update_raw_event_status(
    conn,
    raw_event_id: int,
    status: str,
    event_id: str | None = None,
    event_time_iso: str | None = None,
    validation_errors: str | None = None,
) -> None:
    """Update the raw event record with its validation/error status."""
    parsed_event_time = None
    if event_time_iso and isinstance(event_time_iso, str):
        try:
            parsed_event_time = datetime.fromisoformat(event_time_iso.replace("Z", "+00:00"))
        except ValueError:
            parsed_event_time = None

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE raw_happiness_events
            SET event_id = COALESCE(%s, event_id),
                event_time = COALESCE(%s, event_time),
                processing_status = %s,
                validation_errors = %s
            WHERE raw_event_id = %s;
            """,
            (event_id, parsed_event_time, status, validation_errors, raw_event_id),
        )
    conn.commit()


def commit_valid_prediction_transaction(
    conn,
    raw_event_id: int,
    event: dict[str, Any],
    predicted_score: float,
    prediction_error: float,
) -> None:
    """
    Atomically update raw_happiness_events to 'VALID', upsert dimensions ('dim_country',
    'dim_date'), and upsert 'fact_predictions' (idempotent on 'event_id') in a single transaction.
    """
    country_name = event["country"].strip()
    obs_year = int(event["year"])
    decade_str = f"{(obs_year // 10) * 10}s"
    event_time_dt = datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
    is_test_set = bool(event.get("is_test_set", False))

    with conn.cursor() as cur:
        # 1. Update raw_happiness_events status to VALID inside the same transaction
        cur.execute(
            """
            UPDATE raw_happiness_events
            SET event_id = %s,
                event_time = %s,
                processing_status = 'VALID',
                validation_errors = NULL
            WHERE raw_event_id = %s;
            """,
            (event["event_id"].strip(), event_time_dt, raw_event_id),
        )

        # 2. Upsert dim_country
        cur.execute(
            """
            INSERT INTO dim_country (country_name)
            VALUES (%s)
            ON CONFLICT (country_name) DO UPDATE SET country_name = EXCLUDED.country_name
            RETURNING country_id;
            """,
            (country_name,),
        )
        country_id = cur.fetchone()[0]

        # 3. Upsert dim_date
        cur.execute(
            """
            INSERT INTO dim_date (observation_year, decade)
            VALUES (%s, %s)
            ON CONFLICT (observation_year) DO UPDATE SET decade = EXCLUDED.decade
            RETURNING date_id;
            """,
            (obs_year, decade_str),
        )
        date_id = cur.fetchone()[0]

        # 4. Upsert into fact_predictions (idempotent on event_id so re-running producer updates cleanly)
        cur.execute(
            """
            INSERT INTO fact_predictions (
                raw_event_id, event_id, country_id, date_id,
                gdp, family, health, freedom, generosity, corruption,
                actual_score, predicted_score, prediction_error, is_test_set, event_time
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                raw_event_id = EXCLUDED.raw_event_id,
                country_id = EXCLUDED.country_id,
                date_id = EXCLUDED.date_id,
                gdp = EXCLUDED.gdp,
                family = EXCLUDED.family,
                health = EXCLUDED.health,
                freedom = EXCLUDED.freedom,
                generosity = EXCLUDED.generosity,
                corruption = EXCLUDED.corruption,
                actual_score = EXCLUDED.actual_score,
                predicted_score = EXCLUDED.predicted_score,
                prediction_error = EXCLUDED.prediction_error,
                is_test_set = EXCLUDED.is_test_set,
                event_time = EXCLUDED.event_time,
                prediction_timestamp = CURRENT_TIMESTAMP;
            """,
            (
                raw_event_id,
                event["event_id"].strip(),
                country_id,
                date_id,
                float(event["gdp"]),
                float(event["family"]),
                float(event["health"]),
                float(event["freedom"]),
                float(event["generosity"]),
                float(event["corruption"]),
                float(event["actual_happiness_score"]),
                predicted_score,
                prediction_error,
                is_test_set,
                event_time_dt,
            ),
        )
    conn.commit()


def ensure_topic_exists(bootstrap_servers: str, topic_name: str = TOPIC_NAME) -> None:
    """Create the Kafka topic with 3 partitions and replication factor 1 if missing."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    metadata = admin.list_topics(timeout=10)
    if topic_name in metadata.topics:
        return

    new_topic = NewTopic(
        topic=topic_name,
        num_partitions=NUM_PARTITIONS,
        replication_factor=REPLICATION_FACTOR,
    )
    fs = admin.create_topics([new_topic])
    for topic, future in fs.items():
        try:
            future.result(timeout=15)
            print(
                f"[Consumer] Created topic '{topic}' with {NUM_PARTITIONS} partitions "
                f"and replication factor {REPLICATION_FACTOR}."
            )
        except Exception:
            pass


def run_consumer(
    bootstrap_servers: str,
    max_messages: int | None = None,
    idle_timeout: float | None = None,
) -> None:
    conn = get_pg_connection()
    initialize_database(conn)
    model_pipeline = load_serialized_model(MODEL_PATH)
    ensure_topic_exists(bootstrap_servers, TOPIC_NAME)

    # Manual offset commit (enable.auto.commit=False) prevents message loss if DB fails
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC_NAME])

    print(
        f"[Consumer] Listening on topic '{TOPIC_NAME}' with group '{CONSUMER_GROUP}' "
        f"(bootstrap={bootstrap_servers}, manual offset commit enabled)..."
    )

    processed_total = 0
    status_counts = {
        "VALID": 0,
        "INVALID_SCHEMA": 0,
        "INVALID_VALUES": 0,
        "PREDICTION_ERROR": 0,
    }
    last_message_ts = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if idle_timeout is not None and (time.time() - last_message_ts) >= idle_timeout:
                    print(f"[Consumer] Reached idle timeout ({idle_timeout}s). Stopping consumer.")
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[Consumer] Kafka error: {msg.error()}")
                continue

            last_message_ts = time.time()
            raw_bytes = msg.value()
            raw_payload_str = raw_bytes.decode("utf-8", errors="replace") if raw_bytes else ""
            key_bytes = msg.key()
            event_key_str = key_bytes.decode("utf-8", errors="replace") if key_bytes else None

            # 1. Store raw incoming event BEFORE validation or prediction
            try:
                raw_event_id = insert_raw_event_before_processing(
                    conn=conn,
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    event_key=event_key_str,
                    raw_payload=raw_payload_str,
                )
            except Exception as db_exc:
                conn.rollback()
                print(f"[Consumer] Database error storing raw event (offset not committed): {db_exc}")
                continue

            # 2. Validate against Event Contract
            status, parsed_event, error_msg = validate_event_contract(raw_payload_str)
            ev_id = parsed_event.get("event_id") if isinstance(parsed_event, dict) else None
            ev_time = parsed_event.get("event_time") if isinstance(parsed_event, dict) else None

            if status != "VALID":
                update_raw_event_status(
                    conn=conn,
                    raw_event_id=raw_event_id,
                    status=status,
                    event_id=str(ev_id) if ev_id is not None else None,
                    event_time_iso=str(ev_time) if ev_time is not None else None,
                    validation_errors=error_msg,
                )
                consumer.commit(message=msg, asynchronous=False)
                status_counts[status] = status_counts.get(status, 0) + 1
                processed_total += 1
                print(
                    f"[Consumer] Skipped invalid event (raw_event_id={raw_event_id}, "
                    f"status={status}): {error_msg}"
                )
                if max_messages and processed_total >= max_messages:
                    break
                continue

            # 3. Prepare features, run inference & atomically persist VALID status + fact row
            try:
                if parsed_event.get("simulate_prediction_error") is True:
                    raise RuntimeError("Simulated model inference fault triggered via 'simulate_prediction_error=True'.")

                features_df = pd.DataFrame(
                    [{feat: float(parsed_event[feat]) for feat in MODEL_FEATURES}],
                    columns=MODEL_FEATURES,
                )
                predicted_score = round(float(model_pipeline.predict(features_df)[0]), 6)
                actual_score = float(parsed_event["actual_happiness_score"])
                prediction_error = round(abs(actual_score - predicted_score), 6)

                commit_valid_prediction_transaction(
                    conn=conn,
                    raw_event_id=raw_event_id,
                    event=parsed_event,
                    predicted_score=predicted_score,
                    prediction_error=prediction_error,
                )
                consumer.commit(message=msg, asynchronous=False)
            except Exception as pred_exc:
                conn.rollback()
                update_raw_event_status(
                    conn=conn,
                    raw_event_id=raw_event_id,
                    status="PREDICTION_ERROR",
                    event_id=str(parsed_event.get("event_id")),
                    event_time_iso=str(parsed_event.get("event_time")),
                    validation_errors=f"Inference/Persistence failure: {pred_exc}",
                )
                consumer.commit(message=msg, asynchronous=False)
                status_counts["PREDICTION_ERROR"] += 1
                processed_total += 1
                print(f"[Consumer] Logged PREDICTION_ERROR on raw_event_id={raw_event_id}: {pred_exc}")
                if max_messages and processed_total >= max_messages:
                    break
                continue

            status_counts["VALID"] += 1
            processed_total += 1

            if processed_total == 1 or processed_total % 100 == 0:
                print(
                    f"[Consumer] Processed {processed_total} | event_id={parsed_event['event_id']} | "
                    f"country={parsed_event['country']} ({parsed_event['year']}) | "
                    f"actual={actual_score:.3f} | pred={predicted_score:.3f} | err={prediction_error:.3f}"
                )

            if max_messages and processed_total >= max_messages:
                print(f"[Consumer] Reached max_messages={max_messages}. Stopping.")
                break

    except KeyboardInterrupt:
        print("\n[Consumer] Interrupted by user. Shutting down cleanly...")
    finally:
        consumer.close()
        conn.close()
        print(f"[Consumer] Summary -> Total: {processed_total} | Breakdown: {status_counts}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka Consumer for Real-Time Happiness ML Inference")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Stop after consuming N messages (optional)",
    )
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=None,
        help="Stop if no new message arrives within N seconds (optional)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_consumer(
        bootstrap_servers=args.bootstrap_servers,
        max_messages=args.max_messages,
        idle_timeout=args.idle_timeout,
    )
