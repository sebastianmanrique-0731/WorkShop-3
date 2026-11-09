"""
Workshop-3 — Step 5: Kafka Producer
Reads prepared historical World Happiness records one by one and publishes each
record as a simulated real-time JSON event to the 'happiness-events' Kafka topic.
"""

import argparse
import json
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

TOPIC_NAME = "happiness-events"
NUM_PARTITIONS = 3
REPLICATION_FACTOR = 1

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_CSV_PATH = BASE_DIR / "data" / "processed" / "happiness_unified.csv"
STREAM_SNAPSHOT_PATH = BASE_DIR / "data" / "streaming" / "happiness_events_stream.jsonl"


def ensure_topic_exists(bootstrap_servers: str, topic_name: str = TOPIC_NAME) -> None:
    """Create the Kafka topic with 3 partitions and replication factor 1 if missing."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    metadata = admin.list_topics(timeout=10)
    if topic_name in metadata.topics:
        part_count = len(metadata.topics[topic_name].partitions)
        print(f"[Producer] Topic '{topic_name}' already exists ({part_count} partitions).")
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
                f"[Producer] Created topic '{topic}' with {NUM_PARTITIONS} partitions "
                f"and replication factor {REPLICATION_FACTOR}."
            )
        except Exception as exc:
            print(f"[Producer] Topic creation note for '{topic}': {exc}")


def build_event_payload(row: pd.Series, index_num: int) -> dict:
    """Build the JSON event matching the required Part B Event Contract."""
    payload = {
        "event_id": f"E{index_num:03d}",
        "country": str(row["country"]).strip(),
        "year": int(row["year"]),
        "gdp": round(float(row["gdp"]), 6),
        "family": round(float(row["family"]), 6),
        "health": round(float(row["health"]), 6),
        "freedom": round(float(row["freedom"]), 6),
        "generosity": round(float(row["generosity"]), 6),
        "corruption": round(float(row["corruption"]), 6),
        "actual_happiness_score": round(float(row["happiness_score"]), 6),
        "event_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }
    if "is_test_set" in row:
        payload["is_test_set"] = bool(row["is_test_set"])
    return payload


def get_invalid_test_events() -> list[tuple[str, str]]:
    """
    Sample malformed/faulty events to test consumer Event Contract validation & error handling:
    1. INVALID_SCHEMA: missing model features ('health', 'freedom') and wrong type for 'gdp'.
    2. INVALID_VALUES: negative 'gdp' (-1.5) and out-of-range 'actual_happiness_score' (99.9).
    3. INVALID_SCHEMA: malformed raw JSON string.
    4. PREDICTION_ERROR: valid contract schema/values but includes a fault-injection flag
       ('simulate_prediction_error': True) to verify that runtime inference errors are
       caught, logged as PREDICTION_ERROR in raw_happiness_events, and do not crash the consumer.
    """
    now_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    invalid_schema_event = json.dumps({
        "event_id": "E_BAD_SCHEMA_01",
        "country": "Colombia",
        "year": 2019,
        "gdp": "not_a_number",
        "family": 0.8,
        "generosity": 0.3,
        "corruption": 0.1,
        "actual_happiness_score": 6.2,
        "event_time": now_iso,
    })
    invalid_values_event = json.dumps({
        "event_id": "E_BAD_VALUES_01",
        "country": "Colombia",
        "year": 2019,
        "gdp": -1.5,
        "family": 0.8,
        "health": 0.9,
        "freedom": 0.6,
        "generosity": 0.3,
        "corruption": 0.1,
        "actual_happiness_score": 99.9,
        "event_time": now_iso,
    })
    malformed_json_event = '{"event_id": "E_MALFORMED_JSON", "country": "Peru", BROKEN_JSON'
    prediction_error_event = json.dumps({
        "event_id": "E_SIM_PRED_ERROR_01",
        "country": "Colombia",
        "year": 2019,
        "gdp": 1.0,
        "family": 1.2,
        "health": 0.8,
        "freedom": 0.5,
        "generosity": 0.2,
        "corruption": 0.1,
        "actual_happiness_score": 6.1,
        "event_time": now_iso,
        "simulate_prediction_error": True,
    })
    return [
        ("Colombia", invalid_schema_event),
        ("Colombia", invalid_values_event),
        ("Peru", malformed_json_event),
        ("Colombia", prediction_error_event),
    ]


def delivery_callback(err, msg) -> None:
    if err is not None:
        print(f"[Producer] Delivery failed for key={msg.key()}: {err}")


def run_producer(
    bootstrap_servers: str,
    csv_path: Path,
    delay: float,
    limit: int | None = None,
    inject_invalid: bool = False,
) -> None:
    if not csv_path.exists():
        raise FileNotFoundError(f"Prepared dataset not found at {csv_path}")

    ensure_topic_exists(bootstrap_servers, TOPIC_NAME)

    producer = Producer({
        "bootstrap.servers": bootstrap_servers,
        "client.id": "happiness-event-producer",
        "acks": "all",
    })

    df = pd.read_csv(csv_path)
    if limit is not None and limit > 0:
        df = df.head(limit)

    STREAM_SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    print(f"[Producer] Streaming {len(df)} historical records to topic '{TOPIC_NAME}' (delay={delay}s)...")

    sent_count = 0
    with open(STREAM_SNAPSHOT_PATH, "w", encoding="utf-8") as snapshot_file:
        for idx, (_, row) in enumerate(df.iterrows(), start=1):
            event = build_event_payload(row, idx)
            payload_json = json.dumps(event)
            event_key = event["country"].encode("utf-8")

            producer.produce(
                topic=TOPIC_NAME,
                key=event_key,
                value=payload_json.encode("utf-8"),
                callback=delivery_callback,
            )
            producer.poll(0)
            snapshot_file.write(payload_json + "\n")
            sent_count += 1

            if sent_count == 1 or sent_count % 100 == 0 or sent_count == len(df):
                print(
                    f"[Producer] Published {sent_count}/{len(df)} | "
                    f"event_id={event['event_id']} | key(country)={event['country']} | year={event['year']}"
                )

            if delay > 0:
                time.sleep(delay)

        if inject_invalid:
            invalid_events = get_invalid_test_events()
            print(f"[Producer] Injecting {len(invalid_events)} test invalid/fault events to verify consumer resilience...")
            for country_key, raw_str in invalid_events:
                producer.produce(
                    topic=TOPIC_NAME,
                    key=country_key.encode("utf-8"),
                    value=raw_str.encode("utf-8"),
                    callback=delivery_callback,
                )
                producer.poll(0)
                snapshot_file.write(raw_str + "\n")
                sent_count += 1
                if delay > 0:
                    time.sleep(delay)

    producer.flush(timeout=30)
    print(f"[Producer] Finished streaming {sent_count} total events to '{TOPIC_NAME}'.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka Producer for World Happiness Streaming ETL")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--csv-path",
        type=Path,
        default=DEFAULT_CSV_PATH,
        help="Path to unified historical CSV file",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=float(os.getenv("STREAM_DELAY_SEC", "0.01")),
        help="Delay in seconds between records to simulate streaming arrival (default: 0.01)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of valid records to stream",
    )
    parser.add_argument(
        "--inject-invalid",
        action="store_true",
        help="Also publish 4 test events (INVALID_SCHEMA, INVALID_VALUES, PREDICTION_ERROR) to verify contract & error handling",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_producer(
        bootstrap_servers=args.bootstrap_servers,
        csv_path=args.csv_path,
        delay=args.delay,
        limit=args.limit,
        inject_invalid=args.inject_invalid,
    )
