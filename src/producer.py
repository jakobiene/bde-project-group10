"""Kafka producer — stream live OpenSky flights to the broker.

Polls OpenSky for aircraft over Europe every POLL_INTERVAL seconds and publishes
one JSON message per aircraft to the flights topic. The aircraft's `icao24` is
used as the message key so all updates for the same plane land on one partition.

Usage (from the repo root):
    python -m src.producer                 # run continuously
    python -m src.producer --once          # a single poll, then exit
    python -m src.producer --dry-run       # fetch + serialize, print, DON'T send
    python -m src.producer --once --dry-run
    python -m src.producer --interval 15   # custom poll interval (seconds)
"""
from __future__ import annotations
import argparse
import json
import time

import requests

from src import config, opensky


def build_producer():
    """Create a KafkaProducer (imported lazily so --dry-run needs no broker)."""
    from kafka import KafkaProducer
    from kafka.serializer import Serializer

    class JsonSerializer(Serializer):
        def serialize(self, topic, value):
            return json.dumps(value).encode("utf-8")

    class KeySerializer(Serializer):
        def serialize(self, topic, key):
            return key.encode("utf-8") if key else None

    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BROKER,
        value_serializer=JsonSerializer(),
        key_serializer=KeySerializer(),
        acks=1,
        linger_ms=200,  # small batching for throughput
    )


def poll_once(producer, session, dry_run: bool) -> int:
    """Fetch one snapshot and send it. Returns the number of records handled."""
    snapshot_time, records = opensky.fetch_states(session=session)
    for rec in records:
        if dry_run:
            continue
        producer.send(config.TOPIC_FLIGHTS, key=rec["icao24"], value=rec)
    if not dry_run:
        producer.flush()
    ts = time.strftime("%H:%M:%S", time.localtime(snapshot_time)) if snapshot_time else "?"
    action = "fetched (dry-run)" if dry_run else f"sent -> {config.TOPIC_FLIGHTS}"
    print(f"[{time.strftime('%H:%M:%S')}] snapshot {ts}: {len(records)} aircraft {action}")
    if dry_run and records:
        print("  sample:", json.dumps(records[0]))
    return len(records)


def run(once: bool = False, dry_run: bool = False, interval: int | None = None) -> None:
    interval = interval or config.POLL_INTERVAL
    session = requests.Session()
    producer = None if dry_run else build_producer()
    print(f"producer start | broker={config.KAFKA_BROKER} | topic={config.TOPIC_FLIGHTS} "
          f"| interval={interval}s | dry_run={dry_run}")
    try:
        while True:
            try:
                poll_once(producer, session, dry_run)
            except requests.HTTPError as e:
                # OpenSky rate-limit / transient error: log and keep going.
                print(f"  OpenSky HTTP error: {e} (will retry next interval)")
            if once:
                break
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nstopping (Ctrl-C)")
    finally:
        if producer is not None:
            producer.flush()
            producer.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Stream OpenSky flights to Kafka")
    ap.add_argument("--once", action="store_true", help="single poll then exit")
    ap.add_argument("--dry-run", action="store_true", help="don't send to Kafka, just print")
    ap.add_argument("--interval", type=int, default=None, help="poll interval in seconds")
    args = ap.parse_args()
    run(once=args.once, dry_run=args.dry_run, interval=args.interval)


if __name__ == "__main__":
    main()
