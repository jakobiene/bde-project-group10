"""Central configuration for the BDE pipeline.

All infrastructure endpoints, the topic name, the OpenSky query and the poll
interval live here so the producer, the Spark job and the notebooks agree.
Every value can be overridden with an environment variable (useful when running
on the hub vs. locally).
"""
import os

# --- Provided infrastructure ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "172.29.16.101:9092")
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://172.29.16.102:7077")

# --- Kafka topic (the live flights stream) ---
TOPIC_FLIGHTS = os.getenv("TOPIC_FLIGHTS", "group10.flights")

# --- OpenSky REST API ---
OPENSKY_URL = os.getenv("OPENSKY_URL", "https://opensky-network.org/api/states/all")

# Europe bounding box (lat 34-72 N, lon -25-50 E) sent to OpenSky as a filter.
EUROPE_BBOX = {
    "lamin": float(os.getenv("EUROPE_LAMIN", "34.0")),
    "lomin": float(os.getenv("EUROPE_LOMIN", "-25.0")),
    "lamax": float(os.getenv("EUROPE_LAMAX", "72.0")),
    "lomax": float(os.getenv("EUROPE_LOMAX", "50.0")),
}

# Seconds between OpenSky polls (anonymous users are limited to ~10s).
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))
