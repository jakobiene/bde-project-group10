"""OpenSky client — fetch live aircraft states over Europe.

Turns the raw OpenSky "state vector" (a fixed-order list of 17 values) into a
list of named dicts, keeping only the fields the pipeline needs (see the field
decisions in notebooks/01_explore_rest_api_opensky.ipynb).

Filtering (on_ground, commercial, outliers) is intentionally NOT done here —
the producer ships the data as-is and Spark does the cleaning/ETL downstream.
"""
from __future__ import annotations
from typing import Iterator

import requests

from src import config

# Index positions in an OpenSky state vector we keep (drop sensors/squawk/spi/...).
_FIELDS = {
    "icao24": 0,
    "callsign": 1,
    "origin_country": 2,
    "longitude": 5,
    "latitude": 6,
    "baro_altitude": 7,
    "on_ground": 8,
    "velocity": 9,
    "true_track": 10,
    "vertical_rate": 11,
    "geo_altitude": 13,
}


def fetch_states(session: requests.Session | None = None) -> tuple[int | None, list[dict]]:
    """Fetch one snapshot of European aircraft.

    Returns ``(snapshot_time, records)`` where ``records`` is a list of dicts,
    one per aircraft, each tagged with the snapshot's ``snapshot_time``.
    """
    get = (session or requests).get
    resp = get(config.OPENSKY_URL, params=config.EUROPE_BBOX, timeout=60)
    resp.raise_for_status()
    payload = resp.json()

    snapshot_time = payload.get("time")
    records = []
    for vec in payload.get("states") or []:
        rec = {name: vec[idx] for name, idx in _FIELDS.items()}
        rec["callsign"] = (rec["callsign"] or "").strip()
        rec["snapshot_time"] = snapshot_time
        records.append(rec)
    return snapshot_time, records
