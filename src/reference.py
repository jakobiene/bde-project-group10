"""Reference-data preparation (the two non-streaming sources).

Builds the static lookups that Spark joins against the live flight stream:

  1. airlines  — Wikipedia "List of airline codes" scraped into icao -> (airline, country)
                 Used to (a) filter flights to commercial and (b) label airlines.
  2. airports  — OurAirports CSV filtered to European commercial airports with
                 coordinates. Used for the busiest-airports spatial join.

Writes both to data/processed as CSV. Run once (and re-run to refresh):
    python -m src.reference
"""
from __future__ import annotations
import os

import pandas as pd
import requests
from bs4 import BeautifulSoup

WIKI_AIRLINES_URL = "https://en.wikipedia.org/wiki/List_of_airline_codes"
OURAIRPORTS_URL = (
    "https://raw.githubusercontent.com/davidmegginson/"
    "ourairports-data/refs/heads/main/airports.csv"
)
OUT_DIR = "data/processed"


def build_airlines() -> pd.DataFrame:
    """Scrape Wikipedia airline codes -> deduplicated icao/airline/country lookup."""
    html = requests.get(WIKI_AIRLINES_URL, headers={"User-Agent": "bde-group10-bot"},
                        timeout=30).text
    soup = BeautifulSoup(html, "html.parser")
    table = [t for t in soup.find_all("table") if "wikitable" in (t.get("class") or [])][0]

    cols = ["iata", "icao", "airline", "callsign", "country", "comments"]
    recs = []
    for tr in table.find_all("tr")[1:]:
        cells = [c.get_text(strip=True) for c in tr.find_all(["td", "th"], recursive=False)]
        if len(cells) >= 5:
            recs.append((cells + [""])[:6])
    df = pd.DataFrame(recs, columns=cols)

    valid = df[df["icao"].str.fullmatch(r"[A-Za-z]{3}")].copy()
    lookup = valid.drop_duplicates(subset="icao", keep="first")[["icao", "airline", "country"]]
    return lookup.reset_index(drop=True)


def build_airports() -> pd.DataFrame:
    """Load OurAirports -> European commercial airports with coordinates.

    Note the keep_default_na=False: otherwise pandas turns continent 'NA'
    (North America) into NaN. Not strictly needed for the EU filter, but correct.
    """
    df = pd.read_csv(OURAIRPORTS_URL, keep_default_na=False, na_values=[""])
    eu = df[
        (df["continent"] == "EU")
        & (df["scheduled_service"] == "yes")
        & (df["type"].isin(["large_airport", "medium_airport"]))
    ].copy()
    keep = ["icao_code", "iata_code", "name", "iso_country",
            "latitude_deg", "longitude_deg", "type"]
    return eu[keep].reset_index(drop=True)


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)

    airlines = build_airlines()
    airlines_path = os.path.join(OUT_DIR, "airlines.csv")
    airlines.to_csv(airlines_path, index=False)
    print(f"airlines: {len(airlines)} rows -> {airlines_path}")

    airports = build_airports()
    airports_path = os.path.join(OUT_DIR, "airports.csv")
    airports.to_csv(airports_path, index=False)
    print(f"airports: {len(airports)} rows -> {airports_path}")


if __name__ == "__main__":
    main()
