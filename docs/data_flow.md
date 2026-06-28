# Data Flow — Analysis of European Commercial Airspace

End-to-end flow of the project: **3 data sources → ingestion → Kafka → Spark ETL → stored results → visualisation.**

## Diagram

```mermaid
flowchart TD
    %% ---------- Sources ----------
    subgraph SRC["Data sources"]
        A["REST API<br/>OpenSky /states/all<br/>(live aircraft, Europe bbox)"]
        B["Web scraping<br/>Wikipedia<br/>List of airline codes"]
        C["File<br/>OurAirports<br/>airports.csv"]
    end

    %% ---------- Ingestion ----------
    A -->|"src/producer.py<br/>poll, JSON per aircraft"| K[("Kafka topic<br/>group10.flights<br/>172.29.16.101:9092")]
    B -->|"src/reference.py<br/>scrape + clean + dedup"| RA["airlines.csv"]
    C -->|"src/reference.py<br/>filter EU commercial"| RP["airports.csv"]

    %% ---------- Spark ETL (04) ----------
    K -->|"read stream (batch)"| S
    RA -.broadcast join.-> S
    RP -.broadcast join.-> S
    S["Spark ETL  (notebooks/04)<br/>parse · clean · keep commercial<br/>aggregate · spatial join"]

    %% ---------- Results ----------
    S --> R1["dominance_by_airline / _country"]
    S --> R2["density_grid"]
    S --> R3["busiest_airports"]
    S --> R4["top_departures / top_arrivals"]
    S --> R5["altitude_hist / flow_field"]

    %% ---------- Visualisation (05) ----------
    R1 --> V["Visualisation  (notebooks/05)<br/>bar charts · density map<br/>altitude histogram · flow field"]
    R2 --> V
    R3 --> V
    R4 --> V
    R5 --> V
```

Only the **live OpenSky feed** flows through Kafka (the streaming source). The two
reference sources are static, so they are prepared once into CSV files and
**broadcast-joined** inside Spark — Kafka is the right tool for the live stream,
not for static lookup tables.

## Data sources
| # | Type | Source | Obtained via |
|---|------|--------|--------------|
| 1 | REST API | OpenSky Network `/api/states/all` (Europe bbox) | `requests` polling in `src/producer.py` |
| 2 | Web scraping | Wikipedia "List of airline codes" | `BeautifulSoup` in `src/reference.py` |
| 3 | File | OurAirports `airports.csv` | `pandas.read_csv` in `src/reference.py` |

## Transformations
| Stage | Where | What |
|-------|-------|------|
| Ingest flights | `src/producer.py` | poll OpenSky, map state vectors → JSON, publish one message per aircraft to Kafka |
| Prepare references | `src/reference.py` | scrape airline codes (dedup ICAO); filter airports to EU + scheduled + large/medium |
| Read & parse | Spark (04) | batch-read Kafka, parse JSON to typed columns, cache |
| Clean | Spark (04) | drop on-ground / empty callsign; derive airline ICAO; drop altitude outliers |
| Commercial filter | Spark (04) | inner broadcast-join flights ↔ airline codes (private/military drop out) |
| Aggregate | Spark (04) | dominance counts; density grid; nearest-airport spatial join (haversine); climb/descent split; altitude bins; heading flow field |

## Results (stored to `data/processed/`, parquet)
| File | Question |
|------|----------|
| `dominance_by_airline`, `dominance_by_country` | Q1 — who dominates |
| `density_grid` | Q2 — busiest corridors |
| `busiest_airports` | Q3 — busiest airports |
| `top_departures`, `top_arrivals` | Q4a — departures vs arrivals |
| `altitude_hist` | extra — altitude / flight levels |
| `flow_field` | extra — dominant headings |