# Big Data Engineering (BWI, SoSe2026), Group 10

## The idea:
**With this project, we want to answer questions about the European Commercial Airspace!**

We collect live aircraft positions over Europe, keep only commercial flights,
and analyse who dominates the airspace, where traffic concentrates and what else seems interesting to us.

## Some example questions we want to answer:
1. **Dominance**: Which airlines, and which countries' carriers, fly the most over Europe?
2. **Geography**: Whose airspace is busiest, and where are the densest air corridors?
3. **Busiest airports**: Which airports are busiest, inferred by matching low-altitude climbing/descending aircraft to their nearest airport (spatial join with the airports file)?
4. **Routes**: What are the top departure/arrival airports and busiest routes?

## Data sources (as per Project Requirements):
| Type | Source | Expected Information |
|------|--------|------------------|
| **REST API** | https://openskynetwork.github.io/opensky-api/ | position, altitude, speed, callsign, operator country |
| **Web scraping** | https://en.wikipedia.org/wiki/List_of_airline_codes | maps a flight's callsign (ICAO) to its airline (also for filtering out non-commercial flights) |
| **File** | https://raw.githubusercontent.com/davidmegginson/ourairports-data/refs/heads/main/airports.csv | airport names, coordinates and countries |

## Architecture / data flow
```
OpenSky API ──(producer)──► Kafka topic group10.flights ──► Spark ETL (nb 04) ──► parquet results ──► Visualisation (nb 05)
Wikipedia (scrape) ─┐
OurAirports (file) ─┴─► reference CSVs ──(broadcast join)──► Spark ETL
```
Only the live OpenSky feed runs through Kafka (the streaming source); the two static
sources are prepared into CSVs and broadcast-joined in Spark. Full diagram + the
sources/transformations/results lists: see [`docs/data_flow.md`](docs/data_flow.md).

## Repository layout
```
src/
  config.py        infrastructure endpoints, topic name, Europe bbox
  opensky.py       OpenSky client (fetch live aircraft states)
  producer.py      Kafka producer: OpenSky -> topic group10.flights
  reference.py     build the airline + airport reference CSVs
notebooks/
  01_explore_*     data-source exploration (REST API, scraping, file)
  04_spark_etl     Kafka -> Spark -> aggregate -> parquet  (the ETL)
  05_visualization charts + maps from the stored results
data/processed/    reference CSVs (committed) + result parquets (generated)
docs/data_flow.md  data-flow diagram
```
## Setup
```bash
git clone https://github.com/jakobiene/bde-project-group10.git
cd bde-project-group10
pip install -r requirements.txt
```
Connection to VPN is needed to reach the servers.

## How to run the pipeline
The Spark notebooks must run **on the JupyterHub** (it is on the cluster network).
The producer can run anywhere with VPN + internet (laptop or a JupyterHub terminal).

1. **(once) Build the reference data** — already committed in `data/processed/`, but to
   regenerate: `python -m src.reference` (scrapes airlines, builds the airports subset).
2. **Produce flight data into Kafka** — one snapshot is enough:
   ```bash
   python -m src.producer --once      # ~4000 aircraft -> group10.flights
   ```
   (On JuypterHub use `python3.11`. Run a few times for denser maps.)
3. **Run the ETL** — open `notebooks/04_spark_etl.ipynb` on the hub → *Restart Kernel → Run All*.
   Reads Kafka, filters to commercial flights, computes Q1–Q4a + extras, writes parquet to `data/processed/`.
4. **Run the visualisation** — open `notebooks/05_visualization.ipynb` → *Run All*.
   Reads the parquet results and renders the charts + maps. Needs `reverse_geocoder`
   (`pip install reverse_geocoder`) for the "whose airspace" chart.

> **Important — do steps 2 and 3 close together.** The shared Kafka broker has short
> retention, so produce the data and run the ETL shortly after. The ETL stores results
> to parquet, which is the permanent copy used for analysis.