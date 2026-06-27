# Big Data Engineering (BWI, SoSe2026), Group 10

## The idea:
**With this project, we want to answer questions about the European Commercial Airspace!**

We collect live aircraft positions over Europe, keep only commercial flights,
and analyse who dominates the airspace, where traffic concentrates and what else seems interesting to us.

## Some example questions we want to answer:
1. **Dominance** — which airlines, and which countries' carriers, fly the most over Europe?
2. **Geography** — whose airspace is busiest, and where are the densest air corridors?
3. **Busiest airports** — which airports are busiest, inferred by matching low-altitude climbing/descending aircraft to their nearest airport (spatial join with the airports file)?
4. **Routes** — what are the top departure/arrival airports and busiest routes?

## Data sources (as per Project Requirements):
| Type | Source | Expected Information |
|------|--------|------------------|
| **REST API** | https://openskynetwork.github.io/opensky-api/ | position, altitude, speed, callsign, operator country |
| **Web scraping** | https://en.wikipedia.org/wiki/List_of_airline_codes | maps a flight's callsign (ICAO) to its airline (also for filtering out non-commercial flights) |
| **File** | https://raw.githubusercontent.com/davidmegginson/ourairports-data/refs/heads/main/airports.csv | airport names, coordinates and countries |