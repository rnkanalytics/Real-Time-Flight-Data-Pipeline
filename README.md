# ✈️ Real-Time Global Flight Risk Monitor

> Real-time airspace threat intelligence — tracking 10,000+ flights worldwide against active conflict zones, providing instant visibility into safety compliance and rerouting requirements.

🔴 [Live Dashboard](https://rnkanalytics.grafana.net/public-dashboards/d7b34806ee1f4c449e603dc80f691448) &nbsp;|&nbsp; 👤 [LinkedIn](https://www.linkedin.com/in/ramiz-khatib/) &nbsp;|&nbsp; 💻 [GitHub](https://github.com/rnkanalytics)

---

## 🌍 Project Overview

This pipeline ingests live ADS-B flight data every 5 seconds, cross-references it against AI-powered conflict zone intelligence, scores each flight by risk level, and visualizes everything on a real-time Grafana dashboard.

---

## 🏗️ Architecture
```
airplanes.live API (Live ADS-B)
         ↓
    producer.py          ← Fetches 10,000+ live flights worldwide
         ↓
      Kafka              ← Message queue (topic: flights-raw)
         ↓
  spark_stream.py        ← Processes & enriches flight data
         ↓
    BigQuery             ← flights-490708.flight_data
         ↓
flight_risk_analytics    ← View: JOIN flights + restricted zones
         ↓
flight_risk_snapshot     ← Table: persistent last known state
         ↓
    Grafana 11           ← Live public dashboard

         +

  update_airspace.py     ← Runs daily via GitHub Actions
         ↓
  Claude AI + Web Search ← Researches current NOTAMs & restrictions
         ↓
  restricted_airspace    ← BigQuery table: auto-updated daily
```

---

## 🛠️ Tech Stack

| Component | Technology |
|---|---|
| Data Ingestion | Python, airplanes.live API |
| Message Queue | Apache Kafka + Zookeeper (Confluent 7.4.0) |
| Stream Processing | Apache Spark 3.5.0 (PySpark) |
| Data Warehouse | Google BigQuery |
| AI Enrichment | Claude AI (Anthropic) + Web Search |
| Visualization | Grafana 11 |
| Infrastructure | GCP e2-medium VM, Docker Compose |
| CI/CD | GitHub Actions |

---

## 📊 BigQuery Data Model

### flights
Raw ADS-B streaming data — partitioned by day, 1-day expiry

### restricted_airspace
Conflict zones and no-fly zones — refreshed daily by Claude AI

### flight_risk_analytics (View)
JOIN of flights + restricted_airspace with enriched risk scoring

| Column | Description |
|---|---|
| zone_status | INSIDE / NEAR / APPROACHING |
| miles_from_zone | Distance to nearest restricted zone edge |
| risk_score | 1-10 based on severity x proximity |
| risk_label | CRITICAL / HIGH / MEDIUM / LOW |
| flight_phase | CLIMBING / CRUISING / DESCENDING |

### flight_risk_snapshot
Persistent last known state — dashboard stays populated even when VM is off

---

## 🎯 Risk Scoring Logic

| Zone Status | CLOSED | HIGH RISK | RESTRICTED |
|---|---|---|---|
| INSIDE | 10 | 7 | 5 |
| NEAR (<69 miles) | 7 | 5 | 3 |
| APPROACHING (<138 miles) | 2 | 2 | 2 |

---

## 🤖 AI-Powered Airspace Updates

update_airspace.py runs every morning at 6am UTC via GitHub Actions:

1. Claude searches the web for current NOTAMs, FAA SFARs, and EASA bulletins
2. Extracts structured data — country, bounding box, severity, reason
3. Wipes and reloads the restricted_airspace table in BigQuery
4. Spark picks it up automatically on the next batch

---

## 📈 Grafana Dashboard

Live URL: https://rnkanalytics.grafana.net/public-dashboards/d7b34806ee1f4c449e603dc80f691448

### Panels
- Live Flight Risk Map — color-coded planes by risk score with heading arrows
- Total Flights Monitored — count of flights near restricted zones
- Restricted Zones Active — total active no-fly zones
- Active Conflict Zones — zones with flights currently inside
- Inside Closed Airspace — flights violating closed airspace (risk score 10)
- Near Closed or Inside High Risk — risk score 7 flights
- Near High Risk or Inside Restricted — risk score 5 flights
- Live Alert Feed — sortable table of all flagged flights
- Restricted Airspace Reference — full list of active zones with reasons

---

## 🗂️ Repository Structure
```
├── .github/
│   └── workflows/
│       └── update_airspace.yml    ← Daily AI airspace updater
├── bigquery/
│   └── schema.sql                 ← All CREATE TABLE and VIEW scripts
├── grafana/
│   └── grafana_queries.sql        ← All Grafana panel queries
├── producer/
│   └── producer.py                ← Kafka flight producer
├── spark/
│   └── spark_stream.py            ← PySpark streaming processor
├── docker-compose.yml             ← All services
├── update_airspace.py             ← Claude AI airspace updater
└── .env                           ← Environment variables
```

---

## 🚀 Getting Started

### Prerequisites
- GCP account with BigQuery enabled
- Docker + Docker Compose
- Anthropic API key
- GCP service account with BigQuery permissions

### Setup

1. Clone the repo
```bash
git clone https://github.com/rnkanalytics/Real-Time-Flight-Data-Pipeline.git
cd Real-Time-Flight-Data-Pipeline
```

2. Create BigQuery tables
```bash
# Run bigquery/schema.sql in BigQuery console
```

3. Configure environment variables
```bash
cp .env.example .env
# Add your GCP credentials and Anthropic API key
```

4. Start the pipeline
```bash
docker-compose up -d --build
```

5. Add GitHub secrets for automated airspace updates
- ANTHROPIC_API_KEY
- GCP_KEY

---

## 📌 Key Design Decisions

- **Snapshot table** — persists last known flight risk state so dashboard always has data even when VM is off
- **Claude-only zone check** — Claude API only called for flights inside restricted zones, keeping costs near zero
- **Daily AI refresh** — restricted airspace updated automatically every morning without manual intervention
- **BigQuery view** — analytics logic lives in SQL, not Spark, keeping the stream processor lean

---

## 👤 Author

**Ramiz Khatib**
[LinkedIn](https://www.linkedin.com/in/ramiz-khatib/) | [GitHub](https://github.com/rnkanalytics)
