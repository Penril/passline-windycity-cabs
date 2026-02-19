# WindyCity Cabs – Data Engineering Challenge

End-to-end data pipeline built to ingest, process and model Chicago Taxi Trips data, exposing executive and operational dashboards.

---

## Project Overview

This solution implements:

- Incremental ingestion from Socrata API (Chicago Taxi dataset)
- Staging and cleaned fact table (`fact_trips`)
- Analytical aggregates (`daily_kpis`, `hourly_kpis`, `zone_kpis`, `payment_kpis`)
- Business dashboards in Looker Studio

The system is designed to be idempotent, reproducible and safe to re-run.

---

## Architecture

Socrata API  
- Incremental ingestion (watermark)  
- `stg_trips`  
- `fact_trips`  
- Aggregated tables  
- Looker dashboards  

---

## Data Model

**Grain:** 1 row = 1 taxi trip  
**Primary Key:** `trip_id`  
**Incremental Strategy:**  
- Watermark based on `trip_start_timestamp`  
- Offset pagination  
- `ON DUPLICATE KEY UPDATE` upserts  

---

## Core Tables

- `fact_trips` → cleaned trip-level data  
- `daily_kpis` → daily revenue & trips  
- `hourly_kpis` → demand by hour  
- `zone_kpis` → revenue & trips by zone  
- `payment_kpis` → payment method distribution  

All monetary values are expressed in USD.

---

## Key Metrics

- Total Revenue → `SUM(trip_total)`
- Trips → `COUNT(trip_id)`
- Avg Revenue per Trip → `SUM(trip_total) / COUNT(trip_id)`
- Tip % → `SUM(tips) / SUM(trip_total)`
- Revenue per Mile → `SUM(trip_total) / SUM(trip_miles)`
- P99 Trip Total → 99th percentile of `trip_total`

---

## Dashboards

1. **Dirección / Finanzas #1** – Executive overview  
2. **Dirección / Finanzas #2** – Company analysis & revenue concentration  
3. **Operación #1** – Demand & time analysis  
4. **Operación #2** – Geographic distribution  

---

## How to Run

Install dependencies:

```bash
pip install -r requirements.txt
```

Initialize database:

```bash
python -m src.db_init
```

Run ingestion:

```bash
python -m src.ingest.ingest_and_stage
```

Build aggregates:

```bash
python -m src.transform.build_fact_and_aggregates
```

---

## Data Quality

- Non-null primary keys  
- Numeric coercion and NaN handling  
- Deduplication via PK  
- Basic outlier awareness (P99 analysis)  

---

## Use of AI

AI tools were used as coding assistants for documentation refinement and idea exploration.  
All architectural decisions, modeling strategy and metric definitions were designed and validated manually.

---

## Future Improvements

- Automated data validation tests  
- Orchestration (Airflow/Prefect)  
- Index optimization  
- Monitoring & alerting  
- Migration to analytical warehouse (e.g., BigQuery)  

