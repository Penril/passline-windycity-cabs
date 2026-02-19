from sqlalchemy import text
from src.config import mysql_engine

DDL = """
CREATE TABLE IF NOT EXISTS ingestion_state (
  dataset VARCHAR(32) PRIMARY KEY,
  last_watermark_ts DATETIME NULL,
  last_run_at DATETIME NULL,
  status VARCHAR(16) NULL,
  notes VARCHAR(255) NULL
);

CREATE TABLE IF NOT EXISTS stg_trips (
  trip_id VARCHAR(64) PRIMARY KEY,
  trip_start_timestamp DATETIME NULL,
  trip_end_timestamp DATETIME NULL,
  trip_seconds INT NULL,
  trip_miles DECIMAL(10,3) NULL,

  pickup_community_area INT NULL,
  dropoff_community_area INT NULL,

  payment_type VARCHAR(32) NULL,
  company VARCHAR(128) NULL,

  fare DECIMAL(10,2) NULL,
  tips DECIMAL(10,2) NULL,
  tolls DECIMAL(10,2) NULL,
  extras DECIMAL(10,2) NULL,
  trip_total DECIMAL(10,2) NULL,

  pickup_centroid_latitude DECIMAL(10,7) NULL,
  pickup_centroid_longitude DECIMAL(10,7) NULL,
  dropoff_centroid_latitude DECIMAL(10,7) NULL,
  dropoff_centroid_longitude DECIMAL(10,7) NULL,

  ingested_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  source_hash CHAR(32) NULL
);

CREATE TABLE IF NOT EXISTS fact_trips (
  trip_id VARCHAR(64) PRIMARY KEY,
  trip_start_timestamp DATETIME NULL,
  trip_end_timestamp DATETIME NULL,
  trip_seconds INT NULL,
  trip_miles DECIMAL(10,3) NULL,
  pickup_community_area INT NULL,
  dropoff_community_area INT NULL,
  payment_type VARCHAR(32) NULL,
  company VARCHAR(128) NULL,
  fare DECIMAL(10,2) NULL,
  tips DECIMAL(10,2) NULL,
  tolls DECIMAL(10,2) NULL,
  extras DECIMAL(10,2) NULL,
  trip_total DECIMAL(10,2) NULL,
  pickup_centroid_latitude DECIMAL(10,7) NULL,
  pickup_centroid_longitude DECIMAL(10,7) NULL,
  dropoff_centroid_latitude DECIMAL(10,7) NULL,
  dropoff_centroid_longitude DECIMAL(10,7) NULL,

  trip_date DATE,
  trip_hour TINYINT,
  speed_mph DECIMAL(10,3),

  ingested_at DATETIME NULL,
  source_hash CHAR(32) NULL
);

CREATE TABLE IF NOT EXISTS daily_kpis (
  dt DATE PRIMARY KEY,
  trips BIGINT NOT NULL,
  revenue_total DECIMAL(18,2) NULL,
  revenue_per_trip DECIMAL(18,4) NULL,
  tips_total DECIMAL(18,2) NULL,
  tip_rate DECIMAL(18,6) NULL,
  avg_trip_miles DECIMAL(18,4) NULL,
  avg_trip_seconds DECIMAL(18,4) NULL,
  avg_speed_mph DECIMAL(18,4) NULL,
  p99_trip_total DECIMAL(18,2) NULL,
  refreshed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS hourly_kpis (
  dt DATE NOT NULL,
  hour TINYINT NOT NULL,
  trips BIGINT NOT NULL,
  revenue_total DECIMAL(18,2) NULL,
  avg_trip_seconds DECIMAL(18,4) NULL,
  avg_speed_mph DECIMAL(18,4) NULL,
  PRIMARY KEY (dt, hour)
);

CREATE TABLE IF NOT EXISTS payment_kpis (
  dt DATE NOT NULL,
  payment_type VARCHAR(32) NOT NULL,
  trips BIGINT NOT NULL,
  revenue_total DECIMAL(18,2) NULL,
  PRIMARY KEY (dt, payment_type)
);

CREATE TABLE IF NOT EXISTS zone_kpis (
  dt DATE NOT NULL,
  zone_type VARCHAR(16) NOT NULL,
  community_area INT NOT NULL,
  trips BIGINT NOT NULL,
  revenue_total DECIMAL(18,2) NULL,
  PRIMARY KEY (dt, zone_type, community_area)
);
"""

def main():
    engine = mysql_engine()
    with engine.begin() as conn:
        for stmt in [s.strip() for s in DDL.split(";") if s.strip()]:
            conn.execute(text(stmt))
    print("Tablas creadas.")

if __name__ == "__main__":
    main()
