from sqlalchemy import text
from src.config import mysql_engine


def ensure_fact_schema(conn):
    # Agrega columnas derivadas si no existen
    stmts = [
        "ALTER TABLE fact_trips ADD COLUMN trip_date DATE NULL",
        "ALTER TABLE fact_trips ADD COLUMN trip_hour TINYINT NULL",
        "ALTER TABLE fact_trips ADD COLUMN speed_mph DECIMAL(10,3) NULL",
        "CREATE INDEX idx_fact_trip_date ON fact_trips (trip_date)",
        "CREATE INDEX idx_fact_trip_hour ON fact_trips (trip_hour)",
        "CREATE INDEX idx_fact_payment ON fact_trips (payment_type)",
        "CREATE INDEX idx_fact_pickup_area ON fact_trips (pickup_community_area)",
        "CREATE INDEX idx_fact_dropoff_area ON fact_trips (dropoff_community_area)",
    ]
    for s in stmts:
        try:
            conn.execute(text(s))
        except Exception:
            # columna/indice ya existe -> ok
            pass


def rebuild_fact(conn):
    print("▶️ Rebuilding fact_trips...")
    conn.execute(text("TRUNCATE TABLE fact_trips"))

    conn.execute(text("""
        INSERT INTO fact_trips (
          trip_id, trip_start_timestamp, trip_end_timestamp, trip_seconds, trip_miles,
          pickup_community_area, dropoff_community_area,
          payment_type, company,
          fare, tips, tolls, extras, trip_total,
          pickup_centroid_latitude, pickup_centroid_longitude,
          dropoff_centroid_latitude, dropoff_centroid_longitude,
          ingested_at, source_hash,
          trip_date, trip_hour, speed_mph
        )
        SELECT
          trip_id, trip_start_timestamp, trip_end_timestamp, trip_seconds, trip_miles,
          pickup_community_area, dropoff_community_area,
          payment_type, company,
          fare, tips, tolls, extras, trip_total,
          pickup_centroid_latitude, pickup_centroid_longitude,
          dropoff_centroid_latitude, dropoff_centroid_longitude,
          ingested_at, source_hash,
          DATE(trip_start_timestamp) AS trip_date,
          HOUR(trip_start_timestamp) AS trip_hour,
          CASE
            WHEN trip_seconds IS NOT NULL AND trip_seconds > 0 AND trip_miles IS NOT NULL
              THEN trip_miles / (trip_seconds / 3600)
            ELSE NULL
          END AS speed_mph
        FROM stg_trips
        WHERE trip_start_timestamp IS NOT NULL
    """))
    print("fact_trips listo")


def rebuild_aggregates(conn):
    print("Construyendo aggregates...")

    conn.execute(text("TRUNCATE TABLE daily_kpis"))
    conn.execute(text("""
        INSERT INTO daily_kpis (
          dt, trips, revenue_total, revenue_per_trip, tips_total, tip_rate,
          avg_trip_miles, avg_trip_seconds, avg_speed_mph, p99_trip_total
        )
        SELECT
          trip_date AS dt,
          COUNT(*) AS trips,
          SUM(trip_total) AS revenue_total,
          CASE WHEN COUNT(*) > 0 THEN SUM(trip_total) / COUNT(*) ELSE NULL END AS revenue_per_trip,
          SUM(tips) AS tips_total,
          CASE WHEN SUM(trip_total) > 0 THEN SUM(tips) / SUM(trip_total) ELSE NULL END AS tip_rate,
          AVG(trip_miles) AS avg_trip_miles,
          AVG(trip_seconds) AS avg_trip_seconds,
          AVG(speed_mph) AS avg_speed_mph,
          -- p99 aproximado con NTILE (MySQL 8+)
          (SELECT MAX(x.trip_total) FROM (
              SELECT trip_total, NTILE(100) OVER (ORDER BY trip_total) AS nt
              FROM fact_trips ft2
              WHERE ft2.trip_date = ft.trip_date AND ft2.trip_total IS NOT NULL
          ) x WHERE x.nt = 100) AS p99_trip_total
        FROM fact_trips ft
        GROUP BY trip_date
    """))

    conn.execute(text("TRUNCATE TABLE hourly_kpis"))
    conn.execute(text("""
        INSERT INTO hourly_kpis (dt, hour, trips, revenue_total, avg_trip_seconds, avg_speed_mph)
        SELECT
          trip_date AS dt,
          trip_hour AS hour,
          COUNT(*) AS trips,
          SUM(trip_total) AS revenue_total,
          AVG(trip_seconds) AS avg_trip_seconds,
          AVG(speed_mph) AS avg_speed_mph
        FROM fact_trips
        WHERE trip_date IS NOT NULL AND trip_hour IS NOT NULL
        GROUP BY trip_date, trip_hour
    """))

    conn.execute(text("TRUNCATE TABLE payment_kpis"))
    conn.execute(text("""
        INSERT INTO payment_kpis (dt, payment_type, trips, revenue_total)
        SELECT
          trip_date AS dt,
          COALESCE(payment_type, 'Unknown') AS payment_type,
          COUNT(*) AS trips,
          SUM(trip_total) AS revenue_total
        FROM fact_trips
        WHERE trip_date IS NOT NULL
        GROUP BY trip_date, COALESCE(payment_type, 'Unknown')
    """))

    conn.execute(text("TRUNCATE TABLE zone_kpis"))
    conn.execute(text("""
        INSERT INTO zone_kpis (dt, zone_type, community_area, trips, revenue_total)
        SELECT
          trip_date AS dt,
          'pickup' AS zone_type,
          pickup_community_area AS community_area,
          COUNT(*) AS trips,
          SUM(trip_total) AS revenue_total
        FROM fact_trips
        WHERE trip_date IS NOT NULL AND pickup_community_area IS NOT NULL
        GROUP BY trip_date, pickup_community_area
    """))

    conn.execute(text("""
        INSERT INTO zone_kpis (dt, zone_type, community_area, trips, revenue_total)
        SELECT
          trip_date AS dt,
          'dropoff' AS zone_type,
          dropoff_community_area AS community_area,
          COUNT(*) AS trips,
          SUM(trip_total) AS revenue_total
        FROM fact_trips
        WHERE trip_date IS NOT NULL AND dropoff_community_area IS NOT NULL
        GROUP BY trip_date, dropoff_community_area
        ON DUPLICATE KEY UPDATE
          trips = VALUES(trips),
          revenue_total = VALUES(revenue_total)
    """))

    print("Agregados listos")


def main():
    engine = mysql_engine()
    with engine.begin() as conn:
        ensure_fact_schema(conn)
        rebuild_fact(conn)
        rebuild_aggregates(conn)
    print("DONE: fact + aggregates")


if __name__ == "__main__":
    main()
