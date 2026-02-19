import os
import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

from src.config import mysql_engine
from src.ingest.socrata_client import SocrataClient

RAW_DIR = Path("data/raw")

# Campos esperados (si alguno no viene, lo dejamos NULL)
COLS = [
    "trip_id",
    "trip_start_timestamp",
    "trip_end_timestamp",
    "trip_seconds",
    "trip_miles",
    "pickup_community_area",
    "dropoff_community_area",
    "payment_type",
    "company",
    "fare",
    "tips",
    "tolls",
    "extras",
    "trip_total",
    "pickup_centroid_latitude",
    "pickup_centroid_longitude",
    "dropoff_centroid_latitude",
    "dropoff_centroid_longitude",
]

def iso_to_dt(x):
    if x is None:
        return None

    # Si viene como NaN (float)
    if isinstance(x, float):
        return None

    # Si no es string, no lo procesamos
    if not isinstance(x, str):
        return None

    try:
        return datetime.fromisoformat(
            x.replace("Z", "+00:00")
        ).replace(tzinfo=None)
    except Exception:
        return None

def _json_safe(v):
    # None / NaN
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None

    # pandas Timestamp
    if isinstance(v, pd.Timestamp):
        # ISO sin tz
        return v.to_pydatetime().replace(tzinfo=None).isoformat(sep=" ")

    # datetime
    if isinstance(v, datetime):
        return v.replace(tzinfo=None).isoformat(sep=" ")

    # enteros pandas (Int64) -> int normal
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    # fallback: deja strings/números como están
    return v

def md5_row(d: dict) -> str:
    safe = {k: _json_safe(v) for k, v in d.items()}
    s = json.dumps(safe, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def ensure_state(conn, dataset: str):
    conn.execute(text("""
        INSERT IGNORE INTO ingestion_state(dataset, last_watermark_ts, last_run_at, status)
        VALUES (:d, NULL, NULL, NULL)
    """), {"d": dataset})

def get_watermark_or_window_start(conn, client: SocrataClient, dataset: str) -> datetime:
    row = conn.execute(
        text("SELECT last_watermark_ts FROM ingestion_state WHERE dataset=:d"),
        {"d": dataset},
    ).mappings().first()

    if row and row["last_watermark_ts"]:
        return row["last_watermark_ts"]

    # Primera corrida: ventana 60 días basada en el max real del dataset
    res = client.get({"$select": "max(trip_start_timestamp) as mx"})
    mx = res[0].get("mx")
    if not mx:
        raise RuntimeError("No pude obtener max(trip_start_timestamp). Revisa nombre de campo.")
    mx_dt = iso_to_dt(mx)
    return mx_dt - timedelta(days=60)

def upsert_staging(conn, df: pd.DataFrame):
    if df.empty:
        return 0

    # Asegura columnas
    for c in COLS:
        if c not in df.columns:
            df[c] = None

    # Tipos/normalización
    df["trip_start_timestamp"] = df["trip_start_timestamp"].apply(iso_to_dt)
    df["trip_end_timestamp"] = df["trip_end_timestamp"].apply(iso_to_dt)

    num_int = ["trip_seconds", "pickup_community_area", "dropoff_community_area"]
    for c in num_int:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    num_dec = ["trip_miles", "fare", "tips", "tolls", "extras", "trip_total",
               "pickup_centroid_latitude", "pickup_centroid_longitude",
               "dropoff_centroid_latitude", "dropoff_centroid_longitude"]
    for c in num_dec:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["source_hash"] = None

    # ✅ ESTA LÍNEA ES LA CLAVE
    df = df.where(pd.notna(df), None)

    # UPSERT por trip_id
    sql = """
    INSERT INTO stg_trips (
    trip_id, trip_start_timestamp, trip_end_timestamp, trip_seconds, trip_miles,
    pickup_community_area, dropoff_community_area,
    payment_type, company,
    fare, tips, tolls, extras, trip_total,
    pickup_centroid_latitude, pickup_centroid_longitude,
    dropoff_centroid_latitude, dropoff_centroid_longitude,
    source_hash
    )
    VALUES (
    :trip_id, :trip_start_timestamp, :trip_end_timestamp, :trip_seconds, :trip_miles,
    :pickup_community_area, :dropoff_community_area,
    :payment_type, :company,
    :fare, :tips, :tolls, :extras, :trip_total,
    :pickup_centroid_latitude, :pickup_centroid_longitude,
    :dropoff_centroid_latitude, :dropoff_centroid_longitude,
    :source_hash
    )
    ON DUPLICATE KEY UPDATE
    trip_start_timestamp=VALUES(trip_start_timestamp),
    trip_end_timestamp=VALUES(trip_end_timestamp),
    trip_seconds=VALUES(trip_seconds),
    trip_miles=VALUES(trip_miles),
    pickup_community_area=VALUES(pickup_community_area),
    dropoff_community_area=VALUES(dropoff_community_area),
    payment_type=VALUES(payment_type),
    company=VALUES(company),
    fare=VALUES(fare),
    tips=VALUES(tips),
    tolls=VALUES(tolls),
    extras=VALUES(extras),
    trip_total=VALUES(trip_total),
    pickup_centroid_latitude=VALUES(pickup_centroid_latitude),
    pickup_centroid_longitude=VALUES(pickup_centroid_longitude),
    dropoff_centroid_latitude=VALUES(dropoff_centroid_latitude),
    dropoff_centroid_longitude=VALUES(dropoff_centroid_longitude),
    source_hash=VALUES(source_hash),
    ingested_at=CURRENT_TIMESTAMP
    """
    df = df.astype(object).where(pd.notna(df), None)
    
    rows = df[[
        "trip_id","trip_start_timestamp","trip_end_timestamp","trip_seconds","trip_miles",
        "pickup_community_area","dropoff_community_area","payment_type","company",
        "fare","tips","tolls","extras","trip_total",
        "pickup_centroid_latitude","pickup_centroid_longitude",
        "dropoff_centroid_latitude","dropoff_centroid_longitude","source_hash"
    ]].to_dict("records")

    conn.execute(text(sql), rows)
    return len(rows)

def main():
    load_dotenv()
    dataset = os.environ["SOCRATA_DATASET"]
    client = SocrataClient()
    engine = mysql_engine()

    limit = 10000
    offset = 0
    max_seen = None

    with engine.begin() as conn:
        ensure_state(conn, dataset)
        start = get_watermark_or_window_start(conn, client, dataset)

    print(f"▶️ Ingestando desde watermark/start: {start}")

    while True:
        where = f"trip_start_timestamp > '{start.strftime('%Y-%m-%dT%H:%M:%S')}'"
        params = {
            "$where": where,
            "$order": "trip_start_timestamp, trip_id",
            "$limit": limit,
            "$offset": offset,
        }
        batch = client.get(params)
        if not batch:
            break

        # Guardar raw JSONL
        out_dir = RAW_DIR / f"since={start.strftime('%Y-%m-%d')}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"part-offset-{offset}.jsonl"
        with out_file.open("w", encoding="utf-8") as f:
            for r in batch:
                f.write(json.dumps(r) + "\n")

        df = pd.DataFrame(batch)

        # Track watermark
        if "trip_start_timestamp" in df.columns:
            ts = df["trip_start_timestamp"].dropna().tolist()
            if ts:
                local_max = max(iso_to_dt(x) for x in ts)
                if (max_seen is None) or (local_max > max_seen):
                    max_seen = local_max

        with engine.begin() as conn:
            n = upsert_staging(conn, df)
        print(f"✅ offset={offset} rows={n}")

        offset += limit

    if max_seen:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE ingestion_state
                SET last_watermark_ts=:w, last_run_at=NOW(), status='ok'
                WHERE dataset=:d
            """), {"w": max_seen, "d": dataset})
        print(f"🏁 Watermark actualizado a: {max_seen}")
    else:
        print("🏁 No hubo filas nuevas.")

if __name__ == "__main__":
    main()
