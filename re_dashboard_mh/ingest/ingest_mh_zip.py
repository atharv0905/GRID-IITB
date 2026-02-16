\
from __future__ import annotations

import os
import re
import json
import time
import zipfile
from datetime import datetime
from typing import Dict, Tuple, Optional

import pandas as pd
from dateutil import parser as dtparser
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# -------------------------
# Config
# -------------------------
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql+psycopg2://reuser:repass@localhost:5432/redb")
WATCH_DIR = os.environ.get("WATCH_DIR", "./incoming")   # drop MH.zip-like bundles here
SITES_CSV = os.environ.get("SITES_CSV", "./MH_sites.csv")
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "10"))

# RLDC list for grouping
RLDC_NAMES = {"WRLDC","NRLDC","SRLDC","ERLDC","NERLDC","NLDC"}

def norm_key(s: str) -> str:
    s = str(s).strip()
    s = s.replace("_", " ")
    s = re.sub(r"\s+", " ", s)
    return s.lower()

def pooling_from_pos_name(pos: str) -> str:
    """
    Heuristic pooling-station grouping from MH_sites.csv pos_name.
    - If name ends with voltage (e.g., 'Warora 220kV'), pooling = 'Warora'
    - If underscore coded (e.g., '*_SLPR_*'), pooling = 'SLPR' (or mapped name)
    """
    raw = str(pos).strip()

    # underscore-coded names: pick a meaningful token
    if "_" in raw and " " not in raw:
        toks = [t for t in raw.split("_") if t]
        # drop generic suffix tokens
        drop = {"S","HS","PSS1","PSS2","PSS3","PSS4","PSS5","KLM"}
        toks2 = [t for t in toks if t.upper() not in drop]
        # prefer all-caps short tokens like SLPR
        for t in toks2:
            if t.isalpha() and t.upper()==t and 3 <= len(t) <= 6:
                return t
        # else pick second token if exists
        if len(toks2) >= 2:
            return toks2[1]
        return toks2[0] if toks2 else raw

    # space names: remove voltage suffix
    name = raw.replace("_", " ")
    name = re.sub(r"\s+", " ", name).strip()
    name = re.sub(r"\s+\d+\s*kV$", "", name, flags=re.IGNORECASE).strip()
    return name

def plant_type_from_name(pos: str) -> str:
    s = str(pos).lower()
    if "wind" in s:
        return "Wind"
    if "hybrid" in s:
        return "Hybrid"
    # these files are GHI-based; default Solar
    return "Solar"

def parse_dt(x) -> datetime:
    if isinstance(x, datetime):
        return x
    return dtparser.parse(str(x))

def make_engine() -> Engine:
    return create_engine(DATABASE_URL, pool_pre_ping=True)

def ensure_region(conn, region_group: str, region_name: str) -> str:
    q = text("""
    INSERT INTO regions(region_group, region_name)
    VALUES (:g, :n)
    ON CONFLICT(region_group, region_name) DO UPDATE SET region_name=EXCLUDED.region_name
    RETURNING region_id
    """)
    return conn.execute(q, {"g": region_group, "n": region_name}).scalar()

def ensure_pooling(conn, region_id: str, pooling_station_name: str) -> str:
    q = text("""
    INSERT INTO pooling_stations(region_id, pooling_station_name)
    VALUES (:rid, :p)
    ON CONFLICT(region_id, pooling_station_name) DO UPDATE SET pooling_station_name=EXCLUDED.pooling_station_name
    RETURNING pooling_station_id
    """)
    return conn.execute(q, {"rid": region_id, "p": pooling_station_name}).scalar()

def ensure_plant(conn, pooling_station_id: str, plant_key: str, plant_name: str, plant_type: str, lat: float, lon: float) -> str:
    q = text("""
    INSERT INTO plants(pooling_station_id, plant_key, plant_name, plant_type, lat, lon)
    VALUES (:psid, :k, :n, :t, :lat, :lon)
    ON CONFLICT(pooling_station_id, plant_key)
    DO UPDATE SET plant_name=EXCLUDED.plant_name, plant_type=EXCLUDED.plant_type, lat=EXCLUDED.lat, lon=EXCLUDED.lon
    RETURNING plant_id
    """)
    return conn.execute(q, {"psid": pooling_station_id, "k": plant_key, "n": plant_name, "t": plant_type, "lat": lat, "lon": lon}).scalar()

def create_run(conn, run_t0_utc: datetime, source_bundle: str) -> str:
    q = text("""
    INSERT INTO forecast_runs(run_t0_utc, model_name, source_bundle)
    VALUES (:rt, 'MI', :sb)
    ON CONFLICT(run_t0_utc, model_name)
    DO UPDATE SET source_bundle=EXCLUDED.source_bundle
    RETURNING run_id
    """)
    return conn.execute(q, {"rt": run_t0_utc, "sb": source_bundle}).scalar()

def notify_new_run(conn, run_id, run_t0_utc: datetime):
    payload = json.dumps(
        {"run_id": run_id, "run_t0_utc": run_t0_utc},
        default=str  # converts UUID + datetime safely
    )
    conn.execute(text("SELECT pg_notify('forecast_updates', :p)"), {"p": payload})


def upsert_pred(conn, run_id: str, plant_id: str, r: Dict):
    q = text("""
    INSERT INTO mi_predictions(
      run_id, plant_id, lead_min, valid_time_utc, valid_time_ist,
      site_lat, site_lon,
      grid_iy, grid_ix, grid_lat, grid_lon,
      ghi_actual_wm2, ghi_pred_wm2, kstar_pred, ghi_cs_wm2, zen_deg, day_mask
    )
    VALUES (
      :rid, :pid, :lead, :vutc, :vist,
      :slat, :slon,
      :giy, :gix, :glat, :glon,
      :gha, :ghp, :kst, :ghcs, :zen, :dm
    )
    ON CONFLICT (run_id, plant_id, lead_min, valid_time_utc)
    DO UPDATE SET
      valid_time_ist=EXCLUDED.valid_time_ist,
      site_lat=EXCLUDED.site_lat,
      site_lon=EXCLUDED.site_lon,
      grid_iy=EXCLUDED.grid_iy,
      grid_ix=EXCLUDED.grid_ix,
      grid_lat=EXCLUDED.grid_lat,
      grid_lon=EXCLUDED.grid_lon,
      ghi_actual_wm2=EXCLUDED.ghi_actual_wm2,
      ghi_pred_wm2=EXCLUDED.ghi_pred_wm2,
      kstar_pred=EXCLUDED.kstar_pred,
      ghi_cs_wm2=EXCLUDED.ghi_cs_wm2,
      zen_deg=EXCLUDED.zen_deg,
      day_mask=EXCLUDED.day_mask
    """)
    conn.execute(q, {
        "rid": run_id,
        "pid": plant_id,
        "lead": int(r["lead_min"]),
        "vutc": parse_dt(r["valid_time_utc"]),
        "vist": parse_dt(r["valid_time_ist"]) if pd.notna(r.get("valid_time_ist")) else None,
        "slat": float(r.get("site_lat")) if pd.notna(r.get("site_lat")) else None,
        "slon": float(r.get("site_lon")) if pd.notna(r.get("site_lon")) else None,
        "giy": int(r.get("grid_iy")) if pd.notna(r.get("grid_iy")) else None,
        "gix": int(r.get("grid_ix")) if pd.notna(r.get("grid_ix")) else None,
        "glat": float(r.get("grid_lat")) if pd.notna(r.get("grid_lat")) else None,
        "glon": float(r.get("grid_lon")) if pd.notna(r.get("grid_lon")) else None,
        "gha": float(r.get("ghi_actual_wm2")) if pd.notna(r.get("ghi_actual_wm2")) else None,
        "ghp": float(r.get("ghi_pred_wm2")) if pd.notna(r.get("ghi_pred_wm2")) else None,
        "kst": float(r.get("kstar_pred")) if pd.notna(r.get("kstar_pred")) else None,
        "ghcs": float(r.get("ghi_cs_wm2")) if pd.notna(r.get("ghi_cs_wm2")) else None,
        "zen": float(r.get("zen_deg")) if pd.notna(r.get("zen_deg")) else None,
        "dm": float(r.get("day_mask")) if pd.notna(r.get("day_mask")) else None,
    })

def bootstrap_sites(engine: Engine, sites_csv: str):
    sites = pd.read_csv(sites_csv)
    # expected columns: pos_name, state, source_sheet, latitude, longitude
    needed = {"pos_name","state","source_sheet","latitude","longitude"}
    miss = needed - set(sites.columns)
    if miss:
        raise ValueError(f"sites CSV missing columns: {sorted(miss)}")

    with engine.begin() as conn:
        for _, r in sites.iterrows():
            source_sheet = str(r["source_sheet"]).strip()
            region_group = "RLDC" if source_sheet in RLDC_NAMES else "SLDC"
            region_name = source_sheet

            region_id = ensure_region(conn, region_group, region_name)

            pooling_name = pooling_from_pos_name(r["pos_name"])
            pooling_id = ensure_pooling(conn, region_id, pooling_name)

            plant_key = norm_key(r["pos_name"])
            plant_name = str(r["pos_name"]).replace("_"," ").strip()
            ptype = plant_type_from_name(r["pos_name"])
            lat = float(r["latitude"])
            lon = float(r["longitude"])

            ensure_plant(conn, pooling_id, plant_key, plant_name, ptype, lat, lon)

def ingest_zip(engine: Engine, zip_path: str, sites_df: pd.DataFrame):
    # Build a quick lookup from sites_df pos_name to region/pooling/plant_id by reading DB
    sites_lookup = {norm_key(x): x for x in sites_df["pos_name"].astype(str).tolist()}

    # read all csv inside zip
    zf = zipfile.ZipFile(zip_path)

    with engine.begin() as conn:
        # cache: pooling_id and plant_id already exist from bootstrap
        # build plant_id map from DB once
        plant_rows = conn.execute(text("""
          SELECT p.plant_id, p.plant_key, ps.pooling_station_id, r.region_name, r.region_group
          FROM plants p
          JOIN pooling_stations ps ON ps.pooling_station_id = p.pooling_station_id
          JOIN regions r ON r.region_id = ps.region_id
        """)).mappings().all()
        plant_id_by_key = {row["plant_key"]: row["plant_id"] for row in plant_rows}

        created_runs = set()

        for name in zf.namelist():
            if not name.lower().endswith(".csv"):
                continue

            with zf.open(name) as f:
                df = pd.read_csv(f)

            if df.empty:
                continue

            # Normalize plant key: match the 'site' field to sites.csv
            site_name = str(df.loc[0, "site"])
            # site_name uses spaces; sites.csv sometimes uses underscores
            candidate_key = norm_key(site_name)
            if candidate_key not in sites_lookup:
                # try also replacing spaces with underscores in lookup-style
                candidate_key2 = norm_key(site_name.replace(" ", "_"))
                if candidate_key2 in sites_lookup:
                    candidate_key = candidate_key2

            # Get plant_id
            plant_id = plant_id_by_key.get(candidate_key)
            if plant_id is None:
                # create minimal plant if missing (rare)
                # attach to a default region/pooling based on df state
                region_id = ensure_region(conn, "SLDC", str(df.loc[0,"state"]).strip())
                pooling_id = ensure_pooling(conn, region_id, "UNKNOWN")
                plant_id = ensure_plant(
                    conn, pooling_id, candidate_key, site_name, "Solar",
                    float(df.loc[0,"site_lat"]), float(df.loc[0,"site_lon"])
                )
                plant_id_by_key[candidate_key] = plant_id

            # parse datetimes
            df["run_t0_utc"] = df["run_t0_utc"].apply(parse_dt)
            df["valid_time_utc"] = df["valid_time_utc"].apply(parse_dt)
            df["valid_time_ist"] = df["valid_time_ist"].apply(parse_dt)

            # group by run_t0_utc: each group is a "run"
            for run_t0, g in df.groupby("run_t0_utc"):
                run_id = create_run(conn, run_t0, os.path.basename(zip_path))

                # notify once per run_t0 (not per plant)
                if run_id not in created_runs:
                    created_runs.add(run_id)

                for _, row in g.iterrows():
                    upsert_pred(conn, run_id, plant_id, row.to_dict())

        # broadcast notifications at the end
        for run_id in created_runs:
            rt = conn.execute(text("SELECT run_t0_utc FROM forecast_runs WHERE run_id=:rid"), {"rid": run_id}).scalar()
            notify_new_run(conn, run_id, rt)

    print(f"[OK] Ingested zip: {os.path.basename(zip_path)}")

def main():
    os.makedirs(WATCH_DIR, exist_ok=True)
    engine = make_engine()

    # 1) bootstrap sites -> DB
    print("[BOOT] Loading sites into DB ...")
    bootstrap_sites(engine, SITES_CSV)

    # 2) watch for new zip bundles
    sites_df = pd.read_csv(SITES_CSV)
    seen = set()
    print(f"[WATCH] {WATCH_DIR} (zip bundles) ...")

    while True:
        bundles = sorted(
            [os.path.join(WATCH_DIR, f) for f in os.listdir(WATCH_DIR) if f.lower().endswith(".zip")],
            key=lambda p: os.path.getmtime(p),
        )
        for fp in bundles:
            if fp in seen:
                continue
            try:
                ingest_zip(engine, fp, sites_df)
                seen.add(fp)
            except Exception as e:
                print(f"[ERROR] {os.path.basename(fp)} -> {e}")

        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
