#!/usr/bin/env python3
# ingest/ingest_results_folders.py

"""
Folder watcher ingest (OVERWRITE mode) — CLEAN SUMMARY LOGS

What it does
------------
- Continuously scans NOWCAST_ROOT and MEDIUM_ROOT recursively for *.csv
- For each CSV:
  - Reads the data
  - Groups by (source_sheet, site_name, revision)
  - Ensures region + plant exist
  - Ensures forecast_run exists (model_name, region_id, revision, run_t0_utc)
  - OVERWRITES predictions for each (run_id, plant_id) as per CSV snapshot:
      DELETE old rows for that run+plant
      INSERT rows from CSV (with ON CONFLICT update for same timestamp)

Logging (as requested)
----------------------
Per folder scan (NOWCAST / MEDIUM) prints ONLY:
1) Which directory it used + overall IST date range found in files + missing days (if any)
2) Folder completion summary (ok/bad/new/changed/unchanged + points/runs)
   plus a DB check for the same date window (min/max + missing days)
No per-file list, no per-group rows spam.
"""

import os
import time
import hashlib
from pathlib import Path
from typing import Optional, List, Dict, Tuple
from datetime import datetime, timedelta, date

import pandas as pd
from sqlalchemy import create_engine, text
from dateutil import tz

IST_TZ = tz.gettz("Asia/Kolkata")
UTC_TZ = tz.UTC

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql+psycopg2://reuser:repass@localhost:5432/redb")
NOWCAST_ROOT = os.environ.get("NOWCAST_ROOT", "")
MEDIUM_ROOT  = os.environ.get("MEDIUM_ROOT", "")
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "30"))
DAYFIRST     = os.environ.get("DAYFIRST", "1").strip() not in ("0", "false", "False", "no")
SINGLE_PASS  = os.environ.get("SINGLE_PASS", "0").strip() in ("1", "true", "True", "yes", "YES")

# Optional: if you later want to skip unchanged files (NOT default)
SKIP_UNCHANGED = os.environ.get("SKIP_UNCHANGED", "0").strip() in ("1", "true", "True", "yes", "YES")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)


# -------------------------
# Small helpers
# -------------------------
def now_ist_str() -> str:
    return datetime.now(tz=IST_TZ).strftime("%Y-%m-%d %H:%M:%S IST")

def sha1_file(path: Path) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def infer_plant_type(site_name: str) -> str:
    s = (site_name or "").lower()
    return "WIND" if "wind" in s else "SOLAR"

def parse_timestamps_no_warning(series: pd.Series) -> Tuple[pd.Series, pd.Series]:
    """
    ISO-first parsing (no warnings), then fallback ONLY for failed rows.
    Supports common ISO variants.
    Returns:
      timestamp_raw: trimmed string
      timestamp_dt : pandas datetime (naive or tz-aware if input had offset)
    """
    ts = series.astype(str).fillna("").str.strip()

    # common formats
    dt = pd.to_datetime(ts, errors="coerce", format="%Y-%m-%d %H:%M:%S")
    if dt.isna().any():
        dt2 = pd.to_datetime(ts, errors="coerce", format="%Y-%m-%d %H:%M")
        dt = dt.fillna(dt2)

    # ISO with T and Z
    if dt.isna().any():
        dt3 = pd.to_datetime(ts, errors="coerce", format="%Y-%m-%dT%H:%M:%S")
        dt = dt.fillna(dt3)
    if dt.isna().any():
        dt4 = pd.to_datetime(ts, errors="coerce", format="%Y-%m-%dT%H:%M:%SZ")
        dt = dt.fillna(dt4)

    # ISO with timezone offset, like 2026-02-16T13:30:00+00:00
    if dt.isna().any():
        dt5 = pd.to_datetime(ts, errors="coerce", format="%Y-%m-%dT%H:%M:%S%z")
        dt = dt.fillna(dt5)

    # fallback for only failed rows
    mask = dt.isna() & ts.ne("")
    if mask.any():
        dt_fb = pd.to_datetime(ts[mask], errors="coerce", dayfirst=DAYFIRST)
        dt.loc[mask] = dt_fb

    return ts, dt

def to_ist_series(dt_series: pd.Series) -> pd.Series:
    """
    Convert datetime series to tz-aware IST.
    - tz-naive => interpret as IST (localize)
    - tz-aware => convert to IST
    """
    if dt_series is None or dt_series.empty:
        return dt_series
    # ensure datetime
    dt_series = pd.to_datetime(dt_series, errors="coerce")
    try:
        if getattr(dt_series.dt, "tz", None) is None:
            return dt_series.dt.tz_localize("Asia/Kolkata")
        return dt_series.dt.tz_convert("Asia/Kolkata")
    except Exception:
        # last-resort: return as-is
        return dt_series

def summarize_missing_dates(missing: List[date], max_ranges: int = 8) -> str:
    """
    Turn list of missing dates into compact ranges:
      [2026-02-01, 2026-02-02, ... 2026-02-08] -> "2026-02-01..2026-02-08"
    """
    if not missing:
        return "[]"

    missing = sorted(missing)
    ranges: List[Tuple[date, date]] = []
    start = prev = missing[0]
    for d in missing[1:]:
        if (d - prev).days == 1:
            prev = d
        else:
            ranges.append((start, prev))
            start = prev = d
    ranges.append((start, prev))

    def fmt(a: date, b: date) -> str:
        return a.isoformat() if a == b else f"{a.isoformat()}..{b.isoformat()}"

    out = [fmt(a, b) for a, b in ranges]
    if len(out) <= max_ranges:
        return "[" + ", ".join(out) + "]"
    return "[" + ", ".join(out[:max_ranges]) + f", ... (+{len(out)-max_ranges} ranges)]"

def missing_dates_between(have_dates: set, dmin: date, dmax: date, max_span_days: int = 120) -> List[date]:
    if not have_dates or dmin is None or dmax is None:
        return []
    span = (dmax - dmin).days
    if span <= 0 or span > max_span_days:
        return []
    full = pd.date_range(dmin, dmax, freq="D").date.tolist()
    return [d for d in full if d not in have_dates]


# -------------------------
# Schema safety
# -------------------------
def ensure_schema(conn) -> None:
    conn.execute(text("""ALTER TABLE forecast_runs ADD COLUMN IF NOT EXISTS run_t0_ist TIMESTAMPTZ;"""))
    conn.execute(text("""ALTER TABLE forecast_runs ADD COLUMN IF NOT EXISTS run_t0_raw TEXT;"""))
    conn.execute(text("""ALTER TABLE mi_predictions ADD COLUMN IF NOT EXISTS valid_time_raw TEXT;"""))


# -------------------------
# ingested_files metadata
# -------------------------
def get_last_ingested(conn, path: Path):
    return conn.execute(text("""
        SELECT ingested_at, mtime, size, sha1
        FROM ingested_files
        WHERE path = :p
    """), {"p": str(path)}).fetchone()


def mark_ingested(conn, path: Path, model_name: str, mtime: int, size: int, sha1: str):
    conn.execute(text("""
        INSERT INTO ingested_files(path, mtime, size, sha1, model_name)
        VALUES (:p, :mt, :sz, :h, :m)
        ON CONFLICT (path)
        DO UPDATE SET
          mtime=EXCLUDED.mtime,
          size=EXCLUDED.size,
          sha1=EXCLUDED.sha1,
          model_name=EXCLUDED.model_name,
          ingested_at=NOW()
    """), {"p": str(path), "mt": mtime, "sz": size, "h": sha1, "m": model_name})


# -------------------------
# DB upserts
# -------------------------
def ensure_region(conn, source_sheet: str) -> str:
    rid = conn.execute(text("""
        INSERT INTO regions(region_code, region_name)
        VALUES (:code, :name)
        ON CONFLICT (region_code)
        DO UPDATE SET region_name = EXCLUDED.region_name
        RETURNING region_id
    """), {"code": source_sheet, "name": source_sheet}).scalar()

    if rid:
        return str(rid)

    rid2 = conn.execute(text("""
        SELECT region_id FROM regions WHERE region_code=:code
    """), {"code": source_sheet}).scalar()
    return str(rid2)


def ensure_plant(conn, region_id: str, site_name: str, lat: Optional[float], lon: Optional[float]) -> str:
    ptype = infer_plant_type(site_name)
    pid = conn.execute(text("""
        INSERT INTO plants(region_id, plant_name, plant_type, lat, lon)
        VALUES (:region_id, :plant_name, :plant_type, :lat, :lon)
        ON CONFLICT (region_id, plant_name)
        DO UPDATE SET
          plant_type = EXCLUDED.plant_type,
          lat = COALESCE(EXCLUDED.lat, plants.lat),
          lon = COALESCE(EXCLUDED.lon, plants.lon)
        RETURNING plant_id
    """), {
        "region_id": region_id,
        "plant_name": site_name,
        "plant_type": ptype,
        "lat": lat,
        "lon": lon,
    }).scalar()

    if pid:
        return str(pid)

    pid2 = conn.execute(text("""
        SELECT plant_id FROM plants
        WHERE region_id=:rid AND plant_name=:p
    """), {"rid": region_id, "p": site_name}).scalar()
    return str(pid2)


def ensure_run(conn, model_name: str, region_id: str, revision: str,
               t0_raw: str, t0_ist, t0_utc) -> str:
    run_id = conn.execute(text("""
        INSERT INTO forecast_runs(model_name, region_id, revision, run_t0_utc, run_t0_ist, run_t0_raw)
        VALUES (:m, :rid, :rev, :t0utc, :t0ist, :t0raw)
        ON CONFLICT (model_name, region_id, revision, run_t0_utc)
        DO UPDATE SET
          run_t0_ist = EXCLUDED.run_t0_ist,
          run_t0_raw = EXCLUDED.run_t0_raw
        RETURNING run_id
    """), {
        "m": model_name,
        "rid": region_id,
        "rev": revision,
        "t0utc": t0_utc,
        "t0ist": t0_ist,
        "t0raw": t0_raw,
    }).scalar()

    if run_id:
        return str(run_id)

    run_id2 = conn.execute(text("""
        SELECT run_id FROM forecast_runs
        WHERE model_name=:m AND region_id=:rid AND revision=:rev AND run_t0_utc=:t0utc
    """), {"m": model_name, "rid": region_id, "rev": revision, "t0utc": t0_utc}).scalar()

    return str(run_id2) if run_id2 else ""


def overwrite_predictions(conn, run_id: str, plant_id: str, df_group: pd.DataFrame, plant_type: str) -> int:
    # Snapshot behavior: DB becomes exactly what CSV currently has for this run+plant.
    conn.execute(text("DELETE FROM mi_predictions WHERE run_id=:r AND plant_id=:p"),
                 {"r": run_id, "p": plant_id})

    rows: List[Dict] = []
    for _, r in df_group.iterrows():
        vt_raw = r.get("timestamp_raw", "")
        vt_dt = r.get("timestamp_dt")
        if pd.isna(vt_dt):
            continue

        # If naive -> interpret as IST
        if getattr(vt_dt, "tzinfo", None) is None:
            vt_ist = vt_dt.replace(tzinfo=IST_TZ)
        else:
            vt_ist = vt_dt.astimezone(IST_TZ)

        vt_utc = vt_ist.astimezone(UTC_TZ)

        pred = r.get("prediction")
        ghi  = r.get("ghi")

        power_pred = float(pred) if pd.notna(pred) else None
        ghi_pred   = float(ghi) if pd.notna(ghi) else None

        rows.append({
            "run_id": run_id,
            "plant_id": plant_id,
            "valid_time_utc": vt_utc,
            "valid_time_ist": vt_ist,
            "valid_time_raw": vt_raw,
            "ghi_pred_wm2": ghi_pred,
            "power_pred_mw": power_pred,
            "solar_power_pred_mw": power_pred if plant_type == "SOLAR" else None,
            "wind_power_pred_mw": power_pred if plant_type == "WIND" else None,
        })

    if not rows:
        return 0

    conn.execute(text("""
        INSERT INTO mi_predictions(
          run_id, plant_id, valid_time_utc, valid_time_ist, valid_time_raw,
          ghi_pred_wm2, power_pred_mw, solar_power_pred_mw, wind_power_pred_mw
        )
        VALUES (
          :run_id, :plant_id, :valid_time_utc, :valid_time_ist, :valid_time_raw,
          :ghi_pred_wm2, :power_pred_mw, :solar_power_pred_mw, :wind_power_pred_mw
        )
        ON CONFLICT (run_id, plant_id, valid_time_utc)
        DO UPDATE SET
          valid_time_ist = EXCLUDED.valid_time_ist,
          valid_time_raw = EXCLUDED.valid_time_raw,
          ghi_pred_wm2 = EXCLUDED.ghi_pred_wm2,
          power_pred_mw = EXCLUDED.power_pred_mw,
          solar_power_pred_mw = EXCLUDED.solar_power_pred_mw,
          wind_power_pred_mw = EXCLUDED.wind_power_pred_mw
    """), rows)

    return len(rows)


# -------------------------
# One CSV ingest (no verbose prints)
# -------------------------
def ingest_csv(path: Path, model_name: str, scan_dates: set) -> Dict:
    """
    Returns a small dict for summary counters:
      ok/bad, status (NEW/CHANGED/UNCHANGED), points_inserted, runs_with_points,
      min_date/max_date from this file (IST, date)
    Also updates scan_dates set with IST dates found in this file.
    """
    st = path.stat()
    mtime, size = int(st.st_mtime), int(st.st_size)

    summary = {
        "ok": False,
        "status": "NEW",
        "points": 0,
        "runs_with_points": 0,
        "min_date": None,
        "max_date": None,
        "parsed_ok": 0,
        "parsed_bad": 0,
    }

    with engine.begin() as conn:
        ensure_schema(conn)

        prev = get_last_ingested(conn, path)
        prev_mtime = prev[1] if prev else None
        prev_size  = prev[2] if prev else None
        prev_sha1  = prev[3] if prev else None

        unchanged_by_stat = (prev is not None and prev_mtime == mtime and prev_size == size)

        if prev is None:
            status = "NEW"
        else:
            status = "UNCHANGED" if unchanged_by_stat else "CHANGED"
        summary["status"] = status

        if SKIP_UNCHANGED and status == "UNCHANGED":
            # still count date window as unknown (we didn't read file)
            summary["ok"] = True
            return summary

        # only compute sha1 if needed
        sha1 = prev_sha1 if (unchanged_by_stat and prev_sha1) else sha1_file(path)

        # read CSV
        try:
            df = pd.read_csv(path)
        except Exception:
            return summary  # ok stays False

        required = ["revision", "timestamp", "site_name", "source_sheet"]
        if any(c not in df.columns for c in required):
            return summary

        df["revision"] = df["revision"].astype(str).str.strip()
        df["source_sheet"] = df["source_sheet"].astype(str).str.strip()
        df["site_name"] = df["site_name"].astype(str).str.strip()

        ts_raw, ts_dt = parse_timestamps_no_warning(df["timestamp"])
        df["timestamp_raw"] = ts_raw
        df["timestamp_dt"] = ts_dt

        # scan date coverage (IST)
        dt_ist = to_ist_series(df["timestamp_dt"])
        parsed_ok = int(dt_ist.notna().sum())
        parsed_bad = int(len(df) - parsed_ok)
        summary["parsed_ok"] = parsed_ok
        summary["parsed_bad"] = parsed_bad

        if parsed_ok > 0:
            dmin = dt_ist.dropna().min()
            dmax = dt_ist.dropna().max()
            summary["min_date"] = dmin.date()
            summary["max_date"] = dmax.date()

            # update set of dates seen in this folder scan
            # (unique IST dates from this file)
            try:
                file_dates = set(dt_ist.dropna().dt.date.tolist())
                scan_dates.update(file_dates)
            except Exception:
                pass

        grouped = df.groupby(["source_sheet", "site_name", "revision"], dropna=False)

        points_total = 0
        runs_with_points = 0

        for (source_sheet, site_name, revision), g in grouped:
            source_sheet = str(source_sheet or "").strip()
            site_name = str(site_name or "").strip()
            revision = str(revision or "").strip()
            if not source_sheet or not site_name or not revision:
                continue

            lat = lon = None
            if "latitude" in g.columns:
                vv = pd.to_numeric(g["latitude"], errors="coerce").dropna().head(1)
                lat = float(vv.iloc[0]) if len(vv) else None
            if "longitude" in g.columns:
                vv = pd.to_numeric(g["longitude"], errors="coerce").dropna().head(1)
                lon = float(vv.iloc[0]) if len(vv) else None

            region_id = ensure_region(conn, source_sheet)
            plant_id = ensure_plant(conn, region_id, site_name, lat, lon)

            g = g.sort_values("timestamp_dt")
            t0_dt = g["timestamp_dt"].min()
            if pd.isna(t0_dt):
                continue

            # interpret run start as IST for naive timestamps
            if getattr(t0_dt, "tzinfo", None) is None:
                t0_ist = t0_dt.replace(tzinfo=IST_TZ)
            else:
                t0_ist = t0_dt.astimezone(IST_TZ)
            t0_utc = t0_ist.astimezone(UTC_TZ)

            try:
                t0_raw = g.loc[g["timestamp_dt"].idxmin(), "timestamp_raw"]
            except Exception:
                t0_raw = ""

            run_id = ensure_run(conn, model_name, region_id, revision, t0_raw, t0_ist, t0_utc)
            if not run_id:
                continue

            plant_type = infer_plant_type(site_name)
            n = overwrite_predictions(conn, run_id, plant_id, g, plant_type)
            points_total += n
            if n > 0:
                runs_with_points += 1

        mark_ingested(conn, path, model_name, mtime, size, sha1)

        summary["ok"] = True
        summary["points"] = points_total
        summary["runs_with_points"] = runs_with_points
        return summary


# -------------------------
# Folder scan
# -------------------------
def list_csv_files(root: Path) -> List[Path]:
    return sorted(set(list(root.rglob("*.csv")) + list(root.rglob("*.CSV"))), key=str)


def db_check_window(model_name: str, dmin: date, dmax: date) -> Tuple[Optional[date], Optional[date], int, List[date]]:
    """
    Check DB coverage for this model within [dmin, dmax] based on valid_time_ist::date.
    Returns (db_min_date, db_max_date, total_rows, missing_days_in_db)
    """
    if dmin is None or dmax is None:
        return None, None, 0, []

    # Use IST window boundaries (inclusive dates)
    start_dt = datetime(dmin.year, dmin.month, dmin.day, 0, 0, 0, tzinfo=IST_TZ)
    end_dt = datetime(dmax.year, dmax.month, dmax.day, 0, 0, 0, tzinfo=IST_TZ) + timedelta(days=1)

    with engine.begin() as conn:
        row = conn.execute(text("""
            SELECT
              MIN(p.valid_time_ist)::date AS dmin,
              MAX(p.valid_time_ist)::date AS dmax,
              COUNT(*)::bigint           AS n
            FROM mi_predictions p
            JOIN forecast_runs r ON r.run_id = p.run_id
            WHERE r.model_name = :m
              AND p.valid_time_ist >= :start_dt
              AND p.valid_time_ist <  :end_dt
        """), {"m": model_name, "start_dt": start_dt, "end_dt": end_dt}).fetchone()

        db_min = row[0]
        db_max = row[1]
        nrows = int(row[2]) if row and row[2] is not None else 0

        # day counts in db (to find missing dates)
        daily = conn.execute(text("""
            SELECT p.valid_time_ist::date AS d, COUNT(*)::bigint AS n
            FROM mi_predictions p
            JOIN forecast_runs r ON r.run_id = p.run_id
            WHERE r.model_name = :m
              AND p.valid_time_ist >= :start_dt
              AND p.valid_time_ist <  :end_dt
            GROUP BY 1
            ORDER BY 1
        """), {"m": model_name, "start_dt": start_dt, "end_dt": end_dt}).fetchall()

    have_db_dates = {r[0] for r in daily if r and r[0] is not None}
    missing_db = missing_dates_between(have_db_dates, dmin, dmax, max_span_days=120)
    return db_min, db_max, nrows, missing_db


def scan_root(root: str, model_name: str):
    if not root:
        print(f"[{now_ist_str()}] [{model_name}] root is empty (NOWCAST_ROOT/MEDIUM_ROOT not set)")
        return

    rp = Path(root).expanduser().resolve()
    if not rp.exists():
        print(f"[{now_ist_str()}] [{model_name}] root not found: {rp}")
        return

    files = list_csv_files(rp)

    # Collect scan coverage (from file timestamps)
    scan_dates: set = set()
    scan_min: Optional[date] = None
    scan_max: Optional[date] = None

    ok = bad = 0
    new = changed = unchanged = 0
    points_total = 0
    runs_total = 0

    # ingest all files (quietly)
    for p in files:
        try:
            s = ingest_csv(p, model_name, scan_dates)
            if not s["ok"]:
                bad += 1
                continue
            ok += 1

            st = s["status"]
            if st == "NEW":
                new += 1
            elif st == "CHANGED":
                changed += 1
            elif st == "UNCHANGED":
                unchanged += 1

            points_total += int(s["points"] or 0)
            runs_total += int(s["runs_with_points"] or 0)

            dmin = s["min_date"]
            dmax = s["max_date"]
            if dmin is not None:
                scan_min = dmin if scan_min is None else min(scan_min, dmin)
            if dmax is not None:
                scan_max = dmax if scan_max is None else max(scan_max, dmax)

        except Exception:
            bad += 1

    # Prepare scan range summary
    if scan_min and scan_max and scan_dates:
        missing_files = missing_dates_between(scan_dates, scan_min, scan_max, max_span_days=120)
        miss_files_str = summarize_missing_dates(missing_files)
        range_str = f"{scan_min.isoformat()}..{scan_max.isoformat()}"
    else:
        miss_files_str = "[]"
        range_str = "NONE"

    # 1) FIRST LINE: folder + date window scanned from files
    print(
        f"[{now_ist_str()}] [SCAN] {model_name} dir={rp} files={len(files)} "
        f"IST_dates={range_str} missing_in_files={miss_files_str}"
    )

    # DB check within this scanned window (only if we got a window)
    db_min, db_max, db_rows, missing_db = db_check_window(model_name, scan_min, scan_max)
    miss_db_str = summarize_missing_dates(missing_db) if missing_db else "[]"
    db_range_str = f"{db_min}..{db_max}" if (db_min and db_max) else "NONE"

    # 2) SECOND LINE: folder completed
    print(
        f"[{now_ist_str()}] [DONE] {model_name} dir={rp} ok={ok} bad={bad} "
        f"new={new} changed={changed} unchanged={unchanged} points={points_total} runs={runs_total} "
        f"DB_dates={db_range_str} missing_in_db={miss_db_str} rows_in_db_window={db_rows}"
    )


def main():
    print(f"[{now_ist_str()}] Folder watcher ingest started.")
    print(f"[{now_ist_str()}] NOWCAST_ROOT={Path(NOWCAST_ROOT).expanduser() if NOWCAST_ROOT else ''}")
    print(f"[{now_ist_str()}] MEDIUM_ROOT={Path(MEDIUM_ROOT).expanduser() if MEDIUM_ROOT else ''}")
    print(f"[{now_ist_str()}] POLL_SECONDS={POLL_SECONDS} SINGLE_PASS={SINGLE_PASS} SKIP_UNCHANGED={SKIP_UNCHANGED}")
    print(f"[{now_ist_str()}] MODE=OVERWRITE (snapshot: delete+insert per run+plant)\n")

    while True:
        scan_root(NOWCAST_ROOT, "NOWCAST")
        scan_root(MEDIUM_ROOT, "MEDIUM")

        if SINGLE_PASS:
            break

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
