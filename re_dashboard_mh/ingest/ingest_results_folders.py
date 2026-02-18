#!/usr/bin/env python3
# ingest/ingest_results_folders.py
"""
Folder watcher ingest (OVERWRITE mode)

- Scans NOWCAST_ROOT and MEDIUM_ROOT recursively for *.csv
- Reads each CSV and groups by (source_sheet, site_name, revision)
- Ensures region + plant rows exist
- Ensures forecast_run exists (model_name, region_id, revision, run_t0_utc)
- OVERWRITES predictions for that (run_id, plant_id) each time (delete + insert)
- Stores:
    forecast_runs.run_t0_raw  (exact earliest timestamp string from CSV)
    forecast_runs.run_t0_ist  (IST datetime)
    forecast_runs.run_t0_utc  (UTC datetime)
    mi_predictions.valid_time_raw (exact timestamp string from CSV row)
    mi_predictions.valid_time_ist/utc

Requested behavior
------------------
- NO pandas dayfirst warning (ISO-first, fallback only for failed rows)
- DO NOT SKIP ingestion (always overwrite)
- Print ALL detected CSVs per scan
- Show if a file was unchanged vs changed (but still ingest)
- Print per file: runs_with_points, total points, and per-plant row counts
- NEW: Print which IST dates were present in each file and each group, and show gaps.
"""

import os
import time
import hashlib
from pathlib import Path
from typing import Optional, List, Dict, Tuple
from datetime import date

import pandas as pd
from sqlalchemy import create_engine, text
from dateutil import tz

IST = tz.gettz("Asia/Kolkata")
UTC = tz.UTC

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql+psycopg2://reuser:repass@localhost:5432/redb")
NOWCAST_ROOT = os.environ.get("NOWCAST_ROOT", "")
MEDIUM_ROOT  = os.environ.get("MEDIUM_ROOT", "")
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "30"))
DAYFIRST     = os.environ.get("DAYFIRST", "1").strip() not in ("0", "false", "False", "no")
SINGLE_PASS  = os.environ.get("SINGLE_PASS", "0").strip() in ("1", "true", "True", "yes", "YES")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)


# -------------------------
# Helpers
# -------------------------
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
    No warning approach:
      1) ISO parse with explicit formats
      2) Fallback parse ONLY for rows that failed ISO parse (so no global warning)

    Returns:
      timestamp_raw: original trimmed strings
      timestamp_dt : parsed datetimes (naive)
    """
    ts = series.astype(str).fillna("").str.strip()

    # Try strict ISO formats first (common in your files)
    dt = pd.to_datetime(ts, errors="coerce", format="%Y-%m-%d %H:%M:%S")
    if dt.isna().any():
        dt2 = pd.to_datetime(ts, errors="coerce", format="%Y-%m-%d %H:%M")
        dt = dt.fillna(dt2)

    # Fallback only on failed rows (prevents pandas warning spam)
    mask = dt.isna() & ts.ne("")
    if mask.any():
        dt_fb = pd.to_datetime(ts[mask], errors="coerce", dayfirst=DAYFIRST)
        dt.loc[mask] = dt_fb

    return ts, dt


def _series_to_ist(dt_series: pd.Series) -> pd.Series:
    """
    Convert a datetime Series to tz-aware IST.
    If naive -> localize IST. If tz-aware -> convert IST.
    """
    if dt_series is None or dt_series.empty:
        return dt_series

    try:
        # dt accessor exists for datetime-like Series
        tzinfo = getattr(dt_series.dt, "tz", None)
    except Exception:
        tzinfo = None

    try:
        if tzinfo is None:
            # naive -> interpret as IST
            return dt_series.dt.tz_localize("Asia/Kolkata")
        else:
            # tz-aware -> convert to IST
            return dt_series.dt.tz_convert("Asia/Kolkata")
    except Exception:
        # last-resort: return original (better than crashing debug)
        return dt_series


def _dates_from_dt_ist(dt_ist: pd.Series) -> List[date]:
    if dt_ist is None or len(dt_ist) == 0:
        return []
    try:
        vals = dt_ist.dropna().dt.date
        uniq = sorted(set(vals.tolist()))
        return uniq
    except Exception:
        return []


def _fmt_dates(dates: List[date], max_items: int = 25) -> str:
    if not dates:
        return "[]"
    if len(dates) <= max_items:
        return "[" + ", ".join(d.isoformat() for d in dates) + "]"
    return "[" + ", ".join(d.isoformat() for d in dates[:max_items]) + f", ... (+{len(dates)-max_items})]"


def _missing_dates(dates: List[date], max_span_days: int = 90) -> List[date]:
    """
    Compute missing dates between min(dates) and max(dates).
    For safety, only if span <= max_span_days.
    """
    if not dates:
        return []
    d0, d1 = dates[0], dates[-1]
    span = (d1 - d0).days
    if span <= 0 or span > max_span_days:
        return []
    full = set(pd.date_range(d0, d1, freq="D").date.tolist())
    have = set(dates)
    miss = sorted(full - have)
    return miss


# -------------------------
# Schema safety (idempotent)
# -------------------------
def ensure_schema(conn) -> None:
    conn.execute(text("""ALTER TABLE forecast_runs ADD COLUMN IF NOT EXISTS run_t0_ist TIMESTAMPTZ;"""))
    conn.execute(text("""ALTER TABLE forecast_runs ADD COLUMN IF NOT EXISTS run_t0_raw TEXT;"""))
    conn.execute(text("""ALTER TABLE mi_predictions ADD COLUMN IF NOT EXISTS valid_time_raw TEXT;"""))


def get_last_ingested(conn, path: Path):
    row = conn.execute(text("""
        SELECT model_name, ingested_at, mtime, size, sha1
        FROM ingested_files
        WHERE path = :p
    """), {"p": str(path)}).fetchone()
    return row


# -------------------------
# DB upserts
# -------------------------
def ensure_region(conn, source_sheet: str) -> str:
    q = text("""
        INSERT INTO regions(region_code, region_name)
        VALUES (:code, :name)
        ON CONFLICT (region_code)
        DO UPDATE SET region_name = EXCLUDED.region_name
        RETURNING region_id
    """)
    rid = conn.execute(q, {"code": source_sheet, "name": source_sheet}).scalar()
    if rid:
        return str(rid)
    rid2 = conn.execute(
        text("SELECT region_id FROM regions WHERE region_code=:code"),
        {"code": source_sheet},
    ).scalar()
    return str(rid2)


def ensure_plant(conn, region_id: str, site_name: str, lat: Optional[float], lon: Optional[float]) -> str:
    ptype = infer_plant_type(site_name)
    q = text("""
        INSERT INTO plants(region_id, plant_name, plant_type, lat, lon)
        VALUES (:region_id, :plant_name, :plant_type, :lat, :lon)
        ON CONFLICT (region_id, plant_name)
        DO UPDATE SET
          plant_type = EXCLUDED.plant_type,
          lat = COALESCE(EXCLUDED.lat, plants.lat),
          lon = COALESCE(EXCLUDED.lon, plants.lon)
        RETURNING plant_id
    """)
    pid = conn.execute(q, {
        "region_id": region_id,
        "plant_name": site_name,
        "plant_type": ptype,
        "lat": lat,
        "lon": lon,
    }).scalar()
    if pid:
        return str(pid)

    pid2 = conn.execute(
        text("SELECT plant_id FROM plants WHERE region_id=:rid AND plant_name=:p"),
        {"rid": region_id, "p": site_name},
    ).scalar()
    return str(pid2)


def ensure_run(conn, model_name: str, region_id: str, revision: str,
               t0_raw: str, t0_ist, t0_utc) -> str:
    q = text("""
        INSERT INTO forecast_runs(model_name, region_id, revision, run_t0_utc, run_t0_ist, run_t0_raw)
        VALUES (:m, :rid, :rev, :t0utc, :t0ist, :t0raw)
        ON CONFLICT (model_name, region_id, revision, run_t0_utc)
        DO UPDATE SET
          run_t0_ist = EXCLUDED.run_t0_ist,
          run_t0_raw = EXCLUDED.run_t0_raw
        RETURNING run_id
    """)
    run_id = conn.execute(q, {
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
    # OVERWRITE MODE: drop everything for this run+plant, then insert snapshot from CSV
    conn.execute(
        text("DELETE FROM mi_predictions WHERE run_id=:r AND plant_id=:p"),
        {"r": run_id, "p": plant_id},
    )

    rows: List[Dict] = []
    for _, r in df_group.iterrows():
        vt_raw = r.get("timestamp_raw", "")
        vt_ist = r.get("timestamp_dt")

        if pd.isna(vt_ist):
            continue

        # interpret naive timestamps as IST
        if getattr(vt_ist, "tzinfo", None) is None:
            vt_ist = vt_ist.replace(tzinfo=IST)
        else:
            vt_ist = vt_ist.astimezone(IST)

        vt_utc = vt_ist.astimezone(UTC)

        ghi = r.get("ghi")
        pred = r.get("prediction")

        power_pred = float(pred) if pd.notna(pred) else None
        ghi_pred = float(ghi) if pd.notna(ghi) else None

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
# Ingest one CSV (no skipping)
# -------------------------
def ingest_csv(path: Path, model_name: str) -> bool:
    st = path.stat()
    mtime, size = int(st.st_mtime), int(st.st_size)
    h = sha1_file(path)

    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"[BADFILE] {model_name} {path} read_csv failed: {e}")
        return False

    required = ["revision", "timestamp", "site_name", "source_sheet"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"[BADFILE] {model_name} {path} missing columns: {missing}")
        return False

    df["revision"] = df["revision"].astype(str).str.strip()
    df["source_sheet"] = df["source_sheet"].astype(str).str.strip()
    df["site_name"] = df["site_name"].astype(str).str.strip()

    ts_raw, ts_dt = parse_timestamps_no_warning(df["timestamp"])
    df["timestamp_raw"] = ts_raw
    df["timestamp_dt"] = ts_dt

    # ---- NEW: file-level timestamp coverage debug ----
    total_rows = int(len(df))
    parsed_ok = int(df["timestamp_dt"].notna().sum())
    parsed_bad = total_rows - parsed_ok

    dt_ist_all = _series_to_ist(df["timestamp_dt"])
    file_dates = _dates_from_dt_ist(dt_ist_all)
    file_missing = _missing_dates(file_dates, max_span_days=120)

    print(f"[FILE] {model_name} path={path}")
    print(f"       rows={total_rows} parsed_ok={parsed_ok} parsed_failed={parsed_bad}")
    if file_dates:
        print(f"       IST date coverage: {file_dates[0].isoformat()} -> {file_dates[-1].isoformat()}  (n_dates={len(file_dates)})")
        if file_missing:
            # This is the key line for your Feb 1–8 gap problem
            print(f"       MISSING dates inside range: {_fmt_dates(file_missing, max_items=40)}")
        else:
            print(f"       Missing dates inside range: []")
    else:
        print(f"       IST date coverage: NONE (timestamps not parsing?)")

    grouped = df.groupby(["source_sheet", "site_name", "revision"], dropna=False)

    file_total_points = 0
    runs_with_points = 0
    per_plant_counts: List[Tuple[str, str, str, int]] = []

    with engine.begin() as conn:
        ensure_schema(conn)

        prev = get_last_ingested(conn, path)
        status = "NEW"
        if prev:
            prev_sha1 = prev[4]
            status = "UNCHANGED_REINGEST" if (prev_sha1 == h) else "CHANGED_REINGEST"

        # ---- NEW: per-file union of group run dates (t0 dates) ----
        t0_dates_union: set = set()

        for (source_sheet, site_name, revision), g in grouped:
            source_sheet = str(source_sheet or "").strip()
            site_name = str(site_name or "").strip()
            revision = str(revision or "").strip()
            if not source_sheet or not site_name or not revision:
                continue

            # group stats
            g_rows = int(len(g))
            g_ok = int(g["timestamp_dt"].notna().sum())
            g_bad = g_rows - g_ok

            # group date coverage (IST)
            g_dt_ist = _series_to_ist(g["timestamp_dt"])
            g_dates = _dates_from_dt_ist(g_dt_ist)
            g_missing = _missing_dates(g_dates, max_span_days=120)

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
                print(f"  [GROUP] {source_sheet} | {site_name} | {revision} rows={g_rows} ok={g_ok} bad={g_bad} -> t0=NaT (SKIP RUN) dates={_fmt_dates(g_dates)}")
                if g_missing:
                    print(f"          missing_dates={_fmt_dates(g_missing, max_items=40)}")
                per_plant_counts.append((source_sheet, site_name, revision, 0))
                continue

            # interpret t0 as IST
            if getattr(t0_dt, "tzinfo", None) is None:
                t0_ist = t0_dt.replace(tzinfo=IST)
            else:
                t0_ist = t0_dt.astimezone(IST)

            t0_utc = t0_ist.astimezone(UTC)

            try:
                t0_raw = g.loc[g["timestamp_dt"].idxmin(), "timestamp_raw"]
            except Exception:
                t0_raw = ""

            # keep union of run dates (t0 date in IST)
            try:
                t0_dates_union.add(t0_ist.date())
            except Exception:
                pass

            run_id = ensure_run(conn, model_name, region_id, revision, t0_raw, t0_ist, t0_utc)
            if not run_id:
                print(f"  [GROUP] {source_sheet} | {site_name} | {revision} -> ensure_run FAILED")
                per_plant_counts.append((source_sheet, site_name, revision, 0))
                continue

            plant_type = infer_plant_type(site_name)
            n_inserted = overwrite_predictions(conn, run_id, plant_id, g, plant_type)

            file_total_points += n_inserted
            if n_inserted > 0:
                runs_with_points += 1

            # ---- NEW: group-level debug print with run date + date coverage ----
            t0_show = ""
            try:
                t0_show = t0_ist.strftime("%Y-%m-%d %H:%M:%S %Z")
            except Exception:
                t0_show = str(t0_ist)

            print(
                f"  [GROUP] {source_sheet} | {site_name} | {revision} "
                f"rows={g_rows} ok={g_ok} bad={g_bad} "
                f"t0_raw='{t0_raw}' t0_ist={t0_show} run_id={run_id} inserted={n_inserted}"
            )
            if g_dates:
                print(f"          dates: {g_dates[0].isoformat()} -> {g_dates[-1].isoformat()} (n={len(g_dates)})")
                if g_missing:
                    print(f"          missing_dates={_fmt_dates(g_missing, max_items=40)}")
            else:
                print(f"          dates: NONE (group timestamps not parsing?)")

            per_plant_counts.append((source_sheet, site_name, revision, n_inserted))

        mark_ingested(conn, path, model_name, mtime, size, h)

    # ---- NEW: show union of run dates created/updated from this file ----
    if t0_dates_union:
        t0_dates_sorted = sorted(t0_dates_union)
        miss_t0 = _missing_dates(t0_dates_sorted, max_span_days=120)
        print(f"[RUN DATES] {model_name} file produced/updated run_t0 IST dates: {t0_dates_sorted[0]} -> {t0_dates_sorted[-1]} (n={len(t0_dates_sorted)})")
        if miss_t0:
            print(f"            missing run dates inside range: {_fmt_dates(miss_t0, max_items=40)}")
    else:
        print(f"[RUN DATES] {model_name} file produced/updated run_t0 IST dates: NONE")

    print(f"[INGEST] {model_name} {status} file={path} runs_with_points={runs_with_points} points={file_total_points}")

    for (src, site, rev, n) in sorted(per_plant_counts, key=lambda x: (x[0], x[1], x[2])):
        print(f"  - {src} | {site} | {rev} -> rows={n}")

    if file_total_points == 0:
        sample = df["timestamp"].astype(str).dropna().head(10).tolist()
        print(f"  !! ZERO rows inserted. Sample timestamps: {sample}")

    return True


# -------------------------
# Scan roots
# -------------------------
def list_csv_files(root: Path) -> List[Path]:
    return sorted(set(list(root.rglob("*.csv")) + list(root.rglob("*.CSV"))), key=str)


def scan_root(root: str, model_name: str):
    if not root:
        print(f"[WARN] {model_name} root is empty")
        return

    rp = Path(root)
    if not rp.exists():
        print(f"[WARN] {model_name} root not found: {rp}")
        return

    files = list_csv_files(rp)
    print(f"[SCAN] {model_name} root={rp} files={len(files)}")

    for i, p in enumerate(files, start=1):
        print(f"  [{i:04d}] {p}")

    ok = 0
    for p in files:
        try:
            if ingest_csv(p, model_name):
                ok += 1
        except Exception as e:
            print(f"[ERROR] {model_name} {p}: {e}")

    print(f"[SCAN DONE] {model_name} processed={ok}/{len(files)}")


def main():
    print("Folder watcher ingest started.")
    print(f"DATABASE_URL={DATABASE_URL}")
    print(f"NOWCAST_ROOT={NOWCAST_ROOT}")
    print(f"MEDIUM_ROOT={MEDIUM_ROOT}")
    print(f"DAYFIRST={DAYFIRST}")
    print(f"SINGLE_PASS={SINGLE_PASS}")
    print("MODE=OVERWRITE (no skipping; always delete+insert)")

    while True:
        scan_root(NOWCAST_ROOT, "NOWCAST")
        scan_root(MEDIUM_ROOT, "MEDIUM")

        if SINGLE_PASS:
            break

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
