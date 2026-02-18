# api/main.py
from __future__ import annotations
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import execute_values

import os
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Query
from sqlalchemy import create_engine, text
import uvicorn

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://reuser:repass@10.135.5.11:5432/redb"
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

app = FastAPI(title="Forecast API")

class LoginRequest(BaseModel):
    username: str
    password: str
    
@app.get("/health")
def health():
    return {"ok": True}

def authenticate_user(username: str, password: str) -> bool:
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        query = """
            SELECT 1
            FROM users
            WHERE uname = %s
              AND password = crypt(%s, password)
        """
        cur.execute(query, (username, password))
        result = cur.fetchone()

        cur.close()
        conn.close()

        return result is not None

    except Exception as e:
        print("Auth DB error:", e)
        return False
    
@app.post("/login")
def login(request: LoginRequest):
    print(f"Login attempt: {request.username}")
    if authenticate_user(request.username, request.password):
        return {"success": True, "message": "Login successful"}
    else:
        return {"success": False, "message": "Invalid credentials"}

class DataWrapper(BaseModel):
    data: Dict[str, Any]

def insert_regions(cur, rows):
    if not rows:
        return 0

    query = """
    INSERT INTO regions (region_id, region_code, region_name, created_at)
    VALUES %s
    ON CONFLICT (region_code) DO NOTHING
    """

    values = [
        (r["region_id"], r["region_code"], r["region_name"], r["created_at"])
        for r in rows
    ]

    execute_values(cur, query, values)
    return cur.rowcount

def insert_users(cur, rows):
    if not rows:
        return 0

    query = """
    INSERT INTO users (user_id, uname, password)
    VALUES %s
    ON CONFLICT (uname) DO NOTHING
    """

    values = [(u["user_id"], u["uname"], u["password"]) for u in rows]
    execute_values(cur, query, values)
    return cur.rowcount

def insert_plants(cur, rows):
    if not rows:
        return 0

    query = """
    INSERT INTO plants
    (plant_id, plant_name, region_id, plant_type, lat, lon, created_at)
    VALUES %s
    ON CONFLICT (region_id, plant_name) DO NOTHING
    """

    values = [
        (
            p["plant_id"], p["plant_name"], p["region_id"],
            p["plant_type"], p["lat"], p["lon"], p["created_at"]
        )
        for p in rows
    ]

    execute_values(cur, query, values)
    return cur.rowcount

def insert_runs(cur, rows):
    if not rows:
        return 0

    query = """
    INSERT INTO forecast_runs
    (run_id, model_name, region_id, revision, run_t0_utc, run_t0_ist, run_t0_raw, created_at)
    VALUES %s
    ON CONFLICT (model_name, region_id, revision, run_t0_utc) DO NOTHING
    """

    values = [
        (
            r["run_id"], r["model_name"], r["region_id"], r["revision"],
            r["run_t0_utc"], r["run_t0_ist"], r["run_t0_raw"], r["created_at"]
        )
        for r in rows
    ]

    execute_values(cur, query, values)
    return cur.rowcount

def insert_predictions(cur, rows):
    if not rows:
        return 0

    query = """
    INSERT INTO mi_predictions
    (run_id, plant_id, valid_time_utc, valid_time_ist, valid_time_raw,
     ghi_pred_wm2, ghi_actual_wm2, power_pred_mw, power_actual_mw,
     solar_power_pred_mw, solar_power_actual_mw,
     wind_power_pred_mw, wind_power_actual_mw)
    VALUES %s
    ON CONFLICT (run_id, plant_id, valid_time_utc) DO NOTHING
    """

    values = [
        (
            r["run_id"], r["plant_id"], r["valid_time_utc"], r["valid_time_ist"], r["valid_time_raw"],
            r["ghi_pred_wm2"], r["ghi_actual_wm2"], r["power_pred_mw"], r["power_actual_mw"],
            r["solar_power_pred_mw"], r["solar_power_actual_mw"],
            r["wind_power_pred_mw"], r["wind_power_actual_mw"]
        )
        for r in rows
    ]

    execute_values(cur, query, values, page_size=1000)
    return cur.rowcount

def insert_files(cur, rows):
    if not rows:
        return 0

    query = """
    INSERT INTO ingested_files (path, mtime, size, sha1, model_name, ingested_at)
    VALUES %s
    ON CONFLICT (path) DO NOTHING
    """

    values = [
        (f["path"], f["mtime"], f["size"], f["sha1"], f["model_name"], f["ingested_at"])
        for f in rows
    ]

    execute_values(cur, query, values)
    return cur.rowcount

@app.post("/data")
def receive_data(wrapper: DataWrapper):
    data = wrapper.data
    counts = {}

    conn = engine.raw_connection()   # get pooled connection

    try:
        cur = conn.cursor()

        counts["regions"] = insert_regions(cur, data.get("regions", []))
        counts["users"] = insert_users(cur, data.get("users", []))
        counts["plants"] = insert_plants(cur, data.get("plants", []))
        counts["runs"] = insert_runs(cur, data.get("forecast_runs", []))
        counts["predictions"] = insert_predictions(cur, data.get("mi_predictions", []))
        counts["files"] = insert_files(cur, data.get("ingested_files", []))

        conn.commit()   # commit transaction
        cur.close()

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        conn.close()   # VERY IMPORTANT (returns to pool)

    print("Inserted rows:", counts)

    return {
        "status": "success",
        "inserted_rows": counts
    }

@app.get("/regions")
def regions():
    q = """
    SELECT region_id, region_code, region_name
    FROM regions
    ORDER BY region_name
    """
    with engine.begin() as conn:
        rows = conn.execute(text(q)).mappings().all()
    return {"items": list(rows)}


@app.get("/plants")
def plants(region_id: Optional[str] = None):
    # region_id=None => all plants
    if region_id and region_id != "ALL":
        q = """
        SELECT plant_id, plant_name, plant_type, lat, lon, region_id
        FROM plants
        WHERE region_id = :rid
        ORDER BY plant_name
        """
        params = {"rid": region_id}
    else:
        q = """
        SELECT plant_id, plant_name, plant_type, lat, lon, region_id
        FROM plants
        ORDER BY plant_name
        """
        params = {}

    with engine.begin() as conn:
        rows = conn.execute(text(q), params).mappings().all()
    return {"items": list(rows)}


@app.get("/runs")
def runs(
    limit: int = Query(400, ge=1, le=5000),
    model_name: Optional[str] = None,
    region_id: Optional[str] = None,
):
    # model_name: NOWCAST / MEDIUM / INTRA / INTER
    # region_id: specific UUID (or None for all)
    #
    # Important:
    # - Postgres TIMESTAMPTZ is stored normalized in UTC.
    # - Returning run_t0_ist directly will *display* as UTC unless the DB/session timezone is set.
    # So we also return a computed, explicit IST-local timestamp derived from run_t0_utc.
    q = """
    SELECT
      run_id,
      model_name,
      region_id,
      revision,
      run_t0_utc,
      timezone('Asia/Kolkata', run_t0_utc) AS run_t0_ist_local,
      run_t0_raw,
      created_at
    FROM forecast_runs
    WHERE (:m IS NULL OR UPPER(model_name) = UPPER(:m))
      AND (:rid IS NULL OR region_id = :rid)
    ORDER BY run_t0_utc DESC
    LIMIT :lim
    """
    with engine.begin() as conn:
        rows = conn.execute(
            text(q),
            {"m": model_name, "rid": region_id, "lim": limit}
        ).mappings().all()
    return {"items": list(rows)}


@app.get("/series")
def series(plant_id: str, run_id: str):
    q = """
    SELECT
      valid_time_raw,
      valid_time_utc,
      timezone('Asia/Kolkata', valid_time_utc) AS valid_time_ist_local,
      ghi_pred_wm2,
      ghi_actual_wm2,
      power_pred_mw,
      power_actual_mw,
      solar_power_pred_mw,
      solar_power_actual_mw,
      wind_power_pred_mw,
      wind_power_actual_mw
    FROM mi_predictions
    WHERE plant_id = :pid AND run_id = :rid
    ORDER BY valid_time_utc
    """
    with engine.begin() as conn:
        rows = conn.execute(text(q), {"pid": plant_id, "rid": run_id}).mappings().all()
    return {"run_id": run_id, "items": list(rows)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
