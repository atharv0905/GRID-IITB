-- db/init.sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- -------------------------
-- Regions = unique source_sheet
-- -------------------------
CREATE TABLE IF NOT EXISTS regions (
  region_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  region_code TEXT UNIQUE NOT NULL,   -- store raw source_sheet
  region_name TEXT NOT NULL,          -- display label (can be same as code)
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- -------------------------
-- User accounts
-- -------------------------
CREATE TABLE IF NOT EXISTS users (
  user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  uname TEXT UNIQUE NOT NULL,
  password TEXT NOT NULL
);

-- -------------------------
-- Plants = unique site_name, tied to a region (source_sheet bucket)
-- -------------------------
CREATE TABLE IF NOT EXISTS plants (
  plant_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  plant_name TEXT NOT NULL,           -- site_name
  region_id UUID NOT NULL REFERENCES regions(region_id) ON DELETE CASCADE,
  plant_type TEXT NOT NULL DEFAULT 'SOLAR',  -- SOLAR/WIND (best-effort inference)
  lat DOUBLE PRECISION,
  lon DOUBLE PRECISION,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (region_id, plant_name)
);

-- -------------------------
-- Forecast runs = (model_name, region, revision, run_t0_utc)
-- model_name: NOWCAST / MEDIUM / INTRA / INTER
-- revision: R1/R2/...
-- -------------------------
CREATE TABLE IF NOT EXISTS forecast_runs (
  run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  model_name TEXT NOT NULL,
  region_id UUID NOT NULL REFERENCES regions(region_id) ON DELETE CASCADE,
  revision TEXT NOT NULL,

  run_t0_utc TIMESTAMPTZ NOT NULL,
  run_t0_ist TIMESTAMPTZ,
  run_t0_raw TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (model_name, region_id, revision, run_t0_utc)
);

CREATE INDEX IF NOT EXISTS idx_runs_model_region ON forecast_runs(model_name, region_id);
CREATE INDEX IF NOT EXISTS idx_runs_t0 ON forecast_runs(run_t0_utc DESC);

-- -------------------------
-- Timeseries rows (one per valid_time)
-- Store both generic + solar/wind power fields to keep dashboard flexible
-- -------------------------
CREATE TABLE IF NOT EXISTS mi_predictions (
  id BIGSERIAL PRIMARY KEY,
  run_id UUID NOT NULL REFERENCES forecast_runs(run_id) ON DELETE CASCADE,
  plant_id UUID NOT NULL REFERENCES plants(plant_id) ON DELETE CASCADE,

  valid_time_utc TIMESTAMPTZ NOT NULL,
  valid_time_ist TIMESTAMPTZ,
  valid_time_raw TEXT,

  ghi_pred_wm2 DOUBLE PRECISION,
  ghi_actual_wm2 DOUBLE PRECISION,
  power_pred_mw DOUBLE PRECISION,
  power_actual_mw DOUBLE PRECISION,
  solar_power_pred_mw DOUBLE PRECISION,
  solar_power_actual_mw DOUBLE PRECISION,
  wind_power_pred_mw DOUBLE PRECISION,
  wind_power_actual_mw DOUBLE PRECISION,

  UNIQUE (run_id, plant_id, valid_time_utc)
);


CREATE INDEX IF NOT EXISTS idx_pred_run_plant_time ON mi_predictions(run_id, plant_id, valid_time_utc);

-- -------------------------
-- Track ingested files so rescans don't duplicate
-- -------------------------
CREATE TABLE IF NOT EXISTS ingested_files (
  path TEXT PRIMARY KEY,
  mtime BIGINT NOT NULL,
  size BIGINT NOT NULL,
  sha1 TEXT NOT NULL,
  model_name TEXT NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
