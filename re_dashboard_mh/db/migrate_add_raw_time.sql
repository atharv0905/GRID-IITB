-- Adds raw Excel timestamp + IST columns for precise time tracking

ALTER TABLE forecast_runs
  ADD COLUMN IF NOT EXISTS run_t0_raw TEXT,
  ADD COLUMN IF NOT EXISTS run_t0_ist TIMESTAMPTZ;

ALTER TABLE mi_predictions
  ADD COLUMN IF NOT EXISTS valid_time_raw TEXT;

CREATE INDEX IF NOT EXISTS idx_pred_run_plant_time_ist
  ON mi_predictions(run_id, plant_id, valid_time_ist);

CREATE INDEX IF NOT EXISTS idx_runs_t0_ist
  ON forecast_runs(run_t0_ist DESC);