#!/usr/bin/env bash
set -euo pipefail
export TZ="Asia/Kolkata"

BASE1="$HOME/Documents/GRID_INDIA/Junaid/Pipeline/code"
BASE2="$HOME/Documents/GRID_INDIA/PANKHI/new_solar"

LOGDIR="$BASE2/logs"
mkdir -p "$LOGDIR"

CONDA="$HOME/anaconda3/bin/conda"

ENV_OP="rt_nowcast"
ENV_MED="power_model"

OP_SCRIPT="$BASE1/Operational_medium_range.py"
MED_SCRIPT="$BASE2/run_medium.py"

LOGFILE="$LOGDIR/evening_medium_$(date +%Y%m%d).log"

# Prevent overlap if cron triggers again
LOCK="/tmp/evening_medium.lock"
exec 9>"$LOCK"
flock -n 9 || exit 0

echo "[$(date)] START" >> "$LOGFILE"

# Run operational script in rt_nowcast
"$CONDA" run -n "$ENV_OP" --no-capture-output python "$OP_SCRIPT" >> "$LOGFILE" 2>&1

# Run run_medium in power_model
"$CONDA" run -n "$ENV_MED" --no-capture-output python "$MED_SCRIPT" >> "$LOGFILE" 2>&1

echo "[$(date)] DONE" >> "$LOGFILE"
