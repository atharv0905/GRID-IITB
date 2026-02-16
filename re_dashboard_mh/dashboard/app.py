# dashboard/app.py
from __future__ import annotations

import os
import json
import math
import re
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote
import requests
import pandas as pd
from flask import session, redirect

from dash import Dash, html, dcc, Input, Output, State, ctx, ALL, no_update
import dash_bootstrap_components as dbc
import dash_leaflet as dl
import plotly.graph_objects as go
from flask_caching import Cache

import psycopg2
import psycopg2.extras

from datetime import timedelta

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://reuser:repass@localhost:5432/redb"
)

try:
    import geopandas as gpd
except Exception:
    gpd = None


# -----------------------------
# Config
# -----------------------------
DAYFIRST = os.environ.get("DAYFIRST", "1").strip() not in ("0", "false", "False", "no")

API_BASE = os.environ.get("API_BASE", "http://10.135.5.11:8000")
SHAPEFILE_PATH = os.environ.get("SHAPEFILE_PATH", "../data/India_State_Boundary_FIXED_4326.shp")

FORECAST_ORDER = ["Nowcasting", "Intra", "Inter", "Medium"]
FORECAST_TO_MODEL = {
    "Nowcasting": "NOWCAST",
    "Intra": "INTRA",
    "Inter": "INTER",
    "Medium": "MEDIUM",
}
REGION_CODE_TO_NAME = {
    "MH": "Maharashtra",
    "MP": "Madhya Pradesh",
}
COLOR_SOLAR = "#F4A340"
COLOR_WIND  = "#4A90E2"
BORDER_COLOR = "#000000"
BORDER_WEIGHT = 2.6
BORDER_OPACITY = 1.0
SELECTED_RING = "#FFD166"

SESSION = requests.Session()
SESSION.headers.update({"Connection": "keep-alive"})


def api_get(path: str, params: Optional[dict] = None) -> dict:
    url = f"{API_BASE}{path}"
    r = SESSION.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


# -----------------------------
# Dash app + caching
# -----------------------------
app = Dash(__name__, external_stylesheets=[dbc.themes.DARKLY], suppress_callback_exceptions=True)
server = app.server

# For Login
server.secret_key = os.environ.get("SECRET_KEY", "super-secret-key")
# Session configuration
server.config["PERMANENT_SESSION_LIFETIME"] = timedelta(minutes=60)
server.config["SESSION_REFRESH_EACH_REQUEST"] = True  # resets timer on activity

cache = Cache(
    server,
    config={
        "CACHE_TYPE": "filesystem",
        "CACHE_DIR": "./.dash_cache",
        "CACHE_DEFAULT_TIMEOUT": 120,
    },
)


# -----------------------------
# Cached API wrappers (NEW endpoints)
# -----------------------------
@cache.memoize(timeout=120)
def get_regions() -> List[dict]:
    return api_get("/regions")["items"]

@cache.memoize(timeout=120)
def get_plants(region_id: Optional[str] = None) -> List[dict]:
    params = {}
    if region_id:
        params["region_id"] = region_id
    return api_get("/plants", params=params)

def get_runs(limit: int = 800, model_name: Optional[str] = None, region_id: Optional[str] = None) -> List[dict]:
    params = {"limit": limit}
    if model_name:
        params["model_name"] = model_name
    if region_id:
        params["region_id"] = region_id
    return api_get("/runs", params=params)["items"]


def get_series(plant_id: str, run_id: str) -> List[dict]:
    return api_get("/series", params={"plant_id": plant_id, "run_id": run_id}).get("items", [])


# -----------------------------
# Boundary load once
# -----------------------------
def load_boundary_geojson(path: str) -> Tuple[Optional[dict], str]:
    if not path or not os.path.exists(path):
        return None, "Shapefile not found. Set SHAPEFILE_PATH."
    if gpd is None:
        return None, "geopandas not installed."
    try:
        gdf = gpd.read_file(path)
        try:
            gdf = gdf.to_crs(epsg=4326)
        except Exception:
            pass
        if gdf.empty:
            return None, "Boundary has 0 features."
        geo = json.loads(gdf.to_json())
        return geo, f"Loaded boundary features: {len(geo.get('features', []))}"
    except Exception as e:
        return None, f"Boundary load failed: {e}"

BOUNDARY_GEO, BOUNDARY_MSG = load_boundary_geojson(SHAPEFILE_PATH)


# -----------------------------
# Helpers
# -----------------------------
def available_run_dates_ist(runs: List[dict]) -> List[pd.Timestamp]:
    dates = set()
    for r in runs or []:
        dt = _parse_run_t0_dt_ist(r)
        if pd.notna(dt):
            dates.add(dt.date())
    return sorted(dates)

def _svg_data_uri(svg: str) -> str:
    return "data:image/svg+xml;charset=utf-8," + quote(svg)
def _parse_iso_date(s):
    """Dash DatePicker returns 'YYYY-MM-DD'. Force parse to avoid DD-MM vs MM-DD flips."""
    if not s:
        return None
    d = pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")
    return None if pd.isna(d) else d.date()

def plant_icon_dict(ptype: str, is_sel: bool) -> dict:
    t = (ptype or "").lower()
    is_wind = "wind" in t

    size = 34 if is_sel else 26
    stroke_w = 4 if is_sel else 3

    fill = marker_color(ptype)  # solar/wind background color
    ring = SELECTED_RING if is_sel else "#111111"
    glyph = "#FFFFFF"  # icon lines

    if is_wind:
        # Simple wind-turbine glyph on colored circle
        svg = f"""
        <svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" viewBox="0 0 32 32">
          <circle cx="16" cy="16" r="12" fill="{fill}" stroke="{ring}" stroke-width="{stroke_w}"/>
          <line x1="16" y1="16" x2="16" y2="28" stroke="{glyph}" stroke-width="2.2" stroke-linecap="round"/>
          <circle cx="16" cy="14" r="2.2" fill="{glyph}"/>
          <line x1="16" y1="14" x2="16" y2="5" stroke="{glyph}" stroke-width="2.2" stroke-linecap="round"/>
          <line x1="16" y1="14" x2="25" y2="18" stroke="{glyph}" stroke-width="2.2" stroke-linecap="round"/>
          <line x1="16" y1="14" x2="7" y2="18" stroke="{glyph}" stroke-width="2.2" stroke-linecap="round"/>
        </svg>
        """
    else:
        # Sun glyph on colored circle
        svg = f"""
        <svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" viewBox="0 0 32 32">
          <circle cx="16" cy="16" r="12" fill="{fill}" stroke="{ring}" stroke-width="{stroke_w}"/>
          <circle cx="16" cy="16" r="4.5" fill="none" stroke="{glyph}" stroke-width="2.2"/>
          <line x1="16" y1="4"  x2="16" y2="8"  stroke="{glyph}" stroke-width="2.2" stroke-linecap="round"/>
          <line x1="16" y1="24" x2="16" y2="28" stroke="{glyph}" stroke-width="2.2" stroke-linecap="round"/>
          <line x1="4"  y1="16" x2="8"  y2="16" stroke="{glyph}" stroke-width="2.2" stroke-linecap="round"/>
          <line x1="24" y1="16" x2="28" y2="16" stroke="{glyph}" stroke-width="2.2" stroke-linecap="round"/>
          <line x1="7"  y1="7"  x2="10" y2="10" stroke="{glyph}" stroke-width="2.2" stroke-linecap="round"/>
          <line x1="22" y1="22" x2="25" y2="25" stroke="{glyph}" stroke-width="2.2" stroke-linecap="round"/>
          <line x1="22" y1="10" x2="25" y2="7"  stroke="{glyph}" stroke-width="2.2" stroke-linecap="round"/>
          <line x1="7"  y1="25" x2="10" y2="22" stroke="{glyph}" stroke-width="2.2" stroke-linecap="round"/>
        </svg>
        """

    return {
        "iconUrl": _svg_data_uri(svg),
        "iconSize": [size, size],
        "iconAnchor": [size / 2, size / 2],
    }



def _norm_code(x: str) -> str:
    # keep only letters, uppercase (e.g., "M.H." -> "MH")
    return re.sub(r"[^A-Za-z]", "", str(x or "")).upper()

def region_display_name(r: dict) -> str:
    raw_name = str(r.get("region_name") or "").strip()
    raw_code = str(r.get("region_code") or "").strip()
    raw_id   = str(r.get("region_id") or "").strip()

    name_norm = _norm_code(raw_name)
    code_norm = _norm_code(raw_code) or name_norm or _norm_code(raw_id)

    # If region_name is a real full name, keep it.
    # If it's just a short code (MH/MP), map it.
    if raw_name and not re.fullmatch(r"[A-Z]{2,3}", name_norm):
        return raw_name

    return REGION_CODE_TO_NAME.get(code_norm, raw_name or raw_code or raw_id or "Region")

def marker_color(ptype: str) -> str:
    t = (ptype or "").lower()
    return COLOR_WIND if "wind" in t else COLOR_SOLAR

def series_to_df(items: List[dict]) -> pd.DataFrame:
    df = pd.DataFrame(items)
    if df.empty:
        return df

    # -------------------------
    # Time handling (fixes "UTC-looking" display)
    # -------------------------
    # 1) Prefer valid_time_raw (exact string from CSV).
    # 2) If missing, use valid_time_ist_local (computed in API from valid_time_utc).
    # 3) For plotting, interpret the chosen timestamp as IST.
    #
    # This avoids the common Postgres TIMESTAMPTZ behaviour where values are stored in UTC
    # and later *rendered* in UTC, which makes an IST run look like "previous day 23:30".
    raw_series = None
    if "valid_time_raw" in df.columns and df["valid_time_raw"].notna().any():
        raw_series = df["valid_time_raw"]
    elif "valid_time_ist_local" in df.columns and df["valid_time_ist_local"].notna().any():
        raw_series = df["valid_time_ist_local"]

    df["timestamp_raw"] = raw_series.astype(str) if raw_series is not None else None

    # Parse raw timestamp for plotting/sorting
    if df["timestamp_raw"].notna().any():
        ts = pd.to_datetime(df["timestamp_raw"], errors="coerce")
    else:
        ts = pd.Series([pd.NaT] * len(df))

    # Parse UTC fallback (tz-aware)
    if "valid_time_utc" in df.columns:
        df["valid_time_utc"] = pd.to_datetime(df["valid_time_utc"], errors="coerce", utc=True)

    # Ensure plot timestamp is IST-aware
    if getattr(ts.dt, "tz", None) is None:
        # interpret naive timestamps as IST
        try:
            ts = ts.dt.tz_localize("Asia/Kolkata")
        except Exception:
            pass

    # Fallback to UTC->IST conversion when raw missing/unparseable
    if "valid_time_utc" in df.columns:
        ts_fallback = df["valid_time_utc"].dt.tz_convert("Asia/Kolkata")
        ts = ts.fillna(ts_fallback)

    df["timestamp"] = ts

    # For table display: show raw string if present, else format IST local time
    df["timestamp_str"] = df["timestamp_raw"]
    if "timestamp" in df.columns:
        df["timestamp_str"] = df["timestamp_str"].fillna(
            df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
        )

    df = df.sort_values("timestamp")
    return df


def plot_pred_vs_actual(df: pd.DataFrame, pred_col: str, actual_col: str, title: str, ytitle: str) -> go.Figure:
    fig = go.Figure()
    if pred_col in df.columns and df[pred_col].notna().any():
        fig.add_trace(go.Scatter(
            x=df["timestamp"], y=df[pred_col],
            mode="lines+markers", name="Prediction",
            marker=dict(size=4)
        ))
    if actual_col in df.columns and df[actual_col].notna().any():
        fig.add_trace(go.Scatter(
            x=df["timestamp"], y=df[actual_col],
            mode="lines", name="Actual",
            line=dict(width=2)
        ))
    if len(fig.data) == 0:
        fig.add_annotation(text="No data", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)

    fig.update_layout(
        template="plotly_dark",
        height=320,
        margin=dict(l=12, r=12, t=44, b=10),
        title=title,
        xaxis_title="Time",
        yaxis_title=ytitle,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="rgba(233,238,246,.90)"),
    )
    return fig

def parse_revision_num(rev: str) -> int:
    # "R1" -> 1, "R12" -> 12, else 0
    m = re.search(r"(\d+)", str(rev or ""))
    return int(m.group(1)) if m else 0
def _parse_run_t0_dt(run: dict) -> pd.Timestamp:
    """
    Parse run t0 using preference:
      run_t0_raw -> run_t0_ist_local -> run_t0_utc
    Returns pandas Timestamp (NaT if cannot parse).
    """
    s = (run.get("run_t0_raw") or run.get("run_t0_ist_local") or run.get("run_t0_utc") or "").strip()
    if not s:
        return pd.NaT
    # robust parse; utc=True will handle +00:00 strings, naive strings become UTC then
    try:
        return pd.to_datetime(s, errors="coerce", utc=True)
    except Exception:
        return pd.NaT


def filter_runs_to_latest_t0(runs: List[dict]) -> List[dict]:
    """
    Keep only runs that belong to the latest run_t0 (latest cycle).
    (Kept for reference; the revision dropdown now filters by *date* instead.)
    """
    if not runs:
        return []
    dts = [(_parse_run_t0_dt(r), r) for r in runs]
    dts = [(dt, r) for dt, r in dts if pd.notna(dt)]
    if not dts:
        return runs  # if parsing failed, fall back to all
    latest_dt = max(dt for dt, _ in dts)
    return [r for dt, r in dts if dt == latest_dt]

def _parse_run_t0_dt_ist(run: dict) -> pd.Timestamp:
    """
    Parse run t0 and return a tz-aware IST timestamp.

    Preference:
      1) run_t0_raw / run_t0_ist_local (treated as IST if naive)
      2) run_t0_utc (treated as UTC and converted to IST)
    """
    s = (run.get("run_t0_raw") or run.get("run_t0_ist_local") or "").strip()
    if s:
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return pd.NaT
        try:
            if getattr(dt, "tzinfo", None) is None:
                dt = dt.tz_localize("Asia/Kolkata")
            else:
                dt = dt.tz_convert("Asia/Kolkata")
        except Exception:
            pass
        return dt

    s = (run.get("run_t0_utc") or "").strip()
    if not s:
        return pd.NaT
    dt = pd.to_datetime(s, errors="coerce", utc=True)
    if pd.isna(dt):
        return pd.NaT
    try:
        return dt.tz_convert("Asia/Kolkata")
    except Exception:
        return dt
    
def filter_runs_to_date_ist(runs: List[dict], target_date_ist) -> List[dict]:
    """
    Keep only runs whose run_t0 falls on `target_date_ist` in IST (date-based filtering).

    This fixes your issue: each revision can have a different run_t0 timestamp,
    so filtering to a single latest run_t0 hides older revisions.

    Fallback:
      If no runs match the target date, keep runs from the most recent available date.
    """
    if not runs:
        return []

    parsed = [(_parse_run_t0_dt_ist(r), r) for r in runs]
    parsed = [(dt, r) for dt, r in parsed if pd.notna(dt)]
    if not parsed:
        return runs

    matches = [r for dt, r in parsed if dt.date() == target_date_ist]
    if matches:
        return matches

    latest_date = max(dt for dt, _ in parsed).date()
    return [r for dt, r in parsed if dt.date() == latest_date]

    
def choose_run_by_revision(runs: List[dict], revision_choice: str) -> Optional[dict]:
    """
    Select a run from the provided `runs` list by revision.

    NOTE: `runs` should already be filtered (e.g., to today's date in IST).
    We do NOT restrict to a single latest run_t0, because that hides older revisions.
    """
    if not runs:
        return None

    runs_sorted = sorted(
        runs,
        key=lambda r: parse_revision_num(r.get("revision") or ""),
        reverse=True
    )

    if not revision_choice or revision_choice == "LATEST":
        return runs_sorted[0] if runs_sorted else None

    for r in runs_sorted:
        if (r.get("revision") or "").strip() == revision_choice:
            return r

    return runs_sorted[0] if runs_sorted else None

def get_table_df(plant_id: str, run_id: str) -> pd.DataFrame:
    items = get_series(plant_id, run_id)
    df = series_to_df(items)
    if df.empty:
        return df

    preferred_cols = [
    "timestamp_str",
    "ghi_pred_wm2",
    "ghi_actual_wm2",
    # "power_pred_mw",
    # "power_actual_mw",
    "solar_power_pred_mw",
    "solar_power_actual_mw",
    # "wind_power_pred_mw",
    # "wind_power_actual_mw",
    ]

    cols = [c for c in preferred_cols if c in df.columns]
    tdf = df[cols].copy() if cols else df.copy()

    rename_map = {
    "timestamp_str": "Time",
    "ghi_pred_wm2": "GHI prediction (W/m²)",
    "ghi_actual_wm2": "GHI actual (W/m²)",
    # "power_pred_mw": "Total power prediction (MW)",
    # "power_actual_mw": "Total power prediction (MW))",
    "solar_power_pred_mw": "Solar power prediction (MW)",
    "solar_power_actual_mw": "Solar power actual (MW)",
    # "wind_power_pred_mw": "Wind Power prediction (MW)",
    # "wind_power_actual_mw": "Wind Power actual (MW)",
    }

    tdf = tdf.rename(columns={k: v for k, v in rename_map.items() if k in tdf.columns})

    numeric_cols = tdf.select_dtypes(include=["float64", "float32", "int64", "int32"]).columns
    if len(numeric_cols) > 0:
        tdf[numeric_cols] = tdf[numeric_cols].round(2)

    # if "Time" in tdf.columns:
    #     tdf["Time"] = tdf["Time"].astype(str)

    return tdf


def blank_fig() -> go.Figure:
    f = go.Figure()
    f.update_layout(
        template="plotly_dark",
        height=320,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=12, r=12, t=44, b=10),
    )
    return f


# Login page layout
app.layout = html.Div([
    dcc.Location(id="url"),
    html.Div(id="page-content")
])

def dashboard_layout():
    return dbc.Container(
        fluid=True,
        children=[
        dcc.Store(id="store_regions"),
        dcc.Store(id="store_plants"),
        dcc.Store(id="store_runs"),
        dcc.Store(id="store_selected_run"),
        dcc.Store(id="store_marker_click"),  # ADD THIS LINE
        dcc.Download(id="download_csv"),

        html.Div(
            [
                html.Div(
                    [
                        html.H2("GI-IITB (GRID-INDIA IITB) Experimental Forecast", className="page-title"),
                        html.Div("NOWCASTING • INTRA • INTER • MEDIUM", className="page-subtitle"),
                    ],
                    className="page-title-wrap",
                ),
                dbc.Button(
                    "Logout",
                    id="btn_logout",
                    color="secondary",
                    outline=True,
                    size="sm",
                    className="logout-btn",
                ),
            ],
            className="page-header",
        ),

        dbc.Row(
            [
                # Controls
                dbc.Col(
                    html.Div(
                        [
                            html.H5("Controls", className="section-title"),
                            html.Hr(),

                            dbc.Label("Forecast"),
                            dcc.Dropdown(
                                id="dd_forecast",
                                options=[{"label": k, "value": k} for k in FORECAST_ORDER],
                                value="Nowcasting",
                                clearable=False,
                            ),

                            html.Br(),
                            dbc.Label("Region"),
                            dcc.Dropdown(id="dd_region", options=[], value=None, clearable=False),

                            html.Br(),
                            dbc.Label("Plant"),
                            dcc.Dropdown(id="dd_plant", options=[], value=None, clearable=False),

                            html.Br(),
                            dbc.Label("Revision"),
                            dcc.Dropdown(id="dd_revision", options=[], value="LATEST", clearable=False),
                            html.Br(),
                            dbc.Label("Run date"),
                            dcc.DatePickerSingle(
                                id="dp_run_date",
                                display_format="YYYY-MM-DD",
                                date=None,
                                clearable=True,
                            ),
                            html.Div(id="run_date_hint", style={"fontSize": "12px", "opacity": 0.75, "marginTop": "4px"}),

                            html.Br(),
                            # dbc.Label("Download date range"),
                            # dcc.DatePickerRange(
                            #     id="dp_dl_range",
                            #     display_format="YYYY-MM-DD",
                            #     start_date=None,
                            #     end_date=None,
                            #     clearable=True,
                            # ),
                            # html.Div(id="dl_range_hint", style={"fontSize": "12px", "opacity": 0.75, "marginTop": "4px"}),


                            html.Hr(),
                            dbc.Checklist(
                                id="ck_autorefresh",
                                options=[{"label": "Auto-refresh", "value": "on"}],
                                value=["on"],
                                switch=True,
                            ),

                            dbc.Label("Refresh interval (minutes)"),
                            dcc.Slider(
                                id="sl_refresh",
                                min=5, max=60, step=None,
                                value=5,
                                marks={5:"5",10:"10",15:"15",30:"30",60:"60"},
                            ),

                            dcc.Interval(id="interval", interval=5*60*1000, n_intervals=0, disabled=False),
                        ],
                        className="panel",
                    ),
                    md=3, lg=3, className="h-100",
                ),

                # Map
                dbc.Col(
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H5("Geospatial View", className="geo-title"),
                                    html.Div(id="plant_meta", className="plant-meta-inline"),
                                ],
                                className="map-header",
                            ),
                            dcc.Loading(
                                type="circle",
                                children=html.Div(
                                    dl.Map(
                                        id="map",
                                        center=[22.0, 78.0],
                                        zoom=5,
                                        
                                        style={"width": "100%", "height": "560px", "borderRadius": "14px"},
                                        children=[
                                            dl.LayersControl(
                                                position="topright",
                                                collapsed=True,
                                                children=[
                                                    dl.BaseLayer(
                                                        dl.TileLayer(
                                                            url="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
                                                            attribution="Esri",
                                                        ),
                                                        name="Satellite (Esri)",
                                                        checked=True,
                                                    ),
                                                    dl.BaseLayer(
                                                        dl.TileLayer(
                                                            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
                                                            attribution="OpenStreetMap",
                                                        ),
                                                        name="Street (OSM)",
                                                        checked=False,
                                                    ),
                                                    dl.Overlay(dl.LayerGroup(id="boundary_layer"), name="India boundary", checked=True),
                                                    dl.Overlay(dl.LayerGroup(id="plants_group"), name="Plants", checked=True),
                                                    dl.Overlay(dl.LayerGroup(id="selected_group"), name="Selected", checked=True),
                                                ],
                                            )
                                        ],
                                    ),
                                    className="map-wrap",
                                ),
                            ),
                            html.Div(
                                ("" if BOUNDARY_GEO else f"Boundary: {BOUNDARY_MSG}"),
                                style={"opacity": 0.7, "marginTop": "6px", "fontSize": "12px"},
                            ),
                        ],
                        className="panel map-panel",
                    ),
                    md=7, lg=7, className="h-00",
                ),

                # Summary (vertical)
                dbc.Col(
                    html.Div(
                        [
                            html.H5("Summary", className="section-title"),
                            html.Div(id="kpi_cards", className="summary-stack"),
                        ],
                        className="panel",
                    ),
                    md=2, lg=2, className="h-100",
                ),
            ],
            className="g-3 align-items-stretch",
        ),

        html.Br(),

        # Charts with separate borders
        html.Div(
            [
                html.H5("Forecast Graphs", className="section-title"),
                dcc.Loading(
                    type="circle",
                    children=html.Div(
                        [
                            html.Div(dcc.Graph(id="g1"), className="chart-card"),
                            html.Div(dcc.Graph(id="g2"), className="chart-card"),
                            html.Div(dcc.Graph(id="g3"), className="chart-card charts_section_hide"),
                            html.Div(dcc.Graph(id="g4"), className="chart-card charts_section_hide"),
                        ],
                        className="charts-grid",
                    ),
                ),
            ],
            className="panel",
        ),

        html.Br(),

        # Table + download
        # --- Data header: keep everything in ONE row and avoid ugly wrap ---
        # -------------------------
        # Table + download
        # -------------------------
        html.Div(
            [
                # Header row (Data + range + download)
                html.Div(
                    [
                        html.Div(
                            [
                                html.H5("Data", className="section-title mb-0"),
                                html.Div(id="dl_hint", className="data-hint-inline"),
                            ],
                            className="data-left",
                        ),

                        html.Div(
                            [
                               dcc.DatePickerRange(
                                    id="dp_dl_range",
                                    className="dl-range-compact",
                                    start_date_placeholder_text="From",
                                    end_date_placeholder_text="To",
                                    minimum_nights=0,
                                    display_format="YYYY-MM-DD",
                                    number_of_months_shown=1,
                                    updatemode="bothdates",   # updates only after both dates selected
                                    clearable=True,
                                ),

                                dbc.Button(
                                    "Download CSV",
                                    id="btn_download",
                                    color="secondary",
                                    size="sm",
                                    className="dl-btn",
                                ),
                            ],
                            className="download-bar",
                        ),
                    ],
                    className="data-header",
                ),

                html.Br(),

                # ✅ THIS WAS MISSING (fixes your error)
                dcc.Loading(
                    type="circle",
                    children=html.Div(id="table_container"),
                ),
            ],
            className="panel",
        ),
        ]
    )


# -----------------------------
# Callbacks
# -----------------------------
@app.callback(
    Output("store_regions", "data"),
    Output("dd_region", "options"),
    Output("dd_region", "value"),
    Input("url", "pathname"),
)
def init_regions(_):
    try:
        regions = get_regions()
    except Exception:
        return [], [], None

    opts = [{"label": "All", "value": "ALL"}] + [
        {"label": region_display_name(r), "value": r["region_id"]}
        for r in regions
    ]
    return regions, opts, "ALL"

def normalize_items(x):
    """
    Accepts list/dict/json-string and returns a list[dict].
    This prevents: TypeError: string indices must be integers
    """
    if x is None:
        return []

    # If already list
    if isinstance(x, list):
        return [i for i in x if isinstance(i, dict)]

    # If dict: common API patterns
    if isinstance(x, dict):
        if isinstance(x.get("items"), list):
            return [i for i in x["items"] if isinstance(i, dict)]
        if isinstance(x.get("data"), list):
            return [i for i in x["data"] if isinstance(i, dict)]
        # sometimes dict is {id: {...}, ...}
        vals = list(x.values())
        if vals and all(isinstance(v, dict) for v in vals):
            return vals
        return []

    # If string: maybe JSON string
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return []
        try:
            j = json.loads(s)
            return normalize_items(j)
        except Exception:
            return []

    return []

@app.callback(
    Output("store_plants", "data"),
    Output("dd_plant", "options"),
    Output("dd_plant", "value"),
    Input("dd_region", "value"),
)
def update_plants(region_id):
    if not region_id:
        return [], [], None

    try:
        raw = get_plants(None if region_id == "ALL" else region_id)
    except Exception:
        return [], [], None

    plants = normalize_items(raw)

    # keep only valid rows
    plants = [p for p in plants if isinstance(p, dict) and p.get("plant_id") and p.get("plant_name")]

    opts = [{"label": p["plant_name"], "value": p["plant_id"]} for p in plants]
    default = opts[0]["value"] if opts else None
    return plants, opts, default



@app.callback(
    Output("interval", "interval"),
    Output("interval", "disabled"),
    Input("sl_refresh", "value"),
    Input("ck_autorefresh", "value"),
)
def update_interval(refresh_mins, autoval):
    refresh_mins = int(refresh_mins or 5)
    enabled = (autoval and "on" in autoval)
    return refresh_mins * 60 * 1000, (not enabled)


@app.callback(
    Output("boundary_layer", "children"),
    Input("url", "pathname"),
)
def render_boundary(_):
    if not BOUNDARY_GEO:
        return []
    return dl.GeoJSON(
    data=BOUNDARY_GEO,
    options=dict(
        interactive=False,  # IMPORTANT
        style=dict(color=BORDER_COLOR, weight=BORDER_WEIGHT, opacity=BORDER_OPACITY, fillOpacity=0),
    ),
)

@app.callback(
    Output("plants_group", "children"),
    Input("store_plants", "data"),
    Input("dd_plant", "value"),
)
def build_markers(plants, selected_plant_id):
    plants = plants or []
    markers = []
    for p in plants:
        pid = p["plant_id"]
        is_sel = (pid == selected_plant_id)
        fill = marker_color(p.get("plant_type", ""))

        markers.append(
            dl.Marker(
                id={"type": "plant-marker", "plant_id": pid},
                position=[float(p["lat"]), float(p["lon"])],
                icon=plant_icon_dict(p.get("plant_type", ""), is_sel),
                children=[dl.Tooltip(p["plant_name"])],
                bubblingMouseEvents=True
            )
        )

    return markers


@app.callback(
    Output("selected_group", "children"),
    Output("map", "center"),
    Output("map", "zoom"),
    Input("dd_plant", "value"),
    State("store_plants", "data"),
)
def update_selected_marker(plant_id, plants):
    plants = plants or []
    sel = next((p for p in plants if p.get("plant_id") == plant_id), None)
    if not sel:
        return [], [22.0, 78.0], 5
    lat, lon = float(sel["lat"]), float(sel["lon"])
    return [], [lat, lon], 7
@app.callback(
    Output("dd_plant", "value", allow_duplicate=True),
    Output("store_marker_click", "data"),
    Input("map", "clickData"),            # Listen to map clicks
    State("store_plants", "data"),        # Get list of plant coordinates
    State("dd_plant", "value"),
    prevent_initial_call=True,
)
def click_marker(map_click, plants, current_plant_id):
    """
    Manually find which plant was clicked by comparing coordinates.
    This fixes the 'non-responsive marker' bug after reloads.
    """
    if not map_click or not plants:
        return no_update, no_update

    # 1. Get click coordinates
    click_lat = map_click['latlng']['lat']
    click_lon = map_click['latlng']['lng']

    # 2. Find the nearest plant
    # Threshold: How close (in degrees) the click must be to count. 
    # 0.1 degrees is roughly 10km, usually safe for map markers.
    THRESHOLD = 0.1
    nearest_plant = None
    min_dist = float("inf")

    for p in plants:
        # Calculate simple Euclidean distance
        plat, plon = float(p['lat']), float(p['lon'])
        dist = math.sqrt((click_lat - plat)**2 + (click_lon - plon)**2)
        
        if dist < min_dist:
            min_dist = dist
            nearest_plant = p

    # 3. If the click was too far from any plant, ignore it
    if min_dist > THRESHOLD:
        return no_update, no_update

    # 4. We found a valid plant!
    plant_id = nearest_plant['plant_id']
    ts = pd.Timestamp.utcnow().isoformat()

    # Even if it's the same plant, update the store to trigger refreshes if needed
    if plant_id == current_plant_id:
        return no_update, {"clicked_plant_id": plant_id, "ts": ts}

    return plant_id, {"clicked_plant_id": plant_id, "ts": ts}





# Make sure update_revision_dropdown is properly connected
@app.callback(
    Output("store_runs", "data"),
    Output("dd_revision", "options"),
    Output("dd_revision", "value"),
    Output("dp_run_date", "min_date_allowed"),
    Output("dp_run_date", "max_date_allowed"),
    Output("dp_run_date", "date"),
    Output("run_date_hint", "children"),
    Output("dp_dl_range", "min_date_allowed"),
    Output("dp_dl_range", "max_date_allowed"),
    Output("dl_hint", "children"),
    Input("dd_forecast", "value"),
    Input("dd_plant", "value"),
    Input("interval", "n_intervals"),
    Input("dp_run_date", "date"),
    State("store_plants", "data"),
    State("dd_revision", "value"),
)
def update_revision_dropdown(forecast_ui, plant_id, _n, picked_date, plants, current_rev):
    model_name = FORECAST_TO_MODEL.get(forecast_ui or "Nowcasting", "NOWCAST")

    region_id = None
    if plant_id and plants:
        p = next((x for x in plants if x.get("plant_id") == plant_id), None)
        region_id = p.get("region_id") if p else None

    try:
        rlist = get_runs(limit=2000, model_name=model_name, region_id=region_id)
    except Exception:
        return (
            [],
            [{"label": "Latest", "value": "LATEST"}],
            "LATEST",
            None, None, None, "",
            None, None, ""
        )

    # store ALL runs so download can use ranges later
    all_runs = rlist or []

    dates = available_run_dates_ist(all_runs)
    if not dates:
        return (
            all_runs,
            [{"label": "Latest", "value": "LATEST"}],
            "LATEST",
            None, None, None, "No run dates available.",
            None, None, ""
        )

    min_d = dates[0].isoformat()
    max_d = dates[-1].isoformat()

    # decide active date
    target_date = None
    if picked_date:
        try:
            d = _parse_iso_date(picked_date)
            if d in dates:
                target_date = d
        except Exception:
            target_date = None

    if target_date is None:
        target_date = dates[-1]  # latest available date
    print("DATES IST:", dates[0], "->", dates[-1], "min_d/max_d:", min_d, max_d, "target:", target_date)
    # filter to selected date for revision list
    runs_for_day = [r for r in all_runs if (pd.notna(_parse_run_t0_dt_ist(r)) and _parse_run_t0_dt_ist(r).date() == target_date)]
    if not runs_for_day:
        runs_for_day = all_runs  # fallback

    revs = sorted(
        {str(r.get("revision") or "").strip() for r in runs_for_day if str(r.get("revision") or "").strip()},
        key=lambda x: parse_revision_num(x),
        reverse=True
    )

    date_tag = f" ({target_date.isoformat()})"
    opts = [{"label": f"Latest{date_tag}", "value": "LATEST"}] + [{"label": rev, "value": rev} for rev in revs]

    valid_values = {"LATEST"} | set(revs)
    value = current_rev if (current_rev in valid_values) else "LATEST"

    hint = f"Available: {min_d} → {max_d} | Showing: {target_date.isoformat()} | Dates: {len(dates)}"

    # set picker to chosen/nearest available date
    return (
        all_runs,
        opts,
        value,
        min_d,
        max_d,
        target_date.isoformat(),
        hint,
        min_d,
        max_d,
        f"Select range within: {min_d} → {max_d}"
    )



@app.callback(
    Output("download_csv", "data"),
    Input("btn_download", "n_clicks"),
    State("dd_forecast", "value"),
    State("dd_plant", "value"),
    State("dd_revision", "value"),
    State("store_runs", "data"),
    State("store_plants", "data"),
    State("dp_dl_range", "start_date"),
    State("dp_dl_range", "end_date"),
    State("dp_run_date", "date"),
    prevent_initial_call=True,
)

def download_table(_n, forecast_ui, plant_id, revision_choice, runs_store, plants, start_date, end_date, run_date):
    if not plant_id:
        return no_update

    model_name = FORECAST_TO_MODEL.get(forecast_ui or "Nowcasting", "NOWCAST")

    # filter runs to this plant's region (safety)
    region_id = None
    if plants:
        p = next((x for x in plants if x.get("plant_id") == plant_id), None)
        region_id = p.get("region_id") if p else None

    runs = [r for r in (runs_store or []) if (not region_id or r.get("region_id") == region_id)]
    runs = [r for r in runs if (r.get("model_name") or "").upper() == model_name.upper()]

    # Decide download range (run dates)
    if not start_date and not end_date:
        base = run_date or pd.Timestamp.now(tz="Asia/Kolkata").date().isoformat()
        start_date = base
        end_date = base
    elif start_date and not end_date:
        end_date = start_date
    elif end_date and not start_date:
        start_date = end_date

    sd = _parse_iso_date(start_date)
    ed = _parse_iso_date(end_date)
    if not sd or not ed:
        return no_update

    if ed < sd:
        sd, ed = ed, sd

    # safety cap (avoid very large downloads)
    if (ed - sd).days > 31:
        ed = (pd.Timestamp(sd) + pd.Timedelta(days=31)).date()

    rows = []
    for d in pd.date_range(sd, ed, freq="D"):
        day = d.date()
        runs_day = [r for r in runs if (pd.notna(_parse_run_t0_dt_ist(r)) and _parse_run_t0_dt_ist(r).date() == day)]
        if not runs_day:
            continue

        run = choose_run_by_revision(runs_day, revision_choice or "LATEST")
        if not run:
            continue

        tdf = get_table_df(plant_id, run["run_id"])
        if tdf.empty:
            continue

        # add context columns (keeps your existing columns unchanged)
        tdf.insert(0, "RunDate", day.isoformat())
        tdf.insert(1, "Revision", str(run.get("revision") or ""))
        tdf.insert(2, "Forecast", str(model_name))
        rows.append(tdf)

    if not rows:
        return no_update

    out = pd.concat(rows, ignore_index=True)
    fname = f"forecast_series_{sd.isoformat()}_to_{ed.isoformat()}.csv"
    return dcc.send_data_frame(out.to_csv, fname, index=False)



@app.callback(
    Output("kpi_cards", "children"),
    Output("plant_meta", "children"),
    Output("g1", "figure"),
    Output("g2", "figure"),
    Output("g3", "figure"),
    Output("g4", "figure"),
    Output("table_container", "children"),
    Input("dd_forecast", "value"),
    Input("dd_plant", "value"),
    Input("dd_revision", "value"),
    Input("interval", "n_intervals"),
    Input("store_marker_click", "data"),
    Input("dp_run_date", "date"),  # ADD THIS INPUT
    State("store_plants", "data"),
    State("store_runs", "data"),
    State("store_regions", "data"),
)
def update_all(forecast_ui, plant_id, revision_choice, _n, marker_click, run_date, plants, runs_store,regions_store):
    if not plant_id or not forecast_ui:
        msg = dbc.Alert("Select forecast and plant.", color="warning")
        return msg, "", blank_fig(), blank_fig(), blank_fig(), blank_fig(), ""

    model_name = FORECAST_TO_MODEL.get(forecast_ui, "NOWCAST")

    plant = next((p for p in (plants or []) if p.get("plant_id") == plant_id), None)
    if not plant:
        msg = dbc.Alert("Plant not found in current selection.", color="warning")
        return msg, "", blank_fig(), blank_fig(), blank_fig(), blank_fig(), ""

    region_id = plant.get("region_id")

    runs = [r for r in (runs_store or []) if (r.get("region_id") == region_id)]
    runs = [r for r in runs if (r.get("model_name") or "").upper() == model_name.upper()]
    # filter runs to selected run_date (IST)
    try:
        target_date = _parse_iso_date(run_date) if run_date else pd.Timestamp.now(tz="Asia/Kolkata").date()
    except Exception:
        target_date = pd.Timestamp.now(tz="Asia/Kolkata").date()

    runs = [r for r in runs if (pd.notna(_parse_run_t0_dt_ist(r)) and _parse_run_t0_dt_ist(r).date() == target_date)]
    # if none for that date, fallback to existing behavior (don’t break dashboard)
    if not runs:
        runs = [r for r in (runs_store or []) if (r.get("region_id") == region_id)]
        runs = [r for r in runs if (r.get("model_name") or "").upper() == model_name.upper()]


    run = choose_run_by_revision(runs, revision_choice or "LATEST")
    if not run:
        msg = dbc.Alert("No runs found yet for this selection. Wait for ingestion.", color="warning")
        meta = html.Div([html.Div(f"Lat/Lon: {float(plant['lat']):.4f}, {float(plant['lon']):.4f}")])
        return msg, meta, blank_fig(), blank_fig(), blank_fig(), blank_fig(), msg

    run_id = run["run_id"]
    rev = run.get("revision", "")

    items = get_series(plant_id, run_id)
    df = series_to_df(items)
    if df.empty:
        msg = dbc.Alert("No prediction rows yet for this plant/revision. Wait for DB update.", color="warning")
        meta = html.Div([html.Div(f"Lat/Lon: {float(plant['lat']):.4f}, {float(plant['lon']):.4f}")])
        return msg, meta, blank_fig(), blank_fig(), blank_fig(), blank_fig(), msg

    # ===== FIX: Use latest valid_time as Run t0 instead of run_t0_utc =====
    # For Nowcasting: latest valid_time is 15 mins in the past from run start
    # For Medium: run created at 05:30 AM daily
    # run_t0_display = ""
    # if "timestamp_str" in df.columns and df["timestamp_str"].notna().any():
    #     # Use latest timestamp from data as reference
    #     latest_ts = df["timestamp_str"].iloc[-1]
    #     run_t0_display = str(latest_ts)
    # elif "timestamp" in df.columns and df["timestamp"].notna().any():
    #     # Fallback: use timestamp column
    #     latest_ts = df["timestamp"].iloc[-1]
    #     run_t0_display = latest_ts.strftime("%Y-%m-%d %H:%M:%S")
    # else:
    #     # Ultimate fallback: use raw run data
    #     run_t0_display = run.get("run_t0_raw") or run.get("run_t0_ist_local") or run.get("run_t0_utc") or ""
    # ---- Run t0 display: 1 hour before series start time ----
    run_t0_display = ""
    try:
        if "timestamp" in df.columns and df["timestamp"].notna().any():
            # df is already sorted by timestamp in series_to_df()
            start_ts = df["timestamp"].dropna().min()   # e.g., 2026-01-17 15:45:00+05:30
            t0 = start_ts - pd.Timedelta(hours=1)       # -> 14:45
            run_t0_display = t0.strftime("%Y-%m-%d %H:%M:%S")
        elif "timestamp_str" in df.columns and df["timestamp_str"].notna().any():
            # fallback if timestamp column missing
            start_raw = str(df["timestamp_str"].dropna().iloc[0])
            start_parsed = pd.to_datetime(start_raw, errors="coerce", dayfirst=DAYFIRST)
            if pd.notna(start_parsed):
                # interpret naive as IST
                if getattr(start_parsed, "tzinfo", None) is None:
                    start_parsed = start_parsed.tz_localize("Asia/Kolkata")
                t0 = start_parsed - pd.Timedelta(hours=1)
                run_t0_display = t0.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        run_t0_display = ""

    # ultimate fallback
    if not run_t0_display:
        run_t0_display = run.get("run_t0_raw") or run.get("run_t0_ist_local") or run.get("run_t0_utc") or ""
    # ---- Plant + State/Region name (from store_regions) ----
    state_name = ""
    try:
        rid = plant.get("region_id")
        reg = next((r for r in (regions_store or []) if r.get("region_id") == rid), None)
        if reg:
            state_name = region_display_name(reg)  # uses your MH->Maharashtra mapping logic
    except Exception:
        state_name = ""

    plant_name = plant.get("plant_name", "")

    # ---- Series start/end (IST) for revision timestamp context ----
    start_time = ""
    end_time = ""
    if "timestamp" in df.columns and df["timestamp"].notna().any():
        start_ts = df["timestamp"].dropna().min()
        end_ts = df["timestamp"].dropna().max()
        start_time = start_ts.strftime("%Y-%m-%d %H:%M:%S")
        end_time = end_ts.strftime("%Y-%m-%d %H:%M:%S")

    # ---- Power stats (predicted) ----
    def _mean_max(col):
        if col in df.columns and df[col].notna().any():
            return float(df[col].mean()), float(df[col].max())
        return None, None

    avg_total_p, peak_total_p = _mean_max("power_pred_mw")
    avg_sol_p, peak_sol_p     = _mean_max("solar_power_pred_mw")
    avg_wind_p, peak_wind_p   = _mean_max("wind_power_pred_mw")

    # If total power not provided, estimate total = solar + wind (when both exist)
    if avg_total_p is None and ("solar_power_pred_mw" in df.columns) and ("wind_power_pred_mw" in df.columns):
        tot = df["solar_power_pred_mw"].fillna(0) + df["wind_power_pred_mw"].fillna(0)
        avg_total_p = float(tot.mean())
        peak_total_p = float(tot.max())

    meta = html.Div(
        [
            html.Div(
                f"{plant_name} | {state_name} • {plant.get('plant_type','')} • "
                f"{float(plant['lat']):.4f},{float(plant['lon']):.4f}",
                className="meta-line1",
            ),
            html.Div(
                f"Rev {rev} • Run t0 {run_t0_display} • Series {start_time} → {end_time}",
                className="meta-line2",
            ),
        ]
    )




    avg_ghi = float(df["ghi_pred_wm2"].mean()) if "ghi_pred_wm2" in df.columns else 0.0
    peak_ghi = float(df["ghi_pred_wm2"].max()) if "ghi_pred_wm2" in df.columns else 0.0
    latest_time = ""
    if "timestamp_str" in df.columns and df["timestamp_str"].notna().any():
        latest_time = str(df["timestamp_str"].iloc[-1])
    elif "timestamp" in df.columns and df["timestamp"].notna().any():
        latest_time = df["timestamp"].iloc[-1].strftime("%Y-%m-%d %H:%M")
    def fmt_mw(x):
        return "—" if (x is None or (isinstance(x, float) and pd.isna(x))) else f"{x:.2f}"
    run_start_time = ""
    if "timestamp_str" in df.columns and df["timestamp_str"].notna().any():
        run_start_time = str(df["timestamp_str"].iloc[0])  # FIRST row = series start
    elif "timestamp" in df.columns and df["timestamp"].notna().any():
        run_start_time = df["timestamp"].iloc[0].strftime("%Y-%m-%d %H:%M:%S")

    

    kpis = html.Div(
        [
            html.Div(
                [
                    html.Div(f"{avg_ghi:.0f} W/m²", className="kpi-value"),
                    html.Div("Average predicted GHI", className="kpi-label"),
                    html.Div(f"Peak: {peak_ghi:.0f} W/m²", className="kpi-sub"),
                    html.Div(
                        f"Avg Power: {fmt_mw(avg_total_p)} MW | Peak: {fmt_mw(peak_total_p)} MW"
                        if avg_total_p is not None else "Avg Power: —",
                        className="kpi-sub",
                    ),
                    html.Div(
                        f"Solar: {fmt_mw(avg_sol_p)} MW, Wind: {fmt_mw(avg_wind_p)} MW"
                        if (avg_sol_p is not None or avg_wind_p is not None) else "",
                        className="kpi-sub",
                    ),
                ],
                className="kpi-tile",
            ),
            html.Div(
                [
                    html.Div(run_start_time or latest_time, className="kpi-value"),
                    html.Div("series start time", className="kpi-label"),
                    html.Div(f"Revision {rev}", className="kpi-sub"),
                    html.Div(f"", className="kpi-sub"),
                ],
                className="kpi-tile",
            ),
        ],
        className="summary-stack",
    )


    fig1 = plot_pred_vs_actual(df, "ghi_pred_wm2", "ghi_actual_wm2", "GHI Prediction vs Actual", "W/m²")
    fig2 = plot_pred_vs_actual(df, "solar_power_pred_mw", "solar_power_actual_mw", "Solar Power Prediction vs Actual", "MW")
    fig3 = plot_pred_vs_actual(df, "wind_pred", "wind_actual", "Wind Prediction vs Actual", "M/S")
    
    # ===== CHANGE: g4 now shows blank graph instead of Total Power =====
    fig4 =plot_pred_vs_actual(df, "wind_power_pred_mw", "wind_power_actual_mw", "Wind Power Prediction vs Actual", "MW")

    tdf = get_table_df(plant_id, run_id)
    if tdf.empty:
        table = dbc.Alert("No table rows for this selection.", color="warning")
        return kpis, meta, fig1, fig2, fig3, fig4, table

    raw_table = dbc.Table.from_dataframe(
    tdf.head(500),
    striped=True,
    bordered=True,
    hover=True,
    responsive=True,
    color="dark",
    class_name="table-sm align-middle mb-0 table-centered"
    )

    # Scroll only when more than 10 rows (optional)
    if len(tdf) > 10:
        table = html.Div(raw_table, className="table-scroll")
    else:
        table = raw_table


    return kpis, meta, fig1, fig2, fig3, fig4, table

# -----------------------------
# Authentication
# -----------------------------
def login_layout():
    return dbc.Container(
        fluid=True,
        style={"height": "100vh", "display": "flex", "alignItems": "center", "justifyContent": "center"},
        children=[
            dbc.Card(
                dbc.CardBody(
                    [
                        html.H3("Login", className="text-center mb-4"),
                        dbc.Input(id="login_username", placeholder="Username", type="text", className="mb-3"),
                        dbc.Input(id="login_password", placeholder="Password", type="password", className="mb-3"),
                        dbc.Button("Login", id="login_button", color="primary", className="w-100"),
                        html.Div(id="login_message", className="text-danger mt-3 text-center"),
                    ]
                ),
                style={"width": "350px"},
            )
        ],
    )

# def authenticate_user(username: str, password: str) -> bool:
#     try:
#         conn = psycopg2.connect(DATABASE_URL)
#         cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

#         query = """
#             SELECT 1
#             FROM users
#             WHERE uname = %s
#               AND password = crypt(%s, password)
#         """
#         cur.execute(query, (username, password))
#         result = cur.fetchone()

#         cur.close()
#         conn.close()

#         return result is not None

#     except Exception as e:
#         print("Auth DB error:", e)
#         return False


@app.callback(
    Output("page-content", "children"),
    Input("url", "pathname")
)

def display_page(pathname):
    if not session.get("logged_in"):
        return login_layout()

    if pathname in ("/", "/dashboard"):
        return dashboard_layout()

    return login_layout()


@app.callback(
    Output("login_message", "children"),
    Output("url", "pathname"),
    Input("login_button", "n_clicks"),
    State("login_username", "value"),
    State("login_password", "value"),
    prevent_initial_call=True
)
def login(n_clicks, username, password):
    if not username or not password:
        return "", "/"

    try:
        print("LOGIN CALLBACK TRIGGERED")
        print("API_BASE:", API_BASE)
        r = SESSION.post(
            f"{API_BASE}/login",
            json={"username": username, "password": password},
        )

        if r.ok:
            session.permanent = True
            session["logged_in"] = True
            session["username"] = username
            return "", "/dashboard"
        else:
            return "Invalid username or password", "/"
        print("STATUS CODE:", r.status_code)
        print("RESPONSE TEXT:", r.text)

    except Exception as e:
        print("Login API error:", e)
        return "Authentication service unavailable", "/"


@app.callback(
    Output("url", "pathname", allow_duplicate=True),
    Input("btn_logout", "n_clicks"),
    prevent_initial_call=True,
)
def logout(n_clicks):
    if not n_clicks:
        return no_update
    session.clear()
    return "/"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8060, debug=True)