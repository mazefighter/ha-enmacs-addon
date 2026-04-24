"""
Lädt das trainierte XGBoost-Modell (PKL), berechnet Prognosen + Ist-Daten
und schreibt beides direkt in InfluxDB 1.8 — ohne CSV-Zwischenschritt.

Verwendung:
  python write_influx.py                  # echte Daten (HASS_TOKEN nötig)
  python write_influx.py --demo           # Demo-Daten
  python write_influx.py --only-forecast  # nur Forecast schreiben
  python write_influx.py --only-actual    # nur Ist-Daten schreiben

InfluxDB-Verbindung via Umgebungsvariablen (oder Standardwerte):
  INFLUX_HOST      (Standard: localhost)
  INFLUX_PORT      (Standard: 8086)
  INFLUX_DB        (Standard: xgb_forecasts)
  INFLUX_USER      (Standard: leer)
  INFLUX_PASSWORD  (Standard: leer)
"""

import argparse
import asyncio
import os
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

from train_xgboost import (
    WB_CONFIG, TZ, LOOKBACK_SLOTS, INACTIVE_DAYS_FILTER,
    MIN_ACTIVE_RATE_PCT, ENR_THRESHOLD, de_holidays_nw,
    load_data, build_features, is_inactive_wallbox,
    predict_wallbox_ar, calc_max_session_kwh,
)

try:
    from influxdb import InfluxDBClient
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "influxdb", "-q"])
    from influxdb import InfluxDBClient

# ─── Konfiguration ────────────────────────────────────────────────────────────
INFLUX_HOST     = os.getenv("INFLUX_HOST",     "localhost")
INFLUX_PORT     = int(os.getenv("INFLUX_PORT", "8086"))
INFLUX_DB       = os.getenv("INFLUX_DB",       "xgb_forecasts")
INFLUX_USER     = os.getenv("INFLUX_USER",     "")
INFLUX_PASSWORD = os.getenv("INFLUX_PASSWORD", "")

MODEL_DIR  = Path("/usr/src/models")
HIST_WEEKS = 26
BATCH_SIZE = 500


# ─── Modell laden ─────────────────────────────────────────────────────────────
def load_model() -> dict:
    path = MODEL_DIR / "xgboost_ar_model.pkl"
    if not path.exists():
        raise FileNotFoundError(
            f"Modell nicht gefunden: {path}\n"
            "Bitte zuerst train_xgboost.py ausführen."
        )
    with open(path, "rb") as f:
        bundle = pickle.load(f)
    print(f"  Modell geladen: {path.name}  "
          f"({len(bundle['wb_to_idx'])} Wallboxen, "
          f"{len(bundle.get('feature_names', []))} Features)")
    return bundle


# ─── InfluxDB verbinden ───────────────────────────────────────────────────────
def connect() -> InfluxDBClient:
    client = InfluxDBClient(
        host=INFLUX_HOST, port=INFLUX_PORT,
        username=INFLUX_USER, password=INFLUX_PASSWORD,
    )
    existing = [d["name"] for d in client.get_list_database()]
    if INFLUX_DB not in existing:
        client.create_database(INFLUX_DB)
        print(f"  Datenbank '{INFLUX_DB}' neu angelegt.")
    client.switch_database(INFLUX_DB)
    return client


# ─── DataFrame → InfluxDB-Points ──────────────────────────────────────────────
def df_to_points(df: pd.DataFrame, measurement: str, type_tag: str,
                 ts_col: str = "timestamp") -> list:
    points = []
    for _, row in df.iterrows():
        ts = pd.Timestamp(row[ts_col])
        if ts.tzinfo is None:
            ts = ts.tz_localize("Europe/Berlin")
        points.append({
            "measurement": measurement,
            "tags": {
                "wallbox": str(row["wallbox"]),
                "type":    type_tag,
            },
            "time": ts.tz_convert("UTC").isoformat(),
            "fields": {
                "enr_kwh":    float(row["enr_kwh"]),
                "is_weekend": int(row["is_weekend"]),
                "is_holiday": int(row["is_holiday"]),
            },
        })
    return points


# ─── Batched Write ────────────────────────────────────────────────────────────
def write_points(client: InfluxDBClient, points: list, label: str) -> None:
    total = len(points)
    if total == 0:
        print(f"  {label}: keine Punkte zu schreiben.")
        return
    written = 0
    for i in range(0, total, BATCH_SIZE):
        client.write_points(points[i:i + BATCH_SIZE])
        written += len(points[i:i + BATCH_SIZE])
        print(f"  {label}: {written:>6,} / {total:,} Punkte ...", end="\r")
    print(f"  {label}: {written:,} Punkte  ✓{' ' * 20}")


# ─── Hauptprogramm ────────────────────────────────────────────────────────────
async def main(use_demo: bool, only_forecast: bool, only_actual: bool) -> None:
    from datetime import datetime
    SEP = "═" * 62
    sep = "─" * 62

    print(f"\n{SEP}")
    print(f"  XGBoost → InfluxDB  │  {datetime.now(TZ).strftime('%d.%m.%Y %H:%M')}")
    print(f"  Ziel: {INFLUX_HOST}:{INFLUX_PORT} / {INFLUX_DB}")
    print(f"{SEP}")

    print(f"\n{sep}\n  SCHRITT 1/4  ─  Modell laden\n{sep}")
    bundle          = load_model()
    clf             = bundle["clf"]
    reg             = bundle["reg"]
    wb_to_idx       = bundle["wb_to_idx"]
    wb_max_kws      = bundle.get("wb_max_kws", {})
    wb_typical_kws  = bundle.get("wb_typical_kws", {})
    wb_max_sessions = bundle.get("wb_max_session_kwhs", {})

    print(f"\n{sep}\n  SCHRITT 2/4  ─  Aktuelle Daten laden\n{sep}")
    raw_stats = await load_data(use_demo, WB_CONFIG)

    wb_dataframes = {}
    for wb_name, data in raw_stats.items():
        if wb_name not in wb_to_idx:
            continue
        enr_slots = data["enr"] if isinstance(data, dict) else data
        if is_inactive_wallbox(enr_slots, INACTIVE_DAYS_FILTER) or \
                len(enr_slots) < LOOKBACK_SLOTS + 10:
            print(f"  SKIP  {wb_name:<10} inaktiv / zu wenig Daten")
            continue
        df_wb = build_features(enr_slots)
        rate = (df_wb["enr"] > ENR_THRESHOLD).sum() / len(df_wb) * 100
        if rate >= MIN_ACTIVE_RATE_PCT:
            wb_dataframes[wb_name] = df_wb
            print(f"  OK    {wb_name:<10}  {len(df_wb):>5,} Slots │ {rate:.1f}% aktiv")
        else:
            print(f"  SKIP  {wb_name:<10}  Aktivrate {rate:.1f}% zu gering")

    if not wb_dataframes:
        raise RuntimeError("Keine aktiven Wallboxen gefunden.")

    print(f"\n{sep}\n  SCHRITT 3/4  ─  Prognosen berechnen\n{sep}")
    print(f"  {'Wallbox':<10} {'FC-Slots':>8} {'FC-Σ kWh':>10} {'Actual-Slots':>13}")
    print(f"  {'─'*10} {'─'*8} {'─'*10} {'─'*13}")

    forecast_points  = []
    actual_points    = []
    fc_wallboxes     = set()
    actual_wallboxes = set()

    for wb_name, df_wb in wb_dataframes.items():
        wb_id           = wb_to_idx[wb_name]
        max_kw          = wb_max_kws.get(wb_name, float(df_wb["enr"].max()) or 22.0)
        typical_kw      = wb_typical_kws.get(wb_name)
        max_session_kwh = wb_max_sessions.get(wb_name) or calc_max_session_kwh(df_wb["enr"])

        df_fc     = predict_wallbox_ar(wb_name, clf, reg, df_wb, wb_id,
                                       max_kw, typical_kw, max_session_kwh)
        fc_active = df_fc[df_fc["enr_kwh"] > 0]
        fc_pts    = df_to_points(fc_active, "wallbox_energy", "forecast")
        forecast_points  += fc_pts
        fc_wallboxes.add(wb_name)

        hist = df_wb.tail(HIST_WEEKS * 7 * 24).copy()
        df_hist = pd.DataFrame({
            "timestamp":  hist.index.strftime("%Y-%m-%d %H:%M:%S"),
            "wallbox":    wb_name,
            "enr_kwh":    hist["enr"].values.round(4),
            "is_weekend": (hist.index.dayofweek >= 5).astype(int),
            "is_holiday": [1 if d in de_holidays_nw else 0 for d in hist.index.date],
        })
        act_df  = df_hist[df_hist["enr_kwh"] > 0]
        act_pts = df_to_points(act_df, "wallbox_energy", "actual")
        actual_points    += act_pts
        actual_wallboxes.add(wb_name)

        print(f"  {wb_name:<10} {len(fc_active):>8} {fc_active['enr_kwh'].sum():>9.1f} kWh "
              f"{len(act_df):>12}")

    missing_actual = fc_wallboxes - actual_wallboxes
    if missing_actual:
        raise RuntimeError(
            f"FEHLER: Folgende Wallboxen haben Forecasts aber keine Ist-Daten: {missing_actual}"
        )
    print(f"\n  ✓ Parität OK: {len(fc_wallboxes)} Wallboxen haben jeweils Forecast + Ist-Daten")

    print(f"\n{sep}\n  SCHRITT 4/4  ─  In InfluxDB schreiben\n{sep}")
    client = connect()
    print(f"  Verbunden: {INFLUX_HOST}:{INFLUX_PORT} / {INFLUX_DB}")

    if not only_actual:
        write_points(client, forecast_points, "Forecast  ")
    if not only_forecast:
        write_points(client, actual_points,   "Ist-Daten ")

    client.close()
    print(f"\n{SEP}\n  FERTIG\n{SEP}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="XGBoost PKL → InfluxDB 1.8 (kein CSV)"
    )
    parser.add_argument("--demo",          action="store_true", help="Demo-Daten verwenden")
    parser.add_argument("--only-forecast", action="store_true", help="Nur Forecast schreiben")
    parser.add_argument("--only-actual",   action="store_true", help="Nur Ist-Daten schreiben")
    args = parser.parse_args()

    if args.only_forecast and args.only_actual:
        print("Fehler: --only-forecast und --only-actual schließen sich aus.")
        sys.exit(1)

    asyncio.run(main(
        use_demo=args.demo,
        only_forecast=args.only_forecast,
        only_actual=args.only_actual,
    ))
