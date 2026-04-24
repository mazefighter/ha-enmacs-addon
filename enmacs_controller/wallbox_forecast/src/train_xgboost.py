"""
XGBoost-Training für Wallbox-Ladeprognose (Autoregressiver Ansatz)
Löst das Over-Smoothing-Problem: Sagt scharfe, physikalisch realistische
Ladeblöcke voraus (z.B. 11 kW für 4h statt 4.5 kW für 9h).

Verwendung:
  python train_xgboost.py                     # interaktive Auswahl, echte Daten
  python train_xgboost.py --demo              # Demo-Daten
"""

import argparse
import asyncio
import json
import os
import pickle
import time
import urllib3
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import holidays
import numpy as np
import pandas as pd

try:
    import xgboost as xgb
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "xgboost", "-q"])
    import xgboost as xgb

try:
    import websockets
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "websockets", "-q"])
    import websockets

urllib3.disable_warnings()

# ─── Konfiguration ────────────────────────────────────────────────────────────
TZ            = ZoneInfo("Europe/Berlin")
HASS_HOST     = os.getenv("HASS_HOST", "ems.kis-gmbh.de")
HASS_TOKEN    = os.getenv("HASS_TOKEN")
HISTORY_START = datetime(2025, 11, 1, tzinfo=TZ)

MODEL_DIR = Path("/usr/src/models")
MODEL_DIR.mkdir(exist_ok=True)

WB_CONFIG = {
    "1GF":   "sensor.geschaeftsfuehrung",
    "2EL":   "sensor.e_auto_seite_linke_ladebox",
    "3EM":   "sensor.e_auto_seite_mittlere_ladebox",
    "4ER":   "sensor.e_auto_seite_rechte_ladebox",
    "5SL":   "sensor.sudseite_linke_box",
    "6SR":   "sensor.sudseite_rechte_box",
    "7P1L":  "sensor.wb_parkplatz_1_links",
    "8P1R":  "sensor.wb_parkplatz_1_rechts",
    "9P2L":  "sensor.wb_parkplatz_2_links",
    "10P2R": "sensor.wb_parkplatz_2_rechts",
    "11H1L": "sensor.hybrid_seite_ladebox_1_von_links",
    "12H2L": "sensor.hybrid_seite_ladebox_2_von_links",
    "13H3L": "sensor.hybrid_seite_ladebox_3_von_links",
    "14H4L": "sensor.hybrid_seite_ladebox_4_von_links",
    "15H5L": "sensor.hybrid_seite_ladebox_5_von_links",
    "16H6L": "sensor.hybrid_seite_ladebox_6_von_links",
    "17B1L": "sensor.wb_besucher_1_links",
    "18B1R": "sensor.wb_besucher_1_rechts",
    "19B2L": "sensor.wb_besucher_2_links",
    "20B2R": "sensor.wb_besucher_2_rechts",
}

STATUS_STATES    = ["disconnected", "ready_to_charge", "awaiting_start", "completed", "charging"]
STATUS_COL_NAMES = {
    "disconnected":    "st_disconnected",
    "ready_to_charge": "st_ready",
    "awaiting_start":  "st_awaiting",
    "completed":       "st_completed",
    "charging":        "st_charging",
}

ENR_THRESHOLD        = 1.0
INACTIVE_DAYS_FILTER = 30
MIN_ACTIVE_RATE_PCT  = 2.0

current_year   = datetime.now().year
de_holidays_nw = holidays.Germany(subdiv="NW", years=range(current_year - 1, current_year + 2))

# ─── XGBoost AR-Hyperparameter ────────────────────────────────────────────────
FORECAST_SLOTS = 72
LOOKBACK_SLOTS = 48
MIN_TRAIN_ROWS = LOOKBACK_SLOTS + FORECAST_SLOTS + 200

FEATURE_NAMES = [
    "enr_last_1h",
    "enr_last_2h",
    "enr_last_3h",
    "enr_last_24h_sum",
    "is_charging_now",
    "session_kwh_so_far",
    "target_hour",
    "target_dow",
    "target_is_weekend",
    "target_is_holiday",
    "target_month",
    "hist_mean_target",
    "enr_last_168h",
    "wallbox_id",
    "wb_max_kw",
]


# ─── Datenabruf ───────────────────────────────────────────────────────────────
async def _ws_fetch_all(host, token, wb_config, start_dt, batch_size=4):
    url = f"wss://{host}/api/websocket"
    all_data = {}
    async with websockets.connect(url, ssl=True, max_size=8 * 1024 * 1024) as ws:
        await ws.recv()
        await ws.send(json.dumps({"type": "auth", "access_token": token}))
        auth = json.loads(await ws.recv())
        if auth.get("type") != "auth_ok": raise RuntimeError("WebSocket Auth fehlgeschlagen")
        wb_list = list(wb_config.items())
        msg_id = 1
        for i in range(0, len(wb_list), batch_size):
            batch = wb_list[i:i + batch_size]
            stat_ids = ([f"{prefix}_gesamtenergie" for _, prefix in batch] +
                        [f"{prefix}_leistung" for _, prefix in batch])
            await ws.send(json.dumps({
                "id": msg_id, "type": "recorder/statistics_during_period",
                "start_time": start_dt.isoformat(), "statistic_ids": stat_ids,
                "period": "hour", "types": ["sum", "state", "mean"]
            }))
            resp = json.loads(await ws.recv())
            if resp.get("success"): all_data.update(resp.get("result", {}))
            msg_id += 1
    return all_data

async def _ws_fetch_status_history(host, token, wb_config, start_dt, batch_size=5):
    url = f"wss://{host}/api/websocket"
    result = {}
    async with websockets.connect(url, ssl=True, max_size=16 * 1024 * 1024) as ws:
        await ws.recv()
        await ws.send(json.dumps({"type": "auth", "access_token": token}))
        auth = json.loads(await ws.recv())
        if auth.get("type") != "auth_ok": raise RuntimeError("WebSocket Auth fehlgeschlagen")
        status_ids = [f"{prefix}_status" for prefix in wb_config.values()]
        wb_names = list(wb_config.keys())
        msg_id = 1
        for i in range(0, len(status_ids), batch_size):
            batch_ids, batch_names = status_ids[i:i+batch_size], wb_names[i:i+batch_size]
            await ws.send(json.dumps({
                "id": msg_id, "type": "history/history_during_period",
                "start_time": start_dt.isoformat(), "entity_ids": batch_ids,
                "no_attributes": True, "significant_changes_only": False
            }))
            resp = json.loads(await ws.recv())
            if resp.get("success"):
                data = resp.get("result", {})
                for wb_name, sid in zip(batch_names, batch_ids):
                    result[wb_name] = data.get(sid, [])
            msg_id += 1
    return result

def hourly_stats_to_1h(rows, tz):
    if not rows: return []
    records = {}
    for r in rows:
        ts = datetime.fromtimestamp(r["start"] / 1000, tz=tz)
        val = r.get("sum") if r.get("sum") is not None else r.get("state")
        if val is not None: records[ts] = float(val)
    if not records: return []
    hourly = pd.Series(records).sort_index()
    deltas = hourly.diff().clip(lower=0).dropna()
    slots = [{"dt": ts.strftime("%Y-%m-%dT%H:%M"), "enr": round(v, 4)}
             for ts, v in deltas.items() if v <= 50]
    return slots

def is_inactive_wallbox(enr_slots, n_days=INACTIVE_DAYS_FILTER):
    if not enr_slots: return True
    cutoff = (datetime.now(tz=TZ) - timedelta(days=n_days)).strftime("%Y-%m-%dT%H:%M")
    recent = [s["enr"] for s in enr_slots if s["dt"] >= cutoff]
    return len(recent) == 0 or all(e == 0.0 for e in recent)

def generate_demo_data(wb_names=None, n_days=50):
    if wb_names is None: wb_names = list(WB_CONFIG.keys())[:5]
    np.random.seed(42)
    ref = pd.date_range(start=datetime.now(tz=TZ) - timedelta(days=n_days),
                        periods=n_days * 24, freq="1h", tz="Europe/Berlin")
    demo = {}
    for wb_name in wb_names:
        enr_slots = []
        connected = False
        for ts in ref:
            is_we, is_fei = ts.weekday() >= 5, ts.date() in de_holidays_nw
            hour = ts.hour
            if not is_we and not is_fei and 7 <= hour < 18:
                if not connected and np.random.rand() < 0.08: connected = True
                elif connected and np.random.rand() < 0.12: connected = False
            else:
                if connected and np.random.rand() < 0.3: connected = False

            if connected: enr = max(0.0, round(11.0 + np.random.normal(0, 0.5), 4))
            else: enr = 0.0

            enr_slots.append({"dt": ts.strftime("%Y-%m-%dT%H:%M"), "enr": enr})
        demo[wb_name] = {"enr": enr_slots}
    return demo

async def load_data(use_demo: bool, wb_config: dict) -> dict:
    if not use_demo and HASS_TOKEN:
        print(f"Lade via WebSocket Statistics seit {HISTORY_START.strftime('%d.%m.%Y')} ...")
        ws_data = await _ws_fetch_all(HASS_HOST, HASS_TOKEN, wb_config, HISTORY_START)
        raw_stats = {}
        for wb_name, prefix in wb_config.items():
            enr_rows = ws_data.get(f"{prefix}_gesamtenergie", [])
            enr_slots = hourly_stats_to_1h(enr_rows, TZ)
            if enr_slots: raw_stats[wb_name] = {"enr": enr_slots}
        return raw_stats
    else:
        print("Demo-Modus: generiere synthetische Daten ...")
        return generate_demo_data(wb_names=list(wb_config.keys()), n_days=50)

def build_features(records_enr: list) -> pd.DataFrame:
    df = pd.DataFrame(records_enr)
    df["dt"] = pd.to_datetime(df["dt"])
    df = df.sort_values("dt").set_index("dt")
    full_idx = pd.date_range(start=df.index.min().floor("1h"), end=df.index.max().floor("1h"), freq="1h")
    df = df.reindex(full_idx).fillna(0.0)

    hist_mean = np.zeros(len(df))
    enr_vals = df["enr"].values
    HIST_WEIGHTS = [0.50, 0.25, 0.15, 0.10]
    MIN_ACTIVE_WEEKS = 2
    for i in range(len(df)):
        if i >= 168:
            pairs = [(HIST_WEIGHTS[w - 1], enr_vals[i - w * 168])
                     for w in range(1, 5) if i - w * 168 >= 0]
            n_active = sum(1 for _, v in pairs if v > ENR_THRESHOLD)
            if n_active < MIN_ACTIVE_WEEKS:
                hist_mean[i] = 0.0
            else:
                total_w = sum(w for w, _ in pairs)
                hist_mean[i] = float(sum(w * v for w, v in pairs) / total_w)
    df["hist_mean"] = hist_mean
    return df


# ─── Feature-Erstellung für Autoregression ────────────────────────────────────
def calc_session_kwh(history_array):
    total = 0.0
    gaps = 0
    for val in reversed(history_array):
        if val > ENR_THRESHOLD:
            total += val
            gaps = 0
        elif gaps < 1:
            gaps += 1
        else:
            break
    return total

def calc_max_session_kwh(enr_series: pd.Series) -> float:
    sessions = []
    current = 0.0
    gaps = 0
    for val in enr_series:
        if val > ENR_THRESHOLD:
            current += val
            gaps = 0
        elif gaps < 1:
            gaps += 1
        else:
            if current > 0:
                sessions.append(current)
            current = 0.0
            gaps = 0
    if current > 0:
        sessions.append(current)

    if not sessions:
        return 40.0

    p75 = float(np.percentile(sessions, 75))
    return p75 * 1.20

def _build_ar_feature_row(df_wb: pd.DataFrame, t: int, wb_id: int, max_kw: float, override_enr=None) -> np.ndarray:
    enr_vals = override_enr if override_enr is not None else df_wb["enr"].values

    enr_last_1h = float(enr_vals[t-1]) if t >= 1 else 0.0
    enr_last_2h = float(enr_vals[t-2]) if t >= 2 else 0.0
    enr_last_3h = float(enr_vals[t-3]) if t >= 3 else 0.0
    enr_24h_sum = float(np.sum(enr_vals[max(0, t-24):t])) if t >= 1 else 0.0

    is_charging_now = 1.0 if enr_last_1h > ENR_THRESHOLD else 0.0

    history_so_far = enr_vals[max(0, t-48):t]
    session_kwh = float(calc_session_kwh(history_so_far))

    ts = df_wb.index[t] if t < len(df_wb) else df_wb.index[-1] + timedelta(hours=t - len(df_wb) + 1)
    t_hour  = float(ts.hour)
    t_dow   = float(ts.dayofweek)
    t_is_we = 1.0 if t_dow >= 5 else 0.0
    t_is_ho = 1.0 if ts.date() in de_holidays_nw else 0.0
    t_month = float(ts.month)

    hist_mean_t = float(df_wb["hist_mean"].iloc[t]) if t < len(df_wb) else float(df_wb["hist_mean"].iloc[t - 168])
    enr_last_168 = float(enr_vals[t-168]) if t >= 168 else 0.0

    row = [
        enr_last_1h, enr_last_2h, enr_last_3h, enr_24h_sum, is_charging_now, session_kwh,
        t_hour, t_dow, t_is_we, t_is_ho, t_month,
        hist_mean_t, enr_last_168, float(wb_id), float(max_kw)
    ]
    return np.array(row, dtype=np.float32)

def prepare_ar_training_data(wb_dataframes, wb_to_idx, wb_max_kws):
    X_parts, y_parts, ts_parts = [], [], []
    for wb_name, df_wb in wb_dataframes.items():
        wb_id = wb_to_idx[wb_name]
        max_kw = wb_max_kws[wb_name]
        n = len(df_wb)

        X_wb = np.empty((n - LOOKBACK_SLOTS, len(FEATURE_NAMES)), dtype=np.float32)
        y_wb = df_wb["enr"].values[LOOKBACK_SLOTS:]
        ts_wb = df_wb.index[LOOKBACK_SLOTS:]

        for i, t in enumerate(range(LOOKBACK_SLOTS, n)):
            X_wb[i] = _build_ar_feature_row(df_wb, t, wb_id, max_kw)

        X_parts.append(X_wb)
        y_parts.append(y_wb)
        ts_parts.append(ts_wb)

    X = np.concatenate(X_parts, axis=0)
    y = np.concatenate(y_parts, axis=0)
    timestamps = np.concatenate(ts_parts, axis=0)
    return X, y, timestamps


# ─── Training ─────────────────────────────────────────────────────────────────
def train_two_stage_ar(X, y, timestamps):
    """Trainiert Klassifikator + Regressor mit Recency-Gewichtung.

    Samples der letzten 4 Wochen: volles Gewicht (1.0).
    Ältere Samples: Exponential Decay mit Halbwertszeit 28 Tage (min. 0.15).
    """
    now = pd.Timestamp.now(tz=None)
    ts = pd.DatetimeIndex(timestamps)
    if ts.tz is not None:
        ts = ts.tz_localize(None)
    days_ago = (now - ts).days.values.astype(float)
    HALF_LIFE_DAYS = 28.0
    weights = np.exp(-np.log(2) / HALF_LIFE_DAYS * np.maximum(days_ago - 28.0, 0.0))
    weights = np.clip(weights, 0.15, 1.0).astype(np.float32)

    recent_mask = days_ago <= 28
    print(f"  Recency-Gewichtung:  Halbwertszeit {HALF_LIFE_DAYS:.0f}d | "
          f"Letzte 4W: {recent_mask.sum():,} Samples (Gewicht 1.0) | "
          f"Älter: {(~recent_mask).sum():,} Samples (Gew. ≥ 0.15)")

    split = int(len(X) * 0.85)
    X_tr, X_v = X[:split], X[split:]
    y_tr, y_v = y[:split], y[split:]
    w_tr       = weights[:split]

    y_bin_tr = (y_tr > ENR_THRESHOLD).astype(np.float32)
    y_bin_v  = (y_v  > ENR_THRESHOLD).astype(np.float32)

    scale_pos = float((y_bin_tr == 0).sum() / max((y_bin_tr == 1).sum(), 1)) * 1.2

    n_act_tr = int(y_bin_tr.sum())
    n_ina_tr = int((y_bin_tr == 0).sum())
    print(f"  Trainings-Split:  {len(X_tr):>7,} Zeilen (85%) │ Val: {len(X_v):>6,} (15%)")
    print(f"  Aktive Slots:     {n_act_tr:>7,}  ({n_act_tr/len(y_bin_tr)*100:.1f}%)")
    print(f"  Inaktive Slots:   {n_ina_tr:>7,}  ({n_ina_tr/len(y_bin_tr)*100:.1f}%)")
    print(f"  Klassengewicht:   scale_pos_weight = {scale_pos:.2f}")

    print("\n  [ Stufe 1 ] Klassifikator — lädt die WB im nächsten Slot?")
    clf = xgb.XGBClassifier(
        objective="binary:logistic",
        n_estimators=200,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=scale_pos,
        random_state=42,
        n_jobs=-1,
        verbosity=0,
        eval_metric="logloss",
        early_stopping_rounds=30,
    )
    clf.fit(
        X_tr, y_bin_tr,
        sample_weight=w_tr,
        eval_set=[(X_v, y_bin_v)],
        verbose=False,
    )
    print(f"  ✓ {clf.n_estimators} Bäume trainiert")

    active_tr = y_tr > ENR_THRESHOLD
    active_v  = y_v  > ENR_THRESHOLD
    print(f"\n  [ Stufe 2 ] Regressor — mit wie viel kW? (Quantile α=0.78)")
    print(f"  Trainings-Samples: {active_tr.sum():,} aktive Slots")

    reg = xgb.XGBRegressor(
        objective="reg:quantileerror",
        quantile_alpha=0.78,
        n_estimators=300,
        learning_rate=0.05,
        max_depth=6,
        random_state=42,
        n_jobs=-1,
        verbosity=0,
        eval_metric="quantile",
        early_stopping_rounds=50,
    )
    reg.fit(
        X_tr[active_tr], y_tr[active_tr],
        sample_weight=w_tr[active_tr],
        eval_set=[(X_v[active_v], y_v[active_v])],
        verbose=False,
    )
    best_iter = reg.best_iteration if hasattr(reg, "best_iteration") and reg.best_iteration else reg.n_estimators
    print(f"  ✓ {reg.n_estimators} Bäume trainiert (best iteration: {best_iter})")

    return clf, reg


# ─── Rollout-Inferenz (Autoregressiv) ─────────────────────────────────────────
def predict_wallbox_ar(wb_name: str, clf, reg, df_wb: pd.DataFrame,
                       wb_id: int, max_kw: float, typical_kw: float = None,
                       max_session_kwh: float = None) -> pd.DataFrame:
    high_power_threshold = typical_kw if typical_kw is not None else max_kw * 0.70
    session_cap = max_session_kwh if max_session_kwh is not None else max_kw * 4
    history_enr = df_wb["enr"].values.tolist()

    fut_dt = pd.date_range(start=df_wb.index[-1] + timedelta(hours=1), periods=FORECAST_SLOTS, freq="1h")
    predictions = []

    current_t = len(df_wb)
    prev_was_boosted = False

    for _ in range(FORECAST_SLOTS):
        current_enr_array = np.array(history_enr, dtype=np.float32)

        x_row = _build_ar_feature_row(df_wb, current_t, wb_id, max_kw, override_enr=current_enr_array)
        x_2d  = x_row.reshape(1, -1)

        current_session_kwh = calc_session_kwh(history_enr)
        if current_session_kwh > session_cap:
            pred_kw = 0.0
            feedback_enr = 0.0
            prev_was_boosted = False
        else:
            prob_act = clf.predict_proba(x_2d)[0, 1]
            if prob_act >= 0.42:
                pred_kw = float(np.clip(reg.predict(x_2d)[0], 0, max_kw))

                if pred_kw >= high_power_threshold and not prev_was_boosted:
                    feedback_enr = max_kw
                    prev_was_boosted = True
                else:
                    feedback_enr = pred_kw
                    prev_was_boosted = pred_kw >= high_power_threshold
            else:
                pred_kw = 0.0
                feedback_enr = 0.0
                prev_was_boosted = False

        predictions.append(pred_kw)
        history_enr.append(feedback_enr)
        current_t += 1

    return pd.DataFrame({
        "timestamp":  list(fut_dt),
        "wallbox":    wb_name,
        "enr_kwh":    np.round(predictions, 4),
        "is_weekend": (fut_dt.dayofweek >= 5).astype(int).tolist(),
        "is_holiday": [1 if d in de_holidays_nw else 0 for d in fut_dt.date],
    })


def predict_wallbox(wb_name: str, clf, reg, df_wb: pd.DataFrame,
                    wb_id: int, max_kw: float = None, typical_kw: float = None,
                    max_session_kwh: float = None) -> pd.DataFrame:
    """Kompatibilitäts-Wrapper für visualize_forecasts.py."""
    active = df_wb.loc[df_wb["enr"] > ENR_THRESHOLD, "enr"]
    if max_kw is None:
        max_kw = float(active.max()) if not active.empty else 22.0
    if typical_kw is None:
        typical_kw = float(active.quantile(0.80)) if not active.empty else max_kw * 0.70
    if max_session_kwh is None:
        max_session_kwh = calc_max_session_kwh(df_wb["enr"])
    return predict_wallbox_ar(wb_name, clf, reg, df_wb, wb_id, max_kw, typical_kw, max_session_kwh)


# ─── Hauptpipeline ────────────────────────────────────────────────────────────
async def main(use_demo: bool, wb_config: dict) -> None:
    raw_stats = await load_data(use_demo, wb_config)

    wb_names_ordered = list(WB_CONFIG.keys())
    wb_to_idx        = {wb: i for i, wb in enumerate(wb_names_ordered)}

    SEP = "═" * 62
    sep = "─" * 62

    print(f"\n{SEP}")
    print(f"  WALLBOX-LADEPROGNOSE  │  XGBoost AR  │  {datetime.now(TZ).strftime('%d.%m.%Y %H:%M')}")
    print(f"{SEP}")

    print(f"\n{'─'*62}")
    print(f"  SCHRITT 1/4  ─  Daten laden")
    print(f"{'─'*62}")
    wb_dataframes       = {}
    wb_max_kws          = {}
    wb_typical_kws      = {}
    wb_max_session_kwhs = {}

    for wb_name, data in raw_stats.items():
        enr_slots = data["enr"] if isinstance(data, dict) else data
        if is_inactive_wallbox(enr_slots, INACTIVE_DAYS_FILTER) or len(enr_slots) < MIN_TRAIN_ROWS:
            inactive_reason = "inaktiv (>30d)" if is_inactive_wallbox(enr_slots, INACTIVE_DAYS_FILTER) else f"zu wenig Daten ({len(enr_slots)} Slots)"
            print(f"  SKIP  {wb_name:<10} {inactive_reason}")
            continue

        df_wb = build_features(enr_slots)
        n_total  = len(df_wb)
        n_active = int((df_wb["enr"] > ENR_THRESHOLD).sum())
        rate = n_active / n_total * 100

        if rate >= MIN_ACTIVE_RATE_PCT:
            wb_dataframes[wb_name]          = df_wb
            cutoff_8w = df_wb.index[-1] - pd.Timedelta(weeks=8)
            recent_vals = df_wb.loc[
                (df_wb.index >= cutoff_8w) & (df_wb["enr"] > ENR_THRESHOLD), "enr"
            ]
            active_vals = df_wb.loc[df_wb["enr"] > ENR_THRESHOLD, "enr"]
            ref_vals = recent_vals if len(recent_vals) >= 10 else active_vals
            wb_max_kws[wb_name]             = float(ref_vals.max())
            wb_typical_kws[wb_name]         = float(ref_vals.quantile(0.80))
            wb_max_session_kwhs[wb_name]    = calc_max_session_kwh(df_wb["enr"])
            print(f"  OK    {wb_name:<10}  {n_total:>5,} Slots gesamt │ "
                  f"{n_active:>4} aktiv ({rate:>4.1f}%) │ "
                  f"Max(8W) {wb_max_kws[wb_name]:.1f} kW │ "
                  f"P80(8W) {wb_typical_kws[wb_name]:.1f} kW │ "
                  f"MaxSession {wb_max_session_kwhs[wb_name]:.0f} kWh")
        else:
            print(f"  SKIP  {wb_name:<10}  Aktivrate {rate:.1f}% < {MIN_ACTIVE_RATE_PCT}% Mindest")

    if not wb_dataframes: raise RuntimeError("Keine Wallbox mit ausreichend Daten.")

    n_ok   = len(wb_dataframes)
    n_skip = len(raw_stats) - n_ok
    print(f"\n  → {n_ok} Wallboxen für Training │ {n_skip} übersprungen")

    print(f"\n{sep}")
    print(f"  SCHRITT 2/4  ─  Trainingsdaten aufbauen ({n_ok} Wallboxen)")
    print(f"{sep}")
    X_all, y_all, ts_all = prepare_ar_training_data(wb_dataframes, wb_to_idx, wb_max_kws)
    print(f"  Gesamt-Trainingsmatrix: {X_all.shape[0]:,} Zeilen × {X_all.shape[1]} Features")

    print(f"\n{sep}")
    print(f"  SCHRITT 3/4  ─  Modell-Training (2-stufig, recency-gewichtet)")
    print(f"{sep}")
    clf, reg = train_two_stage_ar(X_all, y_all, ts_all)

    fi_scores = reg.feature_importances_
    fi = sorted(zip(FEATURE_NAMES, fi_scores), key=lambda x: -x[1])
    fi_max = fi[0][1] if fi else 1
    print(f"\n  Feature Importance (Regressor, Top 10):")
    print(f"  {'Feature':<22} {'Importance':>10}  Gewichtung")
    print(f"  {'─'*22} {'─'*10}  {'─'*20}")
    for feat_name, feat_val in fi[:10]:
        bar_len = int(feat_val / fi_max * 20)
        bar = "█" * bar_len + "░" * (20 - bar_len)
        print(f"  {feat_name:<22} {feat_val:>10.4f}  {bar}")

    print(f"\n{sep}")
    print(f"  SCHRITT 4/4  ─  72h-Prognose (Autoregressiver Rollout)")
    print(f"{sep}")
    print(f"  {'Wallbox':<10} {'FC-Slots':>8} {'FC-Gesamt':>10} {'FC-Peak':>8} {'Max-Session':>12} {'Endet um':>9}")
    print(f"  {'─'*10} {'─'*8} {'─'*10} {'─'*8} {'─'*12} {'─'*9}")
    forecast_dfs = []
    actual_dfs   = []
    HIST_WEEKS = 26

    for wb_name, df_wb in wb_dataframes.items():
        wb_id           = wb_to_idx[wb_name]
        max_kw          = wb_max_kws[wb_name]
        typical_kw      = wb_typical_kws[wb_name]
        max_session_kwh = wb_max_session_kwhs[wb_name]

        df_fc = predict_wallbox_ar(wb_name, clf, reg, df_wb, wb_id, max_kw, typical_kw, max_session_kwh)

        fc_active = df_fc[df_fc['enr_kwh'] > 0]
        kwh_sum   = fc_active['enr_kwh'].sum()
        peak_kw   = fc_active['enr_kwh'].max() if len(fc_active) > 0 else 0.0
        n_fc      = len(fc_active)
        last_ts   = pd.to_datetime(fc_active['timestamp'].iloc[-1]).strftime('%a %H:%M') if n_fc > 0 else "–"
        print(f"  {wb_name:<10} {n_fc:>8} {kwh_sum:>9.1f} kWh {peak_kw:>7.1f} kW {max_session_kwh:>10.0f} kWh {last_ts:>9}")

        df_fc["type"] = "forecast"
        forecast_dfs.append(df_fc[df_fc["enr_kwh"] > 0].copy())

        hist = df_wb.tail(HIST_WEEKS * 7 * 24).copy()
        df_hist = pd.DataFrame({
            "timestamp":  hist.index.strftime("%Y-%m-%d %H:%M:%S"),
            "wallbox":    wb_name,
            "enr_kwh":    hist["enr"].values.round(4),
            "is_weekend": (hist.index.dayofweek >= 5).astype(int),
            "is_holiday": [1 if d in de_holidays_nw else 0 for d in hist.index.date],
        })
        actual_dfs.append(df_hist[df_hist["enr_kwh"] > 0].copy())

    all_fc = pd.concat(forecast_dfs, ignore_index=True)
    all_ac = pd.concat(actual_dfs, ignore_index=True)

    model_path = MODEL_DIR / "xgboost_ar_model.pkl"
    with open(model_path, "wb") as f:
        pickle.dump({
            "clf":                clf,
            "reg":                reg,
            "wb_to_idx":          wb_to_idx,
            "wb_max_kws":          wb_max_kws,
            "wb_typical_kws":      wb_typical_kws,
            "wb_max_session_kwhs": wb_max_session_kwhs,
            "feature_names":       FEATURE_NAMES,
        }, f)

    print(f"\n{SEP}")
    print(f"  FERTIG")
    print(f"{SEP}")
    print(f"  Modell-PKL:    {model_path}")
    print(f"  Forecast:      {len(all_fc)} Zeilen")
    print(f"  Actual:        {len(all_ac)} Zeilen (6 Monate)")
    print(f"{SEP}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--demo", action="store_true", help="Demo-Daten verwenden")
    args = parser.parse_args()

    asyncio.run(main(use_demo=args.demo, wb_config=WB_CONFIG))
