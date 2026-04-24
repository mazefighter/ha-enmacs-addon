"""
Wallbox Forecast Add-on — Scheduler
Liest /data/options.json, setzt Umgebungsvariablen und führt täglich um 01:00 Uhr
(Europe/Berlin) erst train_xgboost.py, dann write_influx.py als Subprocess aus.
"""

import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

import schedule

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("wallbox_forecast")

OPTIONS_FILE = "/data/options.json"
SCRIPT_DIR = Path(__file__).parent
MODELS_DIR = Path("/usr/src/models")


def load_options() -> dict:
    if not Path(OPTIONS_FILE).exists():
        log.warning("%s nicht gefunden — verwende Standard-Werte.", OPTIONS_FILE)
        return {}
    with open(OPTIONS_FILE) as f:
        return json.load(f)


def build_env(options: dict) -> dict:
    env = os.environ.copy()
    env["HASS_TOKEN"]      = options.get("hass_token", "")
    env["HASS_HOST"]       = options.get("hass_host", "ems.kis-gmbh.de")
    env["INFLUX_HOST"]     = options.get("influx_host", "localhost")
    env["INFLUX_PORT"]     = str(options.get("influx_port", 8086))
    env["INFLUX_DB"]       = options.get("influx_db", "xgb_forecasts")
    env["INFLUX_USER"]     = options.get("influx_user", "")
    env["INFLUX_PASSWORD"] = options.get("influx_password", "")
    return env


def run_pipeline() -> None:
    log.info("=== Pipeline gestartet ===")
    options = load_options()
    env = build_env(options)

    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    # Schritt 1: Training
    log.info("Starte train_xgboost.py ...")
    result = subprocess.run(
        [sys.executable, str(SCRIPT_DIR / "train_xgboost.py")],
        env=env,
    )
    if result.returncode != 0:
        log.error("train_xgboost.py fehlgeschlagen (exit code %d) — Pipeline abgebrochen.",
                  result.returncode)
        return

    # Schritt 2: InfluxDB schreiben
    log.info("Starte write_influx.py ...")
    result = subprocess.run(
        [sys.executable, str(SCRIPT_DIR / "write_influx.py")],
        env=env,
    )
    if result.returncode != 0:
        log.error("write_influx.py fehlgeschlagen (exit code %d).", result.returncode)
        return

    log.info("=== Pipeline erfolgreich abgeschlossen ===")


def main() -> None:
    options = load_options()
    run_on_startup: bool = options.get("run_on_startup", False)

    log.info("Wallbox Forecast Add-on gestartet.")
    log.info("Pipeline wird täglich um 01:00 Uhr (Europe/Berlin) ausgeführt.")

    schedule.every().day.at("01:00").do(run_pipeline)

    if run_on_startup:
        log.info("run_on_startup=true: Starte Pipeline sofort ...")
        run_pipeline()

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
