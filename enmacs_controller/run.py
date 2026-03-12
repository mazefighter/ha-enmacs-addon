import time
import os
import importlib.util
import requests
import yaml

# Pfade im Container (/config ist das HA-Konfigurationsverzeichnis durch map: config:rw)
SCRIPTS_DIR  = "/config/enmacs/scripts"
CONFIG_FILE  = "/config/enmacs/config/enmacs.yaml"
POLL_INTERVAL = 10  # Sekunden zwischen jedem Durchlauf

DEFAULT_CONFIG = """\
# Enmacs Konfiguration
# Sensoren, die in jedem Zyklus abgefragt und ins Log geschrieben werden:
sensors:
  - sensor.sun
  - sensor.time
"""

# ---------------------------------------------------------------------------
# HA-API Wrapper
# ---------------------------------------------------------------------------
class HAApi:
    """Einfache Wrapper-Klasse für die HA REST-API."""

    def __init__(self, token: str):
        self._base = "http://supervisor/core/api"
        self._headers = {
            "Authorization": f"Bearer {token}",
            "content-type": "application/json",
        }

    def get_state(self, entity_id: str) -> dict:
        try:
            resp = requests.get(f"{self._base}/states/{entity_id}", headers=self._headers, timeout=10)
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else "unknown"
            reason = e.response.reason if (e.response is not None and hasattr(e.response, "reason")) else ""
            body = e.response.text if (e.response is not None and hasattr(e.response, "text")) else ""
            msg = (
                f"Error fetching state for entity '{entity_id}': "
                f"{status_code} {reason} - Response: {body}"
            )
            raise requests.exceptions.HTTPError(msg, response=e.response, request=e.request) from e
        return resp.json()

    def set_state(self, entity_id: str, state: str, attributes: dict = None) -> dict:
        payload = {"state": state, "attributes": attributes or {}}
        try:
            resp = requests.post(
                f"{self._base}/states/{entity_id}",
                headers=self._headers,
                json=payload,
                timeout=10,
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else "unknown"
            reason = e.response.reason if (e.response is not None and hasattr(e.response, "reason")) else ""
            body = e.response.text if (e.response is not None and hasattr(e.response, "text")) else ""
            msg = (
                f"Error setting state for entity '{entity_id}' to '{state}': "
                f"{status_code} {reason} - Response: {body}"
            )
            raise requests.exceptions.HTTPError(msg, response=e.response, request=e.request) from e
        return resp.json()

    def call_service(self, domain: str, service: str, **kwargs) -> dict:
        try:
            resp = requests.post(
                f"{self._base}/services/{domain}/{service}",
                headers=self._headers,
                json=kwargs,
                timeout=10,
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else "unknown"
            reason = e.response.reason if (e.response is not None and hasattr(e.response, "reason")) else ""
            body = e.response.text if (e.response is not None and hasattr(e.response, "text")) else ""
            msg = (
                f"Error calling service '{domain}.{service}': "
                f"{status_code} {reason} - Response: {body}"
            )
            raise requests.exceptions.HTTPError(msg, response=e.response, request=e.request) from e
        return resp.json()


# ---------------------------------------------------------------------------
# Konfiguration laden
# ---------------------------------------------------------------------------
def load_config() -> dict:
    try:
        with open(CONFIG_FILE, "r") as f:
            config = yaml.safe_load(f) or {}
        print(f"CONFIG: {len(config.get('sensors', []))} Sensoren geladen aus {CONFIG_FILE}", flush=True)
        return config
    except FileNotFoundError:
        print(f"CONFIG: Keine Konfigurationsdatei unter {CONFIG_FILE} – verwende Defaults.", flush=True)
        return {}
    except Exception as e:
        print(f"CONFIG FEHLER: {e}", flush=True)
        return {}


# ---------------------------------------------------------------------------
# Dynamischer Skript-Manager (ähnlich AppDaemon)
# ---------------------------------------------------------------------------
class ScriptManager:
    """
    Überwacht SCRIPTS_DIR und lädt Python-Skripte dynamisch.

    Jedes Skript kann folgende optionale Funktionen definieren:
      initialize(api)  – wird einmalig beim Laden/Neuladen aufgerufen
      run(api)         – wird in jedem Zyklus aufgerufen
    """

    def __init__(self, api: HAApi):
        self._api = api
        self._scripts: dict[str, dict] = {}  # name -> {"module": mod, "mtime": float}

    def scan_and_reload(self):
        if not os.path.isdir(SCRIPTS_DIR):
            print(f"WARNUNG: Skript-Ordner nicht gefunden: {SCRIPTS_DIR}", flush=True)
            return

        try:
            current_files = {f for f in os.listdir(SCRIPTS_DIR) if f.endswith(".py")}
        except FileNotFoundError:
            # Das Verzeichnis wurde nach der obigen Prüfung gelöscht
            print(f"WARNUNG: Skript-Ordner nicht gefunden: {SCRIPTS_DIR}", flush=True)
            return

        # Gelöschte Skripte entfernen
        for name in list(self._scripts):
            if name not in current_files:
                print(f"SKRIPT ENTFERNT: {name}", flush=True)
                del self._scripts[name]

        # Neue oder geänderte Skripte laden
        for filename in sorted(current_files):
            path = os.path.join(SCRIPTS_DIR, filename)
            mtime = os.path.getmtime(path)
            if filename not in self._scripts or self._scripts[filename]["mtime"] != mtime:
                self._load(filename, path, mtime)

    def _load(self, filename: str, path: str, mtime: float):
        try:
            spec = importlib.util.spec_from_file_location(filename[:-3], path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)

            is_reload = filename in self._scripts
            self._scripts[filename] = {"module": mod, "mtime": mtime}
            print(f"SKRIPT {'NEU GELADEN' if is_reload else 'GELADEN'}: {filename}", flush=True)

            if hasattr(mod, "initialize"):
                mod.initialize(self._api)
        except Exception as e:
            print(f"SKRIPT LADEFEHLER ({filename}): {e}", flush=True)

    def run_all(self):
        for filename, entry in self._scripts.items():
            mod = entry["module"]
            if hasattr(mod, "run"):
                try:
                    mod.run(self._api)
                except Exception as e:
                    print(f"LAUFZEITFEHLER in {filename}: {e}", flush=True)


# ---------------------------------------------------------------------------
# Verzeichnisse & Standard-Konfiguration anlegen
# ---------------------------------------------------------------------------
def ensure_structure():
    os.makedirs(SCRIPTS_DIR, exist_ok=True)
    config_dir = os.path.dirname(CONFIG_FILE)
    os.makedirs(config_dir, exist_ok=True)
    if not os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "w") as f:
            f.write(DEFAULT_CONFIG)
        print(f"CONFIG: Standard-Konfiguration erstellt unter {CONFIG_FILE}", flush=True)


# ---------------------------------------------------------------------------
# Hauptprogramm
# ---------------------------------------------------------------------------
print("INIT: Enmacs Controller startet...", flush=True)

token = os.environ.get("SUPERVISOR_TOKEN")
if not token:
    print("CRITICAL: SUPERVISOR_TOKEN fehlt!", flush=True)
    token = "INVALID_TOKEN"
else:
    print(f"DEBUG: Token vorhanden ({len(token)} Zeichen)", flush=True)

api = HAApi(token)
ensure_structure()
script_manager = ScriptManager(api)

while True:
    print("----------------------------------------", flush=True)

    # Konfiguration neu einlesen (erkennt Änderungen ohne Neustart)
    config = load_config()

    # Sensoren aus der YAML-Konfiguration abfragen
    for entity_id in config.get("sensors", []):
        try:
            data = api.get_state(entity_id)
            state = data.get("state")
            unit = data.get("attributes", {}).get("unit_of_measurement", "")
            print(f"SENSOR: {entity_id} = {state} {unit}", flush=True)
        except Exception as e:
            print(f"SENSOR FEHLER ({entity_id}): {e}", flush=True)

    # Skripte aus enmacs/scripts/ laden und ausführen
    script_manager.scan_and_reload()
    script_manager.run_all()

    time.sleep(POLL_INTERVAL)