import time
import os
import importlib.util
import shutil
import json
import yaml

from haapi import HAApi

# Pfade im Container (/config ist das HA-Konfigurationsverzeichnis durch map: config:rw)
SCRIPTS_DIR            = "/config/enmacs/scripts"
CONFIG_FILE            = "/config/enmacs/config/enmacs.yaml"
SCHEMA_SRC             = "/app/enmacs_schema.json"
SCHEMA_DEST            = "/config/enmacs/config/enmacs_schema.json"
ENTITIES_PY            = "/config/enmacs/scripts/entities.py"
POLL_INTERVAL          = 10    # Sekunden zwischen jedem Zyklus
ENTITY_REFRESH_INTERVAL = 3600  # Entity-IDs einmal pro Stunde neu laden

DEFAULT_CONFIG = """\
# yaml-language-server: $schema=./enmacs_schema.json
# Enmacs Konfiguration
# Sensoren, die in jedem Zyklus abgefragt und ins Log geschrieben werden:
sensors:
  - sensor.sun
  - sensor.time
"""

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
# Entity-Autocomplete generieren
# ---------------------------------------------------------------------------
def generate_entity_autocomplete(api: HAApi) -> None:
    """Lädt alle Entity-IDs aus HA und schreibt entities.py + aktualisiert das JSON-Schema."""
    try:
        states = api.get_all_states()
        entity_ids = sorted(s["entity_id"] for s in states)
    except Exception as e:
        print(f"AUTOCOMPLETE: Entity-IDs konnten nicht geladen werden: {e}", flush=True)
        return

    # --- entities.py schreiben (Python-Autocomplete) ---
    lines = [
        "# Auto-generiert von Enmacs Controller – nicht manuell bearbeiten!",
        "# Importiere EntityId und Konstanten in deinen Skripten:",
        "#",
        "#   from entities import EntityId, SENSOR_SUN",
        "#   from haapi import HAApi",
        "#",
        "#   def run(api: HAApi) -> None:",
        "#       api.get_state(SENSOR_SUN)   # Konstante mit Autocomplete",
        "#       api.get_state(\"sensor.\")    # Literal-Typ zeigt alle Entity-IDs",
        "",
        "from typing import Literal",
        "",
        "# Alle bekannten Entity-IDs als Literal-Typ:",
        "EntityId = Literal[",
    ]
    for eid in entity_ids:
        lines.append(f'    "{eid}",')
    lines.append("]")
    lines.append("")
    lines.append("# Konstanten für direkten Zugriff mit Autocomplete:")
    for eid in entity_ids:
        const_name = eid.upper().replace(".", "_").replace("-", "_")
        lines.append(f'{const_name} = "{eid}"')
    lines.append("")

    with open(ENTITIES_PY, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"AUTOCOMPLETE: {len(entity_ids)} Entity-IDs in entities.py geschrieben.", flush=True)

    # --- JSON-Schema aktualisieren (YAML-Autocomplete) ---
    try:
        with open(SCHEMA_DEST, "r", encoding="utf-8") as f:
            schema = json.load(f)
        schema["properties"]["sensors"]["items"]["enum"] = entity_ids
        schema["properties"]["sensors"]["items"].pop("pattern", None)
        with open(SCHEMA_DEST, "w", encoding="utf-8") as f:
            json.dump(schema, f, indent=2, ensure_ascii=False)
        print(f"AUTOCOMPLETE: JSON-Schema mit {len(entity_ids)} Entity-IDs aktualisiert.", flush=True)
    except Exception as e:
        print(f"AUTOCOMPLETE: Schema-Update fehlgeschlagen: {e}", flush=True)


# ---------------------------------------------------------------------------
# Verzeichnisse & Standard-Konfiguration anlegen
# ---------------------------------------------------------------------------
def ensure_structure():
    os.makedirs(SCRIPTS_DIR, exist_ok=True)
    config_dir = os.path.dirname(CONFIG_FILE)
    os.makedirs(config_dir, exist_ok=True)
    # JSON-Schema in den Config-Ordner kopieren (für YAML-Autovervollständigung)
    if os.path.exists(SCHEMA_SRC):
        shutil.copy2(SCHEMA_SRC, SCHEMA_DEST)
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

# Beim Start sofort Entity-Autocomplete generieren
generate_entity_autocomplete(api)
last_entity_refresh = time.time()

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

    # Entity-Autocomplete stündlich aktualisieren
    if time.time() - last_entity_refresh >= ENTITY_REFRESH_INTERVAL:
        generate_entity_autocomplete(api)
        last_entity_refresh = time.time()

    time.sleep(POLL_INTERVAL)