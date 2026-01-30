import time
import os
import requests
import json
import sys

# --- SETUP ---
print("INIT: Starte API-Modus...", flush=True)

# 1. Konfiguration lesen
try:
    with open("/data/options.json", "r") as f:
        options = json.load(f)
    # Die Liste aus der Config holen
    SENSOR_LIST = options.get("sensors_to_watch", [])
    print(f"CONFIG: Überwache {len(SENSOR_LIST)} Sensoren: {SENSOR_LIST}", flush=True)
except Exception as e:
    print(f"CRITICAL: Konfigurations-Fehler: {e}", flush=True)
    SENSOR_LIST = []

# 2. Authentifizierung vorbereiten
# Dieser Token wird von HA automatisch in den Container gelegt
SUPERVISOR_TOKEN = os.environ.get("SUPERVISOR_TOKEN")
API_BASE = "http://supervisor/core/api"

headers = {
    "Authorization": f"Bearer {SUPERVISOR_TOKEN}",
    "content-type": "application/json",
}

# --- HAUPTSCHLEIFE ---
while True:
    print("----------------------------------------", flush=True)

    if not SENSOR_LIST:
        print("WARNUNG: Keine Sensoren in der Konfiguration angegeben!", flush=True)

    for entity_id in SENSOR_LIST:
        try:
            # REST API Aufruf: GET /states/<entity_id>
            url = f"{API_BASE}/states/{entity_id}"
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                data = response.json()
                state = data.get("state")
                # Einheit holen (für schöneren Output)
                attributes = data.get("attributes", {})
                unit = attributes.get("unit_of_measurement", "")

                print(f"Lese {entity_id}: {state} {unit}", flush=True)
            else:
                print(f"Fehler bei {entity_id}: Status {response.status_code}", flush=True)

        except Exception as e:
            print(f"Exception bei {entity_id}: {e}", flush=True)

    # 10 Sekunden warten
    time.sleep(10)