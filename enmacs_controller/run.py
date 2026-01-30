import time
import os
import requests
import json
import sys

print("INIT: Starte API-Modus...", flush=True)

# --- 1. TOKEN DEBUGGING ---
# Wir prüfen, ob der Token überhaupt im Container ankommt
token = os.environ.get("SUPERVISOR_TOKEN")

if not token:
    print("CRITICAL: KEIN Token gefunden! Umgebungsvariable SUPERVISOR_TOKEN fehlt.", flush=True)
    # Wir setzen einen Dummy, damit das Skript nicht sofort crasht,
    # aber API Aufrufe werden fehlschlagen.
    token = "INVALID_TOKEN"
else:
    print(f"DEBUG: Token ist vorhanden! (Länge: {len(token)} Zeichen)", flush=True)

# --- 2. KONFIGURATION LADEN ---
try:
    with open("/data/options.json", "r") as f:
        options = json.load(f)
    SENSOR_LIST = options.get("sensors_to_watch", [])
    print(f"CONFIG: Überwache {len(SENSOR_LIST)} Sensoren.", flush=True)
except Exception as e:
    print(f"CRITICAL: Konfigurations-Fehler: {e}", flush=True)
    SENSOR_LIST = []

# --- 3. API SETUP ---
# Die Standard-URL für Add-ons, um mit dem Supervisor zu reden
API_BASE = "http://supervisor/core/api"

headers = {
    "Authorization": f"Bearer {token}",
    "content-type": "application/json",
}

# --- 4. HAUPTSCHLEIFE ---
while True:
    print("----------------------------------------", flush=True)

    if not SENSOR_LIST:
        print("WARNUNG: Keine Sensoren in der Konfiguration (UI) eingetragen!", flush=True)

    for entity_id in SENSOR_LIST:
        try:
            # URL bauen: http://supervisor/core/api/states/sensor.xyz
            url = f"{API_BASE}/states/{entity_id}"

            # Request senden
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                data = response.json()
                state = data.get("state")
                attributes = data.get("attributes", {})
                unit = attributes.get("unit_of_measurement", "")

                print(f"ERFOLG: {entity_id} = {state} {unit}", flush=True)
            else:
                # Bei Fehler geben wir den Code und die Antwort aus
                print(f"FEHLER bei {entity_id}: Status {response.status_code}", flush=True)
                print(f"Antwort vom Server: {response.text}", flush=True)

        except Exception as e:
            print(f"EXCEPTION bei {entity_id}: {e}", flush=True)

    # 10 Sekunden warten
    time.sleep(10)