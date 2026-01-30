import time
import sys
import datetime

# Sofortige Ausgabe erzwingen
print("INIT: Container wurde gestartet.", flush=True)

# Kurze Pause, damit das System Zeit hat, den Log-Stream zu verbinden
time.sleep(1)

print(f"START: Enmacs Controller v1.0.6 - {datetime.datetime.now()}", flush=True)

counter = 0
while True:
    print(f"PING: System läuft seit {counter} Sekunden", flush=True)
    time.sleep(11)
    counter += 11