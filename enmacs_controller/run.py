import time
import sys

print("---------------------------------------", flush=True)
print("Enmacs Controller v1.0.0 gestartet!", flush=True)
print("Verbindung zur Matrix hergestellt...", flush=True)
print("---------------------------------------", flush=True)

counter = 0
while True:
    print(f"System läuft seit {counter} Sekunden. Alles okay.", flush=True)
    time.sleep(10)
    counter += 10