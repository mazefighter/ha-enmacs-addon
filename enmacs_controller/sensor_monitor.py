"""
Enmacs Sensor Monitor – internes Modul.

Liest alle konfigurierten Sensoren aus der Konfiguration und gibt deren
Zustände im Log aus. Wird von run.py in jedem Zyklus aufgerufen.
"""

from __future__ import annotations
from haapi import HAApi


def print_sensors(api: HAApi, config: dict) -> None:
    """Fragt alle in der Konfiguration definierten Sensoren ab und gibt ihren Zustand aus."""
    sensors = config.get("sensors", [])
    if not sensors:
        return

    for entity_id in sensors:
        try:
            data = api.get_state(entity_id)
            state = data.get("state")
            unit = data.get("attributes", {}).get("unit_of_measurement", "")
            print(f"SENSOR: {entity_id} = {state} {unit}", flush=True)
        except Exception as e:
            print(f"SENSOR FEHLER ({entity_id}): {e}", flush=True)
