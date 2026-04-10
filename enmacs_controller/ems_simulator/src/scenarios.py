"""
Szenarien und initiale Entity-Definitionen fuer den KIS EMS Simulator.

Jedes Szenario setzt gezielt jene Sensor-Werte, die einen bestimmten
EMS-Zustand ausloesen – ohne echte Hardware.
"""

from datetime import datetime, timedelta


def _next_dusk_iso() -> str:
    """Gibt den naechsten Sonnenuntergang als ISO-String zurueck (~20:30 Uhr)."""
    now = datetime.now()
    dusk = now.replace(hour=20, minute=30, second=0, microsecond=0)
    if dusk <= now:
        dusk += timedelta(days=1)
    return dusk.isoformat()


# ---------------------------------------------------------------------------
# Alle Entities, die der Simulator beim Start in HA anlegt.
# Format:  entity_id -> {"state": ..., "attributes": {...}}
# ---------------------------------------------------------------------------
INITIAL_ENTITIES: dict = {

    # ---- Bezugsüberwachung ------------------------------------------------
    "input_boolean.ems_netzbezug_ueberwachung_aktiv": {
        "state": "on",
        "attributes": {"friendly_name": "[SIM] Bezugsüberwachung aktiv"},
    },
    "input_boolean.ems_netzbezug_ueberwachung_simulator_aktiv": {
        "state": "on",
        "attributes": {"friendly_name": "[SIM] Bezugs-Simulator aktiv"},
    },
    "input_number.ems_netzbezug_begrenzung_simulator_leistung": {
        "state": 20,
        "attributes": {
            "friendly_name": "[SIM] Simulierter Netzbezug (kW)",
            "min": 0, "max": 300, "step": 1,
            "unit_of_measurement": "kW",
        },
    },
    "input_number.ems_netzbezug_begrenzung_limit": {
        "state": 50,
        "attributes": {
            "friendly_name": "[SIM] Bezugslimit (kW)",
            "min": 0, "max": 300, "step": 1,
            "unit_of_measurement": "kW",
        },
    },
    # Zustandssensoren – werden vom EMS beschrieben, Simulator legt sie nur an
    "sensor.ems_netzbezug_begrenzung": {
        "state": 0,
        "attributes": {
            "friendly_name": "EMS Netzbezug Begrenzung",
            "aktiviert": False,
            "aktiviert_um": "",
            "aktiviert_bis": "",
            "timestamp": "",
            "timestamp_bestaetigt": "",
            "bezug_limit": 50,
        },
    },
    "sensor.ems_netzbezug_begrenzung_ausfuehrung": {
        "state": 0,
        "attributes": {
            "friendly_name": "EMS Netzbezug Ausführung",
            "source_timestamp": "",
        },
    },

    # ---- Einspeisebegrenzung ----------------------------------------------
    "input_boolean.ems_einspeisebegrenzung_aktiv": {
        "state": "on",
        "attributes": {"friendly_name": "[SIM] Einspeisebegrenzung aktiv"},
    },
    "input_boolean.ems_einspeisebegrenzung_simulator_aktiv": {
        "state": "on",
        "attributes": {"friendly_name": "[SIM] Einspeise-Simulator aktiv"},
    },
    "input_number.ems_einspeisebegrenzung_simulator_leistung": {
        "state": 0,
        "attributes": {
            "friendly_name": "[SIM] Simulierte Einspeisung (W)",
            "min": 0, "max": 200000, "step": 100,
            "unit_of_measurement": "W",
        },
    },
    "input_number.ems_einspeisebegrenzung_limit": {
        "state": 70,
        "attributes": {
            "friendly_name": "[SIM] Einspeise-Limit (kW)",
            "min": 0, "max": 300, "step": 1,
            "unit_of_measurement": "kW",
        },
    },
    "sensor.ems_einspeisesteuerung_zustand": {
        "state": 0,
        "attributes": {
            "friendly_name": "EMS Einspeisesteuerung Zustand",
            "aktiv": False,
            "betrag": 0,
            "prozent": 100,
        },
    },
    # WR3-Begrenzung – wird vom EMS gesetzt, Simulator zeigt aktuellen Wert
    "sensor.pv_anlage_3_wirkleistungsbegrenzung": {
        "state": 100,
        "attributes": {
            "friendly_name": "[SIM] WR3 Wirkleistungsbegrenzung",
            "unit_of_measurement": "%",
            "state_class": "measurement",
        },
    },

    # ---- Physikalische Sensoren (werden von EMS gelesen) ------------------
    "sensor.grid_leistung_gesamt_in_w": {
        "state": 5000,
        "attributes": {
            "friendly_name": "[SIM] Grid Gesamtleistung",
            "unit_of_measurement": "W",
            "device_class": "power",
            "state_class": "measurement",
        },
    },
    "sensor.grid_leistung_einspeisung_in_w": {
        "state": 0,
        "attributes": {
            "friendly_name": "[SIM] Grid Einspeisung",
            "unit_of_measurement": "W",
            "device_class": "power",
            "state_class": "measurement",
        },
    },
    "sensor.pv_anlage_1_2_gesamtleistung_in_w": {
        "state": 0,
        "attributes": {
            "friendly_name": "[SIM] PV Anlage 1+2 Gesamtleistung",
            "unit_of_measurement": "W",
            "device_class": "power",
            "state_class": "measurement",
        },
    },

    # ---- Batterien / Wallboxen --------------------------------------------
    "sensor.soc_gesamt": {
        "state": 50.0,
        "attributes": {
            "friendly_name": "[SIM] Batteriespeicher SoC",
            "unit_of_measurement": "%",
            "device_class": "battery",
            "state_class": "measurement",
        },
    },
    "sensor.batterie_leistung": {
        "state": 0,
        "attributes": {
            "friendly_name": "[SIM] Batteriespeicher Leistung",
            "unit_of_measurement": "W",
            "device_class": "power",
            "state_class": "measurement",
        },
    },
    "sensor.wallbox_leistung": {
        "state": 0,
        "attributes": {
            "friendly_name": "[SIM] Wallboxen Leistung",
            "unit_of_measurement": "W",
            "device_class": "power",
            "state_class": "measurement",
        },
    },

    # ---- Sonne (benötigt von ems_einspeisebegrenzung) ---------------------
    "sensor.sun_next_dusk": {
        "state": _next_dusk_iso(),
        "attributes": {
            "friendly_name": "[SIM] Nächster Sonnenuntergang",
            "device_class": "timestamp",
        },
    },

    # ---- DWD Temperatur (benötigt von ems_supervisor) ---------------------
    "sensor.dwd_temperatur": {
        "state": 15,
        "attributes": {
            "friendly_name": "[SIM] DWD Temperatur",
            "unit_of_measurement": "°C",
            "device_class": "temperature",
            "state_class": "measurement",
            # 12 Halbstundenwerte wie vom DWD-Sensor geliefert
            "forecast": [{"temperature": 15}] * 12,
        },
    },

    # ---- EMS Ausgabe-Sensoren (werden von EMS beschrieben) ----------------
    "sensor.ems_dynamischer_strompreis": {
        "state": 0,
        "attributes": {
            "friendly_name": "EMS Dynamischer Strompreis",
            "unit_of_measurement": "EUR/kWh",
            "data": [18.5] * 96,
        },
    },

    # ---- KIS-EMS Kernsensoren fuer Ladesteuerung / Wallbox / Forecast -----
    "input_boolean.ems_ladesteuerung_aktiv": {
        "state": "on",
        "attributes": {"friendly_name": "[SIM] EMS Ladesteuerung aktiv"},
    },
    "input_boolean.ems_ladesteuerung_peak_shaving": {
        "state": "off",
        "attributes": {"friendly_name": "[SIM] Peak Shaving aktiv"},
    },
    "input_boolean.ems_ladesteuerung_niedrigpreis": {
        "state": "off",
        "attributes": {"friendly_name": "[SIM] Niedrigpreis-Laden aktiv"},
    },
    "input_number.ems_ladesteuerung_niedrigpreisladen_bis_soc": {
        "state": 70,
        "attributes": {
            "friendly_name": "[SIM] Niedrigpreis-Laden bis SoC",
            "min": 0,
            "max": 100,
            "step": 1,
            "unit_of_measurement": "%",
        },
    },
    "input_number.ems_solarprognose_pv_ertrag_bis_low": {
        "state": 25,
        "attributes": {
            "friendly_name": "[SIM] PV Ertrag Grenze Low",
            "min": 0,
            "max": 200,
            "step": 1,
            "unit_of_measurement": "kWh",
        },
    },
    "input_number.ems_solarprognose_pv_ertrag_bis_mid": {
        "state": 60,
        "attributes": {
            "friendly_name": "[SIM] PV Ertrag Grenze Mid",
            "min": 0,
            "max": 300,
            "step": 1,
            "unit_of_measurement": "kWh",
        },
    },
    "sensor.ems_ladesteuerung_grid_leistung_in_w": {
        "state": 5000,
        "attributes": {
            "friendly_name": "[SIM] Ladesteuerung Grid Leistung",
            "unit_of_measurement": "W",
            "device_class": "power",
            "state_class": "measurement",
        },
    },
    "sensor.ems_ladesteuerung_external_battery_leistung_in_w": {
        "state": 0,
        "attributes": {
            "friendly_name": "[SIM] Externe Batterie Leistung",
            "unit_of_measurement": "W",
            "device_class": "power",
            "state_class": "measurement",
        },
    },
    "sensor.ems_ladesteuerung_batterie_ueberschuss_in_w": {
        "state": 0,
        "attributes": {
            "friendly_name": "[SIM] Batterie Ueberschuss",
            "unit_of_measurement": "W",
            "device_class": "power",
            "state_class": "measurement",
        },
    },
    "sensor.ems_ladesteuerung_batterie_ueberschuss_geglaettet_in_w": {
        "state": 0,
        "attributes": {
            "friendly_name": "[SIM] Batterie Ueberschuss geglaettet",
            "unit_of_measurement": "W",
            "device_class": "power",
            "state_class": "measurement",
        },
    },
    "sensor.ems_ladesteuerung_aktive_batterien_soc": {
        "state": 50,
        "attributes": {
            "friendly_name": "[SIM] Aktive Batterien SoC",
            "unit_of_measurement": "%",
            "device_class": "battery",
            "state_class": "measurement",
        },
    },
    "sensor.ems_ladesteuerung_aktive_batterien_unterversorgung_in_w": {
        "state": 0,
        "attributes": {
            "friendly_name": "[SIM] Aktive Batterien Unterversorgung",
            "unit_of_measurement": "W",
            "device_class": "power",
            "state_class": "measurement",
        },
    },
    "sensor.ems_ladesteuerung_wb_main_status": {
        "state": "idle",
        "attributes": {"friendly_name": "[SIM] Wallbox Main Status"},
    },
    "sensor.ems_ladesteuerung_wb_temperatur_status": {
        "state": "ok",
        "attributes": {"friendly_name": "[SIM] Wallbox Temperatur Status"},
    },
    "sensor.ems_netzbezug_begrenzung_wallbox_aktiv": {
        "state": "off",
        "attributes": {"friendly_name": "[SIM] Wallbox Ueberlastregelung aktiv"},
    },
    "sensor.ems_batterie_ladezeit": {
        "state": 0,
        "attributes": {
            "friendly_name": "[SIM] EMS Batterie Ladezeit",
            "unit_of_measurement": "h",
            "rest_stunden_ladung": 0,
        },
    },
    "sensor.pv_forecast_sunshine_peaktime_in_s": {
        "state": 7200,
        "attributes": {
            "friendly_name": "[SIM] PV Forecast Peaktime",
            "unit_of_measurement": "s",
            "state_class": "measurement",
        },
    },
}


# ---------------------------------------------------------------------------
# Szenarien
# Jedes Szenario beschreibt:
#   name        – Anzeigename
#   icon        – Emoji
#   color       – Hex-Farbe für die UI-Karte
#   description – Erklärung was das Szenario testet
#   sensors     – Dict von entity_id → neuer Wert (int/float/str oder dict)
# ---------------------------------------------------------------------------
SCENARIOS: dict = {

    "normalbetrieb": {
        "name": "Normalbetrieb",
        "icon": "✅",
        "color": "#4caf50",
        "description": (
            "Normaler Betrieb. Netzbezug 20 kW bei 50 kW Limit. "
            "Einspeisung 0. PV produziert 15 kW. Temperatur 15°C. "
            "Kein EMS-Eingriff erwartet."
        ),
        "sensors": {
            "input_number.ems_netzbezug_begrenzung_simulator_leistung": 20,
            "input_number.ems_einspeisebegrenzung_simulator_leistung": 0,
            "sensor.pv_anlage_1_2_gesamtleistung_in_w": 15000,
            "sensor.grid_leistung_gesamt_in_w": 5000,
            "sensor.grid_leistung_einspeisung_in_w": 0,
            "sensor.dwd_temperatur": {
                "state": 15,
                "attributes": {
                    "friendly_name": "[SIM] DWD Temperatur",
                    "unit_of_measurement": "°C",
                    "forecast": [{"temperature": 15}] * 12,
                },
            },
        },
    },

    "lastspitze": {
        "name": "Lastspitze (Bezug)",
        "icon": "🔴",
        "color": "#f44336",
        "description": (
            "Netzbezug 80 kW > Limit 50 kW. "
            "ems_bezugsueberwachung soll Wallbox-Begrenzung auslösen. "
            "Der Simulator bestätigt den Job automatisch nach ~3 Sekunden."
        ),
        "sensors": {
            "input_number.ems_netzbezug_begrenzung_simulator_leistung": 80,
            "sensor.grid_leistung_gesamt_in_w": 80000,
        },
    },

    "bezug_normalisiert": {
        "name": "Bezug normalisiert",
        "icon": "📉",
        "color": "#ff9800",
        "description": (
            "Netzbezug 20 kW wieder unter Limit 50 kW. "
            "ems_bezugsueberwachung hält Begrenzung noch 1 Stunde aufrecht, "
            "dann automatischer Reset."
        ),
        "sensors": {
            "input_number.ems_netzbezug_begrenzung_simulator_leistung": 20,
            "sensor.grid_leistung_gesamt_in_w": 20000,
        },
    },

    "einspeisung_limit": {
        "name": "Einspeisung überschritten",
        "icon": "☀️",
        "color": "#ff6f00",
        "description": (
            "PV-Einspeisung 100 kW > Einspeise-Limit 70 kW. "
            "ems_einspeisebegrenzung soll WR auf X% abregeln."
        ),
        "sensors": {
            "input_number.ems_einspeisebegrenzung_simulator_leistung": 100000,
            "sensor.grid_leistung_einspeisung_in_w": 100000,
            "sensor.pv_anlage_1_2_gesamtleistung_in_w": 52000,
            "input_number.ems_netzbezug_begrenzung_simulator_leistung": 0,
            "sensor.grid_leistung_gesamt_in_w": 0,
        },
    },

    "einspeisung_normal": {
        "name": "Einspeisung normal",
        "icon": "🌤️",
        "color": "#8bc34a",
        "description": (
            "Einspeisung 30 kW < Limit 70 kW. "
            "Falls WR abgeregelt war: schrittweise Freigabe alle 10 Minuten."
        ),
        "sensors": {
            "input_number.ems_einspeisebegrenzung_simulator_leistung": 30000,
            "sensor.grid_leistung_einspeisung_in_w": 30000,
            "sensor.pv_anlage_1_2_gesamtleistung_in_w": 30000,
        },
    },

    "frost": {
        "name": "Frost-Warnung",
        "icon": "❄️",
        "color": "#2196f3",
        "description": (
            "Min-Temperatur in 12h-Vorhersage < 5°C. "
            "ems_supervisor soll Niedrigpreis-Automatisierung deaktivieren."
        ),
        "sensors": {
            "sensor.dwd_temperatur": {
                "state": 2,
                "attributes": {
                    "friendly_name": "[SIM] DWD Temperatur",
                    "unit_of_measurement": "°C",
                    "forecast": [
                        {"temperature": t}
                        for t in [2, 1, -1, 0, 3, 4, 1, 2, 3, 4, 5, 3]
                    ],
                },
            },
        },
    },

    "kein_frost": {
        "name": "Kein Frost",
        "icon": "🌡️",
        "color": "#4caf50",
        "description": (
            "Alle Temperaturen in 12h-Vorhersage > 5°C. "
            "ems_supervisor lässt Niedrigpreis-Automatisierung aktiv."
        ),
        "sensors": {
            "sensor.dwd_temperatur": {
                "state": 15,
                "attributes": {
                    "friendly_name": "[SIM] DWD Temperatur",
                    "unit_of_measurement": "°C",
                    "forecast": [{"temperature": 15}] * 12,
                },
            },
        },
    },

    "alles_aus": {
        "name": "Alles deaktiviert",
        "icon": "⏹️",
        "color": "#9e9e9e",
        "description": (
            "Bezugsüberwachung und Einspeisebegrenzung deaktiviert. "
            "Nützlich um den Reset-Zustand zu testen."
        ),
        "sensors": {
            "input_boolean.ems_netzbezug_ueberwachung_aktiv": "off",
            "input_boolean.ems_einspeisebegrenzung_aktiv": "off",
            "input_number.ems_netzbezug_begrenzung_simulator_leistung": 0,
            "input_number.ems_einspeisebegrenzung_simulator_leistung": 0,
        },
    },

    "alles_an": {
        "name": "Alles aktiviert",
        "icon": "▶️",
        "color": "#4caf50",
        "description": "Beide EMS-Module wieder aktivieren nach 'Alles deaktiviert'.",
        "sensors": {
            "input_boolean.ems_netzbezug_ueberwachung_aktiv": "on",
            "input_boolean.ems_einspeisebegrenzung_aktiv": "on",
        },
    },
}


# Beschreibungen der wichtigsten Entities fuer die UI-Tabelle
ENTITY_LABELS: dict = {
    "input_number.ems_netzbezug_begrenzung_simulator_leistung": "Simulierter Netzbezug (kW)",
    "input_number.ems_netzbezug_begrenzung_limit":              "Bezugslimit (kW)",
    "input_boolean.ems_netzbezug_ueberwachung_aktiv":           "Bezugsüberwachung aktiv",
    "input_boolean.ems_netzbezug_ueberwachung_simulator_aktiv": "Bezugs-Simulator aktiv",
    "sensor.ems_netzbezug_begrenzung":                          "EMS Begrenzungsstatus",
    "input_number.ems_einspeisebegrenzung_simulator_leistung":  "Simulierte Einspeisung (W)",
    "input_number.ems_einspeisebegrenzung_limit":               "Einspeise-Limit (kW)",
    "input_boolean.ems_einspeisebegrenzung_aktiv":              "Einspeisebegrenzung aktiv",
    "input_boolean.ems_einspeisebegrenzung_simulator_aktiv":    "Einspeise-Simulator aktiv",
    "sensor.ems_einspeisesteuerung_zustand":                    "EMS Einspeise-Zustand",
    "sensor.pv_anlage_3_wirkleistungsbegrenzung":               "WR3 Wirkleistungsbegrenzung (%)",
    "sensor.grid_leistung_gesamt_in_w":                         "Grid Gesamtleistung (W)",
    "sensor.grid_leistung_einspeisung_in_w":                    "Grid Einspeisung (W)",
    "sensor.pv_anlage_1_2_gesamtleistung_in_w":                 "PV Anlage 1+2 (W)",
    "sensor.soc_gesamt":                                        "Batteriespeicher SoC (%)",
    "sensor.batterie_leistung":                                 "Batterie Leistung (W)",
    "sensor.wallbox_leistung":                                  "Wallbox Leistung (W)",
    "sensor.sun_next_dusk":                                     "Nächster Sonnenuntergang",
    "sensor.dwd_temperatur":                                     "DWD Temperatur (°C)",
    "sensor.ems_dynamischer_strompreis":                        "Dynamischer Strompreis (€/kWh)",
    "sensor.ems_ladesteuerung_grid_leistung_in_w":              "Ladesteuerung Grid Leistung (W)",
    "sensor.ems_ladesteuerung_external_battery_leistung_in_w":  "Externe Batterie Leistung (W)",
    "sensor.ems_ladesteuerung_batterie_ueberschuss_in_w":       "Batterie Ueberschuss (W)",
    "sensor.ems_ladesteuerung_batterie_ueberschuss_geglaettet_in_w": "Batterie Ueberschuss geglaettet (W)",
    "sensor.ems_ladesteuerung_aktive_batterien_soc":            "Aktive Batterien SoC (%)",
    "sensor.ems_ladesteuerung_aktive_batterien_unterversorgung_in_w": "Aktive Batterien Unterversorgung (W)",
    "sensor.ems_ladesteuerung_wb_main_status":                  "Wallbox Main Status",
    "sensor.ems_ladesteuerung_wb_temperatur_status":            "Wallbox Temperatur Status",
    "sensor.ems_netzbezug_begrenzung_wallbox_aktiv":            "Wallbox Ueberlastregelung aktiv",
    "sensor.ems_batterie_ladezeit":                             "Batterie Ladezeit (h)",
    "sensor.pv_forecast_sunshine_peaktime_in_s":                "PV Forecast Peaktime (s)",
}
