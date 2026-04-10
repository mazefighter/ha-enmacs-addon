import zoneinfo
from enum import Enum
from datetime import datetime, timedelta, timezone, time, date

import ha_wrapper as hass
import ha_wrapper as ad

SERVICE_E3DC_RSCP_SET_POWER_MODE = 'e3dc_rscp/set_power_mode'

UNBEKANNTE_BATTERIE = "Unbekannte Batterie"

VOLTERION_BATTERY_STATUS_STANDBY = [1,4] # Status 1 und 4 können

CHARGE_POWER_MAX_E3DC_THRESHOLD = 28000

MAX_LADELIMIT_NACH_ERSTLADUNGSKAPAZITAET = 1.0
""" 
Prozentsatz der maximalen Ladeleistung einer Batterie, mit der geladen wird, wenn der min_soc_erstladung erreicht ist.
Dient dazu den nachfolgenden Batterien, bzw. Wallboxen parallel Ladestrom abzutreten
"""
SET_VALUE_FOR_INPUT_NUMBER = "input_number/set_value"

RUN_EVERY_SEC = 15

SENSOR_EMS_LADESTEUERUNG_GRID_LEISTUNG_IN_W = "sensor.grid_leistung_gesamt_in_w"
SENSOR_EMS_LADESTEUERUNG_EXTERNAL_BATTERY_LEISTUNG_IN_W = "sensor.ems_ladesteuerung_e3dc_leistung_in_w"
SENSOR_EMS_LADESTEUERUNG_BATTERIE_UEBERSCHUSS_IN_W = "sensor.ems_ladesteuerung_batterie_ueberschuss_in_w"
SENSOR_EMS_LADESTEUERUNG_AKTIVE_BATTERIEN_SOC = "sensor.ems_ladesteuerung_aktive_batterien_soc"
SENSOR_EMS_LADESTEUERUNG_AKTIVE_BATTERIEN_UNTERVERSORGUNG = "sensor.ems_ladesteuerung_aktive_batterien_unterversorgung_in_w"
TIME_ENDE_LADESTEUERUNG_SOC = time(17, 0, 0)
TIMEZONE_BERLIN = zoneinfo.ZoneInfo("Europe/Berlin")
MIN_LADELEISTUNG_PRE_PEAKSHAVING = 500 #Ladelesitung in W
BATTERIE_UNTERVERSORGUNG_SOC_THRESHOLD = 30.0 # Prozentangabe, ab wann die Batteriekapazität als unterversorgt gilt
SENSOR_NIEDRIGPREISLADEN_SOC_THRESHOLD = 'input_number.ems_ladesteuerung_niedrigpreisladen_bis_soc'


class Arbeitsmodus(Enum):
    LADEN = "laden"
    ENTLADEN = "entladen"


class EmsBatteriemanagement(hass.Hass):

    def initialize(self):
        self.log("Batterie-Management gestartet")

        self.config = {
            "aktiv_sensor": "input_boolean.ems_ladesteuerung_aktiv",
            "dry_run": False,
            "zeit_ende_lade_steuerung": TIME_ENDE_LADESTEUERUNG_SOC,
            "sensor_ems_ladesteuerung_grid_leistung_in_w": SENSOR_EMS_LADESTEUERUNG_GRID_LEISTUNG_IN_W,
            "externer_batterie_service": "e3dc_rscp/set_power_limits",
            "externer_batterie_param_max_charge_in_w": "max_charge",
            "externer_batterie_param_max_discharge_in_w": "max_discharge",
            "kritische_batterie_sensoren": ["soc_sensor", "ladeleistung_sensor", "entladeleistung_sensor"]
        }
        # Batteriekonfiguration
        init_last_notification_dt = datetime.now() - timedelta(minutes=50)
        self.batterien = [
            {
                "aktiv": True,
                "typ": "e3dc",
                "name": "E3DC",
                "max_ladeleistung_preset": 12500,  # Watt
                "max_ladeleistung": 10000,  # Watt
                "max_entladeleistung": 10000,  # Watt
                "max_entladeleistung_preset": 12500,  # Watt
                "min_ladeleistung": 500,  # Watt
                "min_entladeleistung": 500,  # Watt
                "min_soc_erstladung": 20,  # Prozent
                "soc_ladung_reduzieren": 90,  # Prozent
                "min_soc_entladung": 5,  # nach Erstladung bis zu diesem SOC entladen
                "min_soc_generell": 5,  # Minimaler SOC der Batterie
                "max_soc_ladung": 99, # Maximaler SOC, dann wird auf Entladung gesetzt, speziell für Redox-Flow-Batterien
                "last_day_erstladung": None,  # Letzter Tag, an dem Erstladung erreicht wurde
                "min_soc_erstladung_sensor": "input_number.ems_ladesteuerung_e3dc_erstladung_bis_soc",
                "min_soc_entladung_sensor": "input_number.ems_ladesteuerung_e3dc_entadung_bis_soc",
                "eingestellte_ladeleistung_sensor": None,
                "eingestellte_ladeleistung": None,
                "justiere_entladeleistung": None, # Keine Korrektur der Leistungen notwendig
                "justiere_ladeleistung":  None,
                "justiere_entladeleistung_max":  None,
                "justiere_ladeleistung_max":  None,
                "prioritaet": 1,
                "soc": None,
                "soc_sensor": "sensor.quattroporte_state_of_charge",
                "temperatur": None,
                "temperatur_sensor": None,
                "temperatur_reduzierung": None,
                "temperatur_reduzierung_bis": None,
                "temperatur_reduzierung_aktiv": None,
                "ladeleistung_sensor": SENSOR_EMS_LADESTEUERUNG_EXTERNAL_BATTERY_LEISTUNG_IN_W,
                "ladeleistung": None,
                "entladeleistung": None,
                "entladeleistung_sensor": None,
                "soll_ladeleistung_input_test": "input_number.batterie1_soll_ladeleistung",  # Negativ für Entladung nur zum Testen
                "soll_ladeleistung_input": None,  # nicht für E3DC
                "hat_eigene_regelung": True,
                "externe_device_id": "2ebd6a796df261e0a689179d39cc51f3",
                "prioritaet_sensor": "input_number.ems_ladesteuerung_e3dc_prio",
                "gesamtkapazitaet": 75000,  # in W
                "aktuelle_restkapazitaet": None,
                "last_notification_inactive": init_last_notification_dt,
                "initialisierungs_register": [],
                "terminierungs_register": [],
            },
            {
                "aktiv": True,
                "typ": "volterion",
                "name": "Redox-1",
                "max_ladeleistung_preset": 24000,
                "max_ladeleistung": 24000,
                "max_entladeleistung_preset": 12000,
                "max_entladeleistung": 12000,
                "min_ladeleistung": 2200,
                # Watt Redox-Batterien schalten sich ab, daher sollte immer etwas entladen oder geladen werden
                "min_entladeleistung": 1500,  # Watt
                "min_soc_erstladung": 11,
                "min_soc_entladung": 11,
                "min_soc_generell": 9,  # Redox-Batterien gehen in Standby bei 0
                "max_soc_ladung": 100,
                "last_day_erstladung": None,  # Letzter Tag, an dem Erstladung erreicht wurde
                "soc_ladung_reduzieren": 93,
                "min_soc_erstladung_sensor": "input_number.ems_ladesteuerung_redox_1_erstladung_bis_soc",
                "min_soc_entladung_sensor": "input_number.ems_ladesteuerung_redox_1_entadung_bis_soc",
                "eingestellte_ladeleistung_sensor": "sensor.redox1_settings_ess_acpowersetpoint_memory",
                "eingestellte_ladeleistung": None,
                "justiere_entladeleistung": +200, # Keine Korrektur der Leistungen notwendig
                "justiere_ladeleistung":  -200,
                "justiere_entladeleistung_max": 4000,
                "justiere_ladeleistung_max": 5000, # Schwellwert Entladeleistung, bis zu dem Justierung gemacht wird
                "prioritaet": 2,
                "ladeleistung": None,
                "entladeleistung": None,
                "soc": None,
                "soc_sensor": "sensor.redox_1_vt_soc",
                "temperatur": None,
                "temperatur_sensor": "sensor.redox_1_vt_temp_rest",
                "temperatur_reduzierung": 41.5,
                "temperatur_reduzierung_bis": 40.5,
                "temperatur_reduzierung_aktiv": None,
                "ladeleistung_sensor": "sensor.batterien_redox_1_ladeleistung",
                "entladeleistung_sensor": "sensor.batterien_redox_1_entladeleistung",
                "soll_ladeleistung_input": "input_number.batterien_redox1_ladeleistung_festlegen",  # UI-Feld
                "soll_ladeleistung_input_manuell": "input_boolean.redox_1_ladeleistung_manuell",  # UI-Feld für Edit-Mode
                "soll_ladeleistung_input_test": "input_number.batterie2_soll_ladeleistung",
                "hat_eigene_regelung": False,
                "prioritaet_sensor": "input_number.ems_ladesteuerung_redox_1_prio",
                "gesamtkapazitaet": 130000,  # in W
                "aktuelle_restkapazitaet": None,
                "last_notification_inactive": init_last_notification_dt,
                "modbus_hub": "victron_redox_1",
                "modbus_slave": 100,
                "modbus_address": 2716,
                "initialisierungs_register": [],
                "terminierungs_register": [],
            },
            {
                "aktiv": True,
                "typ": "volterion",
                "name": "Redox-2",
                "max_ladeleistung_preset": 30000,
                "max_ladeleistung": 30000,
                "max_entladeleistung_preset": 12000,
                "max_entladeleistung": 12000,
                "min_ladeleistung": 3000,
                # Watt Redox-Batterien schalten sich ab, daher sollte immer etwas entladen oder geladen werden
                "min_entladeleistung": 2000,  # Watt
                "min_soc_erstladung": 11,
                "min_soc_entladung": 11,
                "min_soc_generell": 9,  # neue SW-Version von Volterion ab 0.8 erlaubn
                "max_soc_ladung": 100,
                "last_day_erstladung": None,  # Letzter Tag, an dem Erstladung erreicht wurde
                "soc_ladung_reduzieren": 94,
                "min_soc_erstladung_sensor": "input_number.ems_ladesteuerung_redox_2_erstladung_bis_soc",
                "min_soc_entladung_sensor": "input_number.ems_ladesteuerung_redox_2_entadung_bis_soc",
                "eingestellte_ladeleistung_sensor": "sensor.redox2_settings_ess_acpowersetpoint_memory",
                "eingestellte_ladeleistung": None,
                "justiere_entladeleistung": -0, # Korrigiere Entladeleistung um den angegebenen Wert, da Redox-2 bei kleinen Leistungen immer 2kW zu wenig entlädt
                "justiere_ladeleistung": -0, # Korrigiere Ladeleistung um den angegebenen Wert, da Redox-2 bei kleinen Leistungen immer 1kW zu viel lädt
                "justiere_entladeleistung_max": 9000, # Schwellwert Ladeleistung, bis zu dem Justierung gemacht wird
                "justiere_ladeleistung_max": 9000, # Schwellwert Entladeleistung, bis zu dem Justierung gemacht wird
                "prioritaet": 3,
                "prioritaet_sensor": "input_number.ems_ladesteuerung_redox_2_prio",
                "ladeleistung": None,
                "entladeleistung": None,
                "soc": None,
                "soc_sensor": "sensor.redox_2_vt_soc_2",
                "temperatur": None,
                "temperatur_sensor": "sensor.redox_2_vt_temp_2",
                "temperatur_reduzierung": 41.9,
                "temperatur_reduzierung_bis": 41.0,
                "temperatur_reduzierung_aktiv": None,
                "ladeleistung_sensor": "sensor.rfb_140_ladeleistung_2",
                "entladeleistung_sensor": "sensor.rfb_140_entladeleistung_2",
                "soll_ladeleistung_input": "input_number.batterien_redox2_ladeleistung_festlegen",  # UI-Feld
                "soll_ladeleistung_input_manuell": "input_boolean.redox_2_ladeleistung_manuell",  # UI-Feld für Edit-Mode
                "soll_ladeleistung_input_test": "input_number.batterie3_soll_ladeleistung",
                "hat_eigene_regelung": False,
                "gesamtkapazitaet": 130000,  # in W
                "aktuelle_restkapazitaet": None,
                "last_notification_inactive": init_last_notification_dt,
                "status": None,
                "status_sensor": "sensor.redox_2_vt_state",
                'ist_status_in_standby': lambda status_float: round(status_float) in VOLTERION_BATTERY_STATUS_STANDBY,
                "modbus_hub": "volterion_redox2",
                "modbus_slave": 2,
                "modbus_address": 42102,
                "initialisierungs_register": [
                    { "value_sensor": None, "set_register": 42096, "set_value": 1 }, # Connection
                    { "value_sensor": None, "set_register": 42097, "set_value": 1},  # Enable EMS
                    { "value_sensor": "sensor.redox_2_vt_mode", "set_register": 42098, "set_value": 2},  # Mode = External Setpoint
                ],
                "terminierungs_register": [
                    { "value_sensor": None, "set_register": 42096, "set_value": 1},  # Connection
                    { "value_sensor": None, "set_register": 42097, "set_value": 1},  # Enable EMS
                    { "value_sensor": "sensor.redox_2_vt_mode", "set_register": 42098, "set_value": 3},  # Mode = OnOff
                ],
            },
        ]

        if self.get_state(SENSOR_EMS_LADESTEUERUNG_BATTERIE_UEBERSCHUSS_IN_W) is None:
            self.set_state(SENSOR_EMS_LADESTEUERUNG_BATTERIE_UEBERSCHUSS_IN_W, state=0,
                           attributes={"unit_of_measurement": 'W',
                                       "device_class": "power", "state_class": "measurement"})
        if self.get_state(SENSOR_EMS_LADESTEUERUNG_AKTIVE_BATTERIEN_SOC) is None:
            self.set_state(SENSOR_EMS_LADESTEUERUNG_AKTIVE_BATTERIEN_SOC, state=0,
                           attributes={"unit_of_measurement": "%", "icon": "mdi:battery-outline",
                                       "device_class": "battery", "state_class": "measurement"})
        if self.get_state(SENSOR_EMS_LADESTEUERUNG_AKTIVE_BATTERIEN_UNTERVERSORGUNG) is None:
            self.set_state(SENSOR_EMS_LADESTEUERUNG_AKTIVE_BATTERIEN_UNTERVERSORGUNG, state=0,
                           attributes={"unit_of_measurement": 'W',
                                       "device_class": "power", "state_class": "measurement"})

        self.initialize_batterien()
        # Reinitialisieren wegen Sychronisierungsschwierigkeiten bei Code-Änderung
        self.get_ad_api().run_in(self.reinit_batteries, 15)

        self.run_every(self.ladesteuerung_batterien, datetime.now(), RUN_EVERY_SEC)

        self.initialize_peak_shaving()
        self.initialize_ac_laden()

    def update_batteriestatus(self):
        """
        Updates the status of the batteries by iterating through provided battery data,
        retrieving associated sensor states, and performing various calculations to
        determine battery activity, state of charge (SOC), remaining capacity, and
        critical conditions. Logs detailed information regarding each battery's
        status and performs checks for last update and temperature reductions.

        :param batterien: A list of dictionaries representing batteries and their associated
            attributes, such as sensor IDs and capacity data.

        :raises ValueError: If any calculation or data retrieval encounters a state that
            cannot be processed.

        :return: None
        """
        for batterie in self.batterien:
            letzte_aktualisierung =datetime.now().astimezone(timezone.utc) - timedelta(days=2)
            sensor_attributes = [ (key) for key in batterie.keys() if key.endswith("_sensor") ]
            for sensor_name in sensor_attributes:
                if batterie[sensor_name] is not None:
                    sensor_state = self.get_state(batterie[sensor_name] )
                    if sensor_state is None:
                        self.log(f"Sensor {batterie[sensor_name]} nicht gefunden.")
                        continue
                    elif sensor_name in self.config["kritische_batterie_sensoren"]:
                        letzte_aktualisierung = self.pruefe_letzte_aktualisierung(batterie, sensor_name, letzte_aktualisierung)
                    value_attribut = sensor_name.removesuffix("_sensor")
                    sensor_value = self.to_float_safe(sensor_state)
                    if value_attribut in batterie:
                        batterie[value_attribut] = sensor_value
            # Berechnungen durchführen
            if batterie["prioritaet"] is not None and (round(batterie["prioritaet"], 0) == 0):
                self.log(f"Batterie {batterie['name']} ist nicht aktiv!")
                batterie["aktiv"] = False
            else:
                batterie["aktiv"] = True
                self.check_batterie_aktiv(batterie, letzte_aktualisierung)
            if batterie["aktiv"]:
                if batterie["soc"] and not self.check_batterie_temp_reduzierung_aktiv(batterie):
                    batterie["aktuelle_restkapazitaet"] = round(( batterie["soc"] / 100) * batterie["gesamtkapazitaet"], 0)
                else:
                    batterie["aktuelle_restkapazitaet"] = 0

            self.log(f"Batterie {batterie['name']} - Aktiv: {batterie['aktiv']}, Temp-Reduzierung: {self.check_batterie_temp_reduzierung_aktiv(batterie)}, SOC: {batterie['soc']}, "
                     f"aktuelle Restkapazitaet: {batterie['aktuelle_restkapazitaet']}, Prio: {batterie['prioritaet']}", level="INFO")

    def check_batterie_aktiv(self, batterie, letzte_aktualisierung):
        keine_aktualisierung_seit_sekunden = (
                    datetime.now().astimezone(timezone.utc) - letzte_aktualisierung).total_seconds()
        redox =  (batterie["typ"]  is not None) and (batterie["typ"] == "volterion")
        if keine_aktualisierung_seit_sekunden > (15 * 60):
            if not redox and self.get_batterie_ac_ladeleistung() > 0: # Bei AC-Laden ist e3dc auf Enladeleistung 0, daher diese Batterie nicht auf deaktiviert setzen
                return
            if redox and self.__ist_direkt_steuerbar(batterie):
                if self.__ist_status_aktuell_standby(batterie): #Bei Redox Status Standby Batterie nicht auf inaktiv setzen
                    self.log(f"Batterie {batterie['name']} ist in Standby {round(batterie.get("status", 0))} und wird nicht deaktiviert.")
                    return
            self.log(
                f"Batterie {batterie['name']} wird deaktiviert!  (now: {datetime.now().astimezone(timezone.utc)}) - letzte Aktualisierung: {letzte_aktualisierung} ({keine_aktualisierung_seit_sekunden}s)")
            batterie["aktiv"] = False
            if keine_aktualisierung_seit_sekunden < (
                    60 * 60) and redox:  # Notification nur innerhalb der ersten Stunde um Wiederholungen im Stundenrhythmus zu vermeiden
                self.log(f"keineAktualisierung seit Sekunden: {keine_aktualisierung_seit_sekunden}")
                self.send_notification_status_inactive(batterie,
                                                    f"Batterie {batterie['name']} wird deaktiviert!  - letzte Aktualisierung vor mehr als {'15' if redox else '15'} Minuten: {letzte_aktualisierung.astimezone(TIMEZONE_BERLIN)}")

    def check_batterie_temp_reduzierung_aktiv(self, batterie):
        return (batterie["temperatur_reduzierung_aktiv"] is not None and batterie["temperatur_reduzierung_aktiv"] is True)

    def pruefe_letzte_aktualisierung(self, batterie, sensor_name, letzte_aktualisierung):
        """
        Checks the latest update or report timestamp for a specific sensor associated with a battery. It retrieves and compares
        the timestamps of the sensor's last reported and last updated state. If one of these timestamps is more recent than
        the current value of `letzte_Aktualisierung`, it updates `letzte_Aktualisierung` and logs the corresponding information.

        :param batterie: Dictionary containing battery information and associated sensors. Expects a sensor_name key in the
                         dictionary for sensor reference.
        :type batterie: dict
        :param sensor_name: Name of the sensor whose last reported or updated timestamp is being checked.
        :type sensor_name: str
        :param letzte_aktualisierung: The current timestamp to be compared with the sensor's last reported and updated values.
        :type letzte_aktualisierung: float
        :return: The updated `letzte_Aktualisierung` value if a more recent timestamp is found; otherwise, it remains unchanged.
        :rtype: float
        """
        letzte_meldung = self.get_state(batterie[sensor_name], attribute="last_reported")
        if letzte_meldung and self.to_datetime_safe(letzte_meldung, default=letzte_aktualisierung) > letzte_aktualisierung:
            letzte_aktualisierung = self.to_datetime_safe(letzte_meldung, default=letzte_aktualisierung)
            self.log(
                f"Batterie {batterie['name']} - {sensor_name} - letzte Meldung: {letzte_aktualisierung.astimezone(TIMEZONE_BERLIN)}",
                level="DEBUG")
        elif not letzte_meldung:
            self.log(f"Batterie {batterie['name']} - {sensor_name} - letzte Meldung nicht gefunden.", level="INFO")
        letztes_update = self.get_state(batterie[sensor_name], attribute="last_updated")
        if letztes_update and self.to_datetime_safe(letztes_update, default=letzte_aktualisierung) > letzte_aktualisierung:
            letzte_aktualisierung = self.to_datetime_safe(letztes_update, default=letzte_aktualisierung)
            self.log(
                f"Batterie {batterie['name']} - {sensor_name} - letzte Aktualisierung: {letzte_aktualisierung.astimezone(TIMEZONE_BERLIN)}",
                level="DEBUG")
        elif not letzte_meldung:
            self.log(f"Batterie {batterie['name']} - {sensor_name} - letzte Aktualisierung nicht gefunden.", level="INFO")

        return letzte_aktualisierung

    def ladesteuerung_batterien(self, kwargs):
        ladesteuerung_aktiv = self.get_state(self.config["aktiv_sensor"])
        if ladesteuerung_aktiv and (ladesteuerung_aktiv == "on"):
            self.update_batteriestatus()
            self.manage_batterien()

    def manage_batterien(self):
        grid_leistung = self.get_state(SENSOR_EMS_LADESTEUERUNG_GRID_LEISTUNG_IN_W)
        if grid_leistung is None:
            self.log("Grid-Leistungssensor nicht gefunden.")
            return

        grid_leistung = self.to_float_safe(grid_leistung)
        batterie_ladungen = 0;
        batterie_entladungen = 0;
        for batterie in self.batterien:
            if batterie["aktiv"]:
                if batterie["hat_eigene_regelung"]:
                    # nur 1 Sensor für Beladungen > 0 und Entladungen < 0
                    lade_entlade_leistung = batterie["ladeleistung"] if batterie["ladeleistung"] else 0
                    if lade_entlade_leistung < 0:
                        batterie_entladungen += abs(lade_entlade_leistung)
                    else:
                        batterie_ladungen += lade_entlade_leistung
                else:
                    batterie_ladungen += batterie["ladeleistung"] if batterie["ladeleistung"] else 0
                    batterie_entladungen += batterie["entladeleistung"] if batterie["entladeleistung"] else 0
        zu_verteilende_leistung = grid_leistung + batterie_entladungen - batterie_ladungen - self.get_batterie_ac_ladeleistung()
        # Lade- oder Entladeentscheidung
        if zu_verteilende_leistung < 0:
            self.lade_batterien(abs(zu_verteilende_leistung))
        elif zu_verteilende_leistung > 0:
            self.entlade_batterien(zu_verteilende_leistung)



    def lade_batterien(self, ladebedarf):
        priorisierende_batterien = self.ermittle_priorisierende_batterien(Arbeitsmodus.LADEN)
        batterien_aktuelle_prio = []
        batterien_aktuelle_prio.extend(priorisierende_batterien["priovorgaenger"])
        batterien_aktuelle_prio.extend(priorisierende_batterien["priorisiert"])
        ladebedarf_verbleibend = self.lade_batterien_gleicher_prio(batterien_aktuelle_prio, ladebedarf)
        if ladebedarf_verbleibend >= 500 :
            batterien_hoehere_prio = priorisierende_batterien["restliche"]
            ladebedarf_verbleibend = self.lade_batterien_gleicher_prio(batterien_hoehere_prio, ladebedarf_verbleibend)
        self.log(f"lade_batterien: nicht verteilte Kapazitaet: {ladebedarf_verbleibend}")
        self.__calc_and_save_aktive_batterien_soc()
        self.__calc_and_save_aktive_batterien_unterversorgung_in_w(BATTERIE_UNTERVERSORGUNG_SOC_THRESHOLD)
        self.save_ladeleistung_ueberschuss(max(0, ladebedarf_verbleibend - self.get_batterie_ac_ladeleistung()))

    def lade_batterien_gleicher_prio(self, batterien_aktuelle_prio, ladebedarf_verbleibend):
        gesamte_ladbare_kapazitaet = 0
        ladebedarf = ladebedarf_verbleibend
        batterienamen = ", ".join([batterie.get("name", "Unbekannt") for batterie in batterien_aktuelle_prio])
        for batterie in batterien_aktuelle_prio:
            ladbare_kapazitaet = batterie["gesamtkapazitaet"] - batterie["aktuelle_restkapazitaet"]
            if ladbare_kapazitaet > 0:  # Stelle sicher, dass die Kapazitaet nicht negativ ist
                gesamte_ladbare_kapazitaet += ladbare_kapazitaet
        self.log(f"batterien: {batterienamen}, gesamt ladbare Kapazitaet: {gesamte_ladbare_kapazitaet} Watt, ladebedarf: {ladebedarf} Watt ")
        for batterie in batterien_aktuelle_prio:
            ladeleistung_batterie = self.ermittle_ladeleistung_batterie(batterie, gesamte_ladbare_kapazitaet,
                                                                        ladebedarf)
            self.setze_ladeleistung_in_ha_modbus(batterie, ladeleistung_batterie)
            ladebedarf_verbleibend -= ladeleistung_batterie
        return ladebedarf_verbleibend

    def setze_ladeleistung_in_ha_input(self, batterie, ladeleistung_batterie):
        """
        Adjusts the charging power of a battery within a home automation system based on specified settings
        and certain conditions. The method considers the battery type and the configured charging power
        to decide whether to trigger specific automation services for modifying input values.

        :param batterie: A dictionary containing information about the battery. It must include keys such
                         as "typ", "eingestellte_ladeleistung", "soll_ladeleistung_input",
                         "soll_ladeleistung_input_test", and "soll_ladeleistung_input_manuell".
        :param ladeleistung_batterie: The target charging power to be set for the battery. Must be an
                                       integer or a float.
        :return: None
        """
        ladeleistung_batterie_unjustiert = ladeleistung_batterie
        redox =  (batterie["typ"]  is not None) and (batterie["typ"] == "volterion")
        ladeleistung_batterie = self.justiere_ladeleistung(batterie, ladeleistung_batterie)
        value_changed = batterie["eingestellte_ladeleistung"] is not None and int(batterie["eingestellte_ladeleistung"]) != ladeleistung_batterie
        if redox and value_changed:
            self.call_service("input_boolean/turn_on", entity_id=batterie["soll_ladeleistung_input_manuell"])
            self.log(f"input_number/set_value, entity_id={batterie['soll_ladeleistung_input']}, value={ladeleistung_batterie}), unjustiert: {ladeleistung_batterie_unjustiert}")
            self.call_service(SET_VALUE_FOR_INPUT_NUMBER, entity_id=batterie["soll_ladeleistung_input"],
                              value=ladeleistung_batterie)
        if not value_changed:
            self.log(f"unveraendert: input_number/set_value, entity_id={batterie['soll_ladeleistung_input_test']}, value={ladeleistung_batterie}), unjustiert: {ladeleistung_batterie_unjustiert}")
        self.call_service(SET_VALUE_FOR_INPUT_NUMBER, entity_id=batterie["soll_ladeleistung_input_test"],
                          value=ladeleistung_batterie)
        if redox and value_changed:
            self.call_service("input_boolean/turn_off", entity_id=batterie["soll_ladeleistung_input_manuell"])

    def setze_ladeleistung_in_ha_modbus(self, batterie, ladeleistung_batterie):
        """
        Adjusts the charging power of a battery within a home automation system based on specified settings
        and certain conditions. The method considers the battery type and the configured charging power
        to decide whether to trigger specific automation services for modifying input values.

        :param batterie: A dictionary containing information about the battery. It must include keys such
                         as "typ", "eingestellte_ladeleistung", "soll_ladeleistung_input",
                         "soll_ladeleistung_input_test", and "soll_ladeleistung_input_manuell".
        :param ladeleistung_batterie: The target charging power to be set for the battery. Must be an
                                       integer or a float.
        :return: None
        """
        ladeleistung_batterie_unjustiert = ladeleistung_batterie
        redox =  (batterie["typ"]  is not None) and (batterie["typ"] == "volterion")
        ladeleistung_batterie = self.justiere_ladeleistung(batterie, ladeleistung_batterie)
        value_changed = True # Setze immer auf true um Victron Eingriff zu übersteuern: batterie["eingestellte_ladeleistung"] is not None and int(batterie["eingestellte_ladeleistung"]) != ladeleistung_batterie
        if redox and value_changed:
            self.log(f"modbus/write_register, hub={batterie['modbus_hub']}, slave={batterie['modbus_slave']}, address={batterie['modbus_address']}, "
                     f"value={EmsBatteriemanagement.signedint32_to_modbusintarray(ladeleistung_batterie)}({ladeleistung_batterie}),  unjustiert: {ladeleistung_batterie_unjustiert}")
            self.call_service("modbus/write_register", hub=batterie['modbus_hub'], slave=batterie['modbus_slave'],
                              address=batterie['modbus_address'], value=self.signedint32_to_modbusintarray(ladeleistung_batterie))
        if not value_changed:
            self.log(f"unveraendert: input_number/set_value, entity_id={batterie['soll_ladeleistung_input_test']}, value={ladeleistung_batterie}),  unjustiert: {ladeleistung_batterie_unjustiert}")
        self.call_service("input_number/set_value", entity_id=batterie["soll_ladeleistung_input_test"],
                          value=ladeleistung_batterie)

    def save_ladeleistung_ueberschuss(self, state):
        """
        Updates the state of the battery surplus charging control by saving the
        provided state and retaining the existing attributes. This function retrieves
        the current attributes of the specified sensor, then updates the sensor's
        state with the given value while preserving all prior attributes.

        :param state: The new state to set for the battery surplus charging control.
                      The value should represent the updated state to be stored.
        :return: None
        """
        attributes = self.get_state(SENSOR_EMS_LADESTEUERUNG_BATTERIE_UEBERSCHUSS_IN_W, attribute="all").get("attributes", {})
        self.set_state(SENSOR_EMS_LADESTEUERUNG_BATTERIE_UEBERSCHUSS_IN_W, state=state,
                       attributes=attributes)

    @staticmethod
    def signedint32_to_modbusintarray(val: int) -> list[int]:
        """
        Converts a signed 32-bit integer to a list of 2 hexadecimal word values (unsigned).
        A word is considered to be 16 bits.  The function returns the words in
        big-endian order.  The sign of the input value is preserved in the
        output words.

        Args:
            val: The signed 32-bit integer to convert.

        Returns:
            A list of 2 integers, each representing a 16-bit word in hexadecimal
            format (0-65535). The list is for modbus in big-endian order
            (most significant word first).  The values in the list
            are unsigned, but represent the signed value.

        Raises:
            TypeError: If the input value is not an integer or float (latter will be rounded).
            ValueError: If the input value is not a 32-bit integer
                (outside the range of -2147483648 to 2147483647).
        """
        if isinstance(val, float):
            val = round(val)
        if not isinstance(val, int):
            raise TypeError("Input value must be an integer.")
        if not -2147483648 <= val <= 2147483647:
            raise ValueError(
                "Input value must be a 32-bit signed integer (-2147483648 to 2147483647)."
            )

        # Use bitwise operations and masking to extract each word.
        word1 = val & 0xFFFF  # Get the lower 16 bits
        word2 = (val >> 16) & 0xFFFF  # Get the upper 16 bits

        return [word2, word1]

    def ermittle_ladeleistung_batterie(self, batterie, gesamte_ladbare_kapazitaet, ladebedarf):
        ladeleistung_batterie = round(
            (ladebedarf * self.ermittle_verbleibende_ladekapazitaet(batterie)
                          / gesamte_ladbare_kapazitaet), 0)
        if batterie["typ"] is not None and batterie["typ"] == "volterion":
            if batterie["soc"] is not None and batterie["soc"] >= batterie["max_soc_ladung"]:
                if self.__ist_direkt_steuerbar(batterie):
                    ladeleistung_batterie = 0 # Besser auf 0 setzen, weil dann Batterie nach einer Zeit disabled wird
                else:
                    ladeleistung_batterie = -batterie["min_entladeleistung"]
                return ladeleistung_batterie
            # Sobald wieder geladen wird, Temp-Steuerung aufheben
            if self.check_batterie_temp_reduzierung_aktiv(batterie):
                batterie["temperatur_reduzierung_aktiv"] = False

        # Wenn die Batterie die minimal geforderte Erstladungskapazität des Tages erreicht hat, nehme maximal 60% der max. Ladeleistung um nachfolgenden Batterien auch schon die Beladung zu ermöglichen
        if (batterie["soc"] is not None and batterie["soc"] >= batterie["min_soc_erstladung"]) :
            ladeleistung_batterie = min(ladeleistung_batterie, batterie["max_ladeleistung"] * MAX_LADELIMIT_NACH_ERSTLADUNGSKAPAZITAET)
        else:
            ladeleistung_batterie = min(batterie["max_ladeleistung"], ladeleistung_batterie)
        # Wenn Soc > batterie[soc_ladung_reduzieren] wird nur noch mit minimalem Ladestrom geladen
        if (batterie["soc"] is not None and batterie["soc"] >= batterie["soc_ladung_reduzieren"]) :
            ladeleistung_batterie = min(ladeleistung_batterie, batterie["min_ladeleistung"])
        if ladeleistung_batterie < batterie["min_ladeleistung"] and self.__ist_direkt_steuerbar(batterie):
            ladeleistung_batterie = 0
            return ladeleistung_batterie
        else:
            ladeleistung_batterie = round(max(batterie["min_ladeleistung"], ladeleistung_batterie), 0)
        ladeleistung_batterie = int(round(ladeleistung_batterie / 100)) * 100 # schneide auf volle 100er Werte ab
        return ladeleistung_batterie

    def ermittle_verbleibende_ladekapazitaet(self, batterie):
        return (batterie["gesamtkapazitaet"] - batterie["aktuelle_restkapazitaet"])

    def ermittle_verbleibende_entladekapazitaet(self, batterie):
        return batterie["aktuelle_restkapazitaet"]

    def ermittle_entladeleistung_batterie(self, batterie, gesamte_entladbare_kapazitaet, entladebedarf):
        if (gesamte_entladbare_kapazitaet == 0):
            entladeleistung_batterie = 0
        else:
            entladeleistung_batterie = round(
                (entladebedarf * self.ermittle_verbleibende_entladekapazitaet(batterie)) / gesamte_entladbare_kapazitaet, 0)
        if (batterie["typ"] is not None and batterie["typ"] == "volterion"):
            if (batterie["soc"] is not None
                    and batterie["soc"] <=  batterie["min_soc_generell"]) :
                if self.__ist_direkt_steuerbar(batterie):
                    ladeleistung_batterie = 0 # führt zum Disablen neuer RFBs, Wechselladen unnötig
                else:
                    ladeleistung_batterie = - batterie["min_ladeleistung"]
                return ladeleistung_batterie
            if batterie["soc"] is not None and batterie["temperatur"] is not None and batterie["temperatur_reduzierung"] is not None:
                if (batterie["temperatur"] >= batterie["temperatur_reduzierung"]
                        or (self.check_batterie_temp_reduzierung_aktiv(batterie)
                            and  batterie["temperatur"] >= batterie["temperatur_reduzierung_bis"])):
                    if batterie["soc"] <= batterie["soc_ladung_reduzieren"]:
                        ladeleistung_batterie = - batterie["min_ladeleistung"]
                        batterie["temperatur_reduzierung_aktiv"] = True
                        return ladeleistung_batterie
                    else:
                        batterie["temperatur_reduzierung_aktiv"] = False
                else:
                    batterie["temperatur_reduzierung_aktiv"] = False
        if (batterie["typ"] is not None and batterie["typ"] == "e3dc") and (batterie["soc"] is not None and batterie["soc"] <=  batterie["min_soc_generell"]) :
            ladeleistung_batterie = 0 # E3dc schaltet sich nicht ab, wenn Batterie leer ist, Volterion schon
            return ladeleistung_batterie

        entladeleistung_batterie = min(batterie["max_entladeleistung"], entladeleistung_batterie)
        entladeleistung_batterie = round(max(batterie["min_entladeleistung"], entladeleistung_batterie), 0)
        entladeleistung_batterie = int(round(entladeleistung_batterie / 100)) * 100 # auf volle 500er runden
        return entladeleistung_batterie

    def entlade_batterien(self, entladebedarf):
        entladebedarf_verbleibend = entladebedarf
        priorisierende_batterien = self.ermittle_priorisierende_batterien(Arbeitsmodus.ENTLADEN)
        batterien_aktuelle_prio = priorisierende_batterien["priorisiert"]
        gesamte_entladbare_kapazitaet = 0
        for batterie in batterien_aktuelle_prio:
            if self.is_nicht_zu_entladen(batterie):
                continue
            entladbare_kapazitaet = self.ermittle_verbleibende_entladekapazitaet(batterie)
            if entladbare_kapazitaet > 0:  # Stelle sicher, dass die Kapazitaet nicht negativ ist
                gesamte_entladbare_kapazitaet += entladbare_kapazitaet
        if gesamte_entladbare_kapazitaet >= 0:
            for batterie in batterien_aktuelle_prio:
                if self.is_nicht_zu_entladen(batterie):
                    if self.__ist_direkt_steuerbar(batterie):
                        entladeleistung_batterie = 0 # Neuere EFB-Software benötigt keine Lade/Entladewechsel mehr
                    else:
                        entladeleistung_batterie = - batterie["min_ladeleistung"]  # Batterie muss erst aufgeladen werden
                else:
                    entladeleistung_batterie = self.ermittle_entladeleistung_batterie(batterie, gesamte_entladbare_kapazitaet,
                                                                            entladebedarf)
                self.setze_ladeleistung_in_ha_modbus(batterie, entladeleistung_batterie * -1)
                entladebedarf_verbleibend -= entladeleistung_batterie

        entladebedarf_verbleibend -= self.get_batterie_ac_ladeleistung()
        if entladebedarf_verbleibend >= 100:
            self.log("Nicht entladbare Leistung: " + str(entladebedarf_verbleibend))
        self.__calc_and_save_aktive_batterien_soc()
        self.__calc_and_save_aktive_batterien_unterversorgung_in_w(BATTERIE_UNTERVERSORGUNG_SOC_THRESHOLD)
        self.save_ladeleistung_ueberschuss(max(0, entladebedarf_verbleibend) * -1)


    def ermittle_kapazitaet_batterie(self, batterie):
        return batterie["aktuelle_restkapazitaet"]

    def terminate(self):
        self.terminate_peak_shaving()
        self.terminate_ac_laden()
        self.terminate_batterien()
        self.log("Batterie-Management beendet")

    @staticmethod
    def to_float_safe(text, default=0.0):
        if isinstance(text, (int, float)):
            return float(text)
        if isinstance(text, str) and text.replace('.', '', 1).isdigit():
            return float(text)
        try:
            return float(text)  # handles scientific notation, etc.
        except ValueError:
            return default

    @staticmethod
    def to_datetime_safe(text, default):
        try:
            return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%f%z")
        except ValueError:
            return default

    def ermittle_priorisierende_batterien(self, modus:Arbeitsmodus):
        priovorgaenger_batterien = []
        priorisierende_batterien = []
        restliche_batterien = []
        if modus == Arbeitsmodus.LADEN:
            self.__ermittle_priorisierende_batterien_laden(priovorgaenger_batterien, priorisierende_batterien, restliche_batterien)
        else:
            self.__ermittle_priorisierende_batterien_entladen(priorisierende_batterien)
        return { "priovorgaenger": priovorgaenger_batterien, "priorisiert": priorisierende_batterien, "restliche": restliche_batterien }

    def __ermittle_priorisierende_batterien_entladen(self, priorisierende_batterien):
        for batterie in sorted(self.batterien, key=lambda b: -b["prioritaet"]):
            if batterie["aktiv"] is not None and batterie["aktiv"] is True:
                if self.is_feierabend_nicht_erreicht():
                    priorisierende_batterien.append(batterie)
                else:
                    self.log(f"Batterie {batterie['name']} - Aktiv: {batterie['aktiv']}, SOC: {batterie['soc']}, hinzugefügt wegen Feierabendregelung: {TIME_ENDE_LADESTEUERUNG_SOC} > { time()}", level="INFO")
                    priorisierende_batterien.append(batterie)

    def __calc_and_save_aktive_batterien_soc(self):
        """
        Calculates and saves the state of charge (SoC) of active batteries in the system. The method iterates through
        a list of batteries, evaluates their activation status, and determines the aggregated state of charge for
        all active batteries. The calculated SoC is then saved as a system state.

        :param self: Refers to the object instance the method is called on.

        :return: None
        """
        kapazitaet_aktiver_batterien = 0
        fuellstand_aktiver_batterien = 0
        for batterie in self.batterien:
            if batterie["aktiv"] is not None and batterie["aktiv"] is True:
                if batterie["soc"]:
                    fuellstand_aktiver_batterien += round((batterie["soc"] / 100) * batterie["gesamtkapazitaet"], 1)
                    kapazitaet_aktiver_batterien += batterie["gesamtkapazitaet"]
        attributes = self.get_state(SENSOR_EMS_LADESTEUERUNG_AKTIVE_BATTERIEN_SOC, attribute="all").get(
            "attributes", {})
        if kapazitaet_aktiver_batterien > 0:
            soc_aktiver_batterien = round(fuellstand_aktiver_batterien / kapazitaet_aktiver_batterien * 100, 1)
            self.set_state(SENSOR_EMS_LADESTEUERUNG_AKTIVE_BATTERIEN_SOC,
                           state=soc_aktiver_batterien,
                           attributes=attributes)
        else:
            self.set_state(SENSOR_EMS_LADESTEUERUNG_AKTIVE_BATTERIEN_SOC,
                           state=None,
            )

    def __calc_and_save_aktive_batterien_unterversorgung_in_w(self, min_soc:float) -> float:
        """
        Calculates and saves the missing capacity to the minimum state of charge (SoC) of active batteries in the system.
        The method iterates through
        a list of batteries, evaluates their activation status, and determines the missing capacity for reaching the minimum state of charge for
        all active batteries. The calculated SoC is then saved as a system state.

        :param self: Refers to the object instance the method is called on.
               min_soc: The minimum state of charge (SoC) of active batteries.

        :return: None
        """
        unterversorgung_aktiver_batterien = 0
        minimum_soc = float(self._get_sensor_value(SENSOR_NIEDRIGPREISLADEN_SOC_THRESHOLD, min_soc))
        self.log(f"Minimum state of charge: {minimum_soc}", level="DEBUG")
        for batterie in self.batterien:
            if batterie["aktiv"] is not None and batterie["aktiv"] is True:
                if batterie["soc"]:
                    unterversorgung_aktiver_batterien += round(((minimum_soc - batterie["soc"]) / 100) * batterie["gesamtkapazitaet"], 0)
                    self.log(f'unterversorgung_aktiver_batterien: round((({minimum_soc} - {batterie["soc"]}) / 100) * {batterie["gesamtkapazitaet"]}, 0)', level="DEBUG")
        attributes = self.get_state(SENSOR_EMS_LADESTEUERUNG_AKTIVE_BATTERIEN_UNTERVERSORGUNG, attribute="all").get(
            "attributes", {})
        self.log(f"unterversorgung_aktiver_batterien: {unterversorgung_aktiver_batterien} bei Ziel-SOC:{minimum_soc}", level="INFO")
        self.set_state(SENSOR_EMS_LADESTEUERUNG_AKTIVE_BATTERIEN_UNTERVERSORGUNG,
                       state=unterversorgung_aktiver_batterien,
                       attributes=attributes)


    def __ermittle_priorisierende_batterien_laden(self, priovorgaenger_batterien, priorisierende_batterien,
                                                  restliche_batterien) -> None:
        prio_erreicht = True
        aktuelle_prio = 0
        for batterie in sorted(self.batterien, key=lambda b: b["prioritaet"]):
            if batterie["aktiv"] and batterie["aktiv"] is True:
                today = date.today()
                if aktuelle_prio == 0:
                    aktuelle_prio = batterie["prioritaet"]
                if (batterie["min_soc_erstladung"]) and (batterie["soc"] >= batterie["min_soc_erstladung"]):
                    batterie["last_day_erstladung"] =  today
                if self.has_erstladung_soc_erreicht(batterie):
                    if prio_erreicht and batterie["prioritaet"] > aktuelle_prio:
                        priovorgaenger_batterien.extend(priorisierende_batterien)
                        priorisierende_batterien.clear()
                        aktuelle_prio = batterie["prioritaet"]
                else:
                    prio_erreicht = False
                if batterie["prioritaet"] == aktuelle_prio:
                    priorisierende_batterien.append(batterie)
                else:
                    restliche_batterien.append(batterie)

    def __ist_direkt_steuerbar(self, batterie: dict) -> bool:
        """
        Prüft, ob die übergebene Batterie direkt für das Laden und Entladen steuerbar ist
        Args:
            batterie: Das Dictionary-Objekt mit den Batterie-Parametern.
        Returns:
            True, wenn der Schlüssel 'modbus_hub' existiert und
            dessen Wert mit "volterion" beginnt, sonst False.
        """
        modbus_hub = batterie.get("modbus_hub")
        return isinstance(modbus_hub, str) and modbus_hub.startswith("volterion")

    def __ist_list_of_dicts(variable):
        """
        Prüft, ob die Variable eine Liste ist UND
        alle Elemente darin Dictionaries sind.
        """
        if not isinstance(variable, list):
            return False

        return all(isinstance(item, dict) for item in variable)

    def has_erstladung_soc_erreicht(self, batterie):
        return batterie["last_day_erstladung"] and batterie["last_day_erstladung"] == date.today()

    def is_feierabend_nicht_erreicht(self):
        return TIME_ENDE_LADESTEUERUNG_SOC >= time()

    def send_notification_status_inactive(self, batterie, message):
        now = datetime.now()
        if (now - batterie["last_notification_inactive"]).total_seconds() > 3600:  # nur 1 Nachricht pro Stunde
            self.call_service("script/notify_admins_with_label_admin_notification",
                              message_id=f"battery_{batterie['name']}_inaktiv",
                              title=f"Batterie {batterie['name']} wurde inaktiv gesetzt",
                              message=message)
            batterie["last_notification_inactive"] = now
            self.log("sendNotificationStatusInactive: Benachrichtigung gesendet.")

    def justiere_ladeleistung(self, batterie, ladeleistung_batterie):
        """
        Adjusts the charging or discharging power of a battery based on the current
        operating conditions. The method ensures that the power adjustments adhere
        to defined operational limits for both charging and discharging scenarios.
        It validates the charging power, and if the power is negative, handles it as
        discharging. If positive, it adjusts charging power conditionally.

        :param batterie: Dictionary containing battery operational parameters.
        :param ladeleistung_batterie: Current power level for charging or discharging.
            The value can be positive for charging or negative for discharging.
        :return: Returns the adjusted charging or discharging power as an integer.
        """
        if ladeleistung_batterie < 0:

            ladeleistung_batterie = self.justiere_ladeleistung_entladung(batterie, ladeleistung_batterie)
        elif ladeleistung_batterie > 0 and batterie["justiere_ladeleistung"]:
            ladeleistung_batterie = self.justiere_ladeleistung_ladung(batterie, ladeleistung_batterie)
        return ladeleistung_batterie

    def justiere_ladeleistung_ladung(self, batterie, ladeleistung_batterie):
        """
        Adjusts the charging power of a battery based on its configuration parameters.

        This method modifies the current charging power of a battery based on its
        attributes. If the maximum adjustment limit (`justiere_ladeleistung_max`)
        is defined, the adjustment is applied only if the absolute value of the new
        charging power does not exceed this limit. Otherwise, the charging power
        is adjusted unconditionally. The initial and adjusted charging power values
        are logged for further analysis or debugging.

        :param batterie: A dictionary containing the battery's configuration and state.
        :param ladeleistung_batterie: The current charging power of the battery.
        :return: The adjusted charging power of the battery.
        """
        ladeleistung_batterie_vorher = ladeleistung_batterie
        if batterie["justiere_ladeleistung_max"]:
            if abs(ladeleistung_batterie) <= batterie["justiere_ladeleistung_max"]:
                ladeleistung_batterie += batterie["justiere_ladeleistung"]
                self.log(
                    f"justiere_ladeleistung Batterie({batterie['name']}) von {ladeleistung_batterie_vorher}: {ladeleistung_batterie}", level="DEBUG")
        else:
            ladeleistung_batterie += batterie["justiere_ladeleistung"]
            self.log(
                f"justiere_ladeleistung Batterie({batterie['name']}) von {ladeleistung_batterie_vorher}: {ladeleistung_batterie}", level="DEBUG")
        return ladeleistung_batterie

    def justiere_ladeleistung_entladung(self, batterie, ladeleistung_batterie):
        """
        Adjusts the discharge power of a battery based on the provided adjustment parameters.

        This method modifies the current battery discharge power (`ladeleistung_batterie`)
        by an adjustment value (`justiere_entladeleistung`) if the `justiere_entladeleistung`
        flag of the battery parameter is set to True. Additionally, if a maximum adjustment
        value (`justiere_entladeleistung_max`) is provided, it ensures that the adjusted
        discharge power does not exceed the defined limit.

        The method logs the original and adjusted discharge power values, along with the
        battery name, for reference.

        :param batterie: Dictionary containing information about the battery. Required fields:
                         - `justiere_entladeleistung` (bool): Indicates whether discharge power
                           adjustment should be applied.
                         - `justiere_entladeleistung` (float): Adjustment value for the
                           discharge power.
                         - `justiere_entladeleistung_max` (float, optional): Optional maximum
                           limit for the discharge power adjustment.
                         - `name` (str): Name of the battery.
        :param ladeleistung_batterie: Current battery discharge power, which will be adjusted
                                      based on the provided parameters.
        :return: Updated discharge power after applying the adjustments, if applicable.
        :rtype: float
        """
        if batterie["justiere_entladeleistung"]:
            ladeleistung_batterie_vorher = ladeleistung_batterie
            if batterie["justiere_entladeleistung_max"]:
                if abs(ladeleistung_batterie) <= batterie["justiere_entladeleistung_max"]:
                    ladeleistung_batterie += batterie["justiere_entladeleistung"]
                    self.log(f"justiere_entladeleistung Batterie({batterie['name']}) von {ladeleistung_batterie_vorher}: {ladeleistung_batterie}", level="DEBUG")
            else:
                ladeleistung_batterie += batterie["justiere_entladeleistung"]
                self.log(
                    f"justiere_entladeleistung Batterie({batterie['name']}) von {ladeleistung_batterie_vorher}: {ladeleistung_batterie}", level="DEBUG")
        return ladeleistung_batterie

    def is_nicht_zu_entladen(self, batterie) -> bool:
        if batterie["min_soc_entladung"] and batterie["soc"] <= batterie["min_soc_entladung"]:
            if self.is_feierabend_nicht_erreicht() and not self.has_erstladung_soc_erreicht(batterie):
                return True
        return batterie["soc"] <= batterie["min_soc_generell"]

    def __ist_status_aktuell_standby(self, battery_dict):
        """
        Ruft die im Dictionary hinterlegte Lambda-Funktion 'ist_status_in_standby'
        mit dem Wert aus 'status' auf.

        Args:
            battery_dict (dict): Ein Dictionary, das 'status' (float) und
                                 'ist_status_in_standby' (callable) enthält.

        Returns:
            bool: Das Ergebnis der Lambda-Funktion.
        """
        pruef_funktion = battery_dict.get('ist_status_in_standby')
        if pruef_funktion is None: return False
        status_wert = battery_dict.get('status', 0.0)
        return pruef_funktion(status_wert)


# Peak-Shaving - Logik:
# Diese App steuert das Peak-Shaving für Batterien basierend auf einem
# Prognose-Sensor und einem Input-Boolean.
#

    def initialize_peak_shaving(self):
        """Initialisiert die App und ruft die Setup-Funktionen auf."""
        self.log("Peak Shaving Controller wird initialisiert.")
        self._setup_einspeise_peak_shaving()

        # Initialen Zustand beim Start prüfen
        if self.get_state(self.peak_shaving_switch) == 'on':
            self.log("Peak-Shaving ist beim Start aktiv. Pruefe Timer.")
            self.schedule_peak_shaving_events(None)

    def _setup_einspeise_peak_shaving(self):
        """Konfiguriert die App und setzt die Listener."""

        # --- Sensoren ---
        self.peak_shaving_switch = "input_boolean.ems_ladesteuerung_peak_shaving"
        self.forecast_sensor = "sensor.pv_forecast_sunshine_peaktime_in_s"

        # --- Interne Zustandsverwaltung ---
        self.timer_handles = {}

        # Listener auf den Hauptschalter für Peak-Shaving
        self.listen_state(self.on_peak_shaving_change, self.peak_shaving_switch)

    def on_peak_shaving_change(self, entity, attribute, old, new, kwargs):
        """Wird aufgerufen, wenn sich der Peak-Shaving-Schalter ändert."""
        self.log(f"Peak-Shaving-Schalter hat sich von '{old}' zu '{new}' geaendert.")

        if new == 'on':
            self.schedule_peak_shaving_events(None)
        else:  # new == 'off'
            self.cancel_all_peak_shaving_timers()
            self.clear_peak_shaving_limits(None)

    def schedule_peak_shaving_events(self, kwargs):
        """Liest den Prognose-Sensor und plant die zeitgesteuerten Aktionen."""
        self.log("Plane Peak-Shaving-Ereignisse...")

        # Zuerst alle alten Timer löschen, um Duplikate zu vermeiden
        self.cancel_all_peak_shaving_timers()

        forecast_state = self.get_state(self.forecast_sensor, attribute="all")
        if not forecast_state or "attributes" not in forecast_state:
            self.error(f"Prognose-Sensor '{self.forecast_sensor}' nicht gefunden.")
            return

        attrs = forecast_state["attributes"]
        last_updated_str = attrs.get("last_updated")
        peak_start_str = attrs.get("peak_window_start")
        peak_end_str = attrs.get("peak_window_end")

        if not all([last_updated_str, peak_start_str, peak_end_str]):
            self.error("Benötigte Attribute im Prognose-Sensor fehlen.")
            return

        # Lokale Zeitzone holen für alle Berechnungen
        local_tz = self.get_timezone()
        now = self.datetime(aware=True)  # Ist bereits in lokaler Zeitzone

        # Alle Zeitstempel in die lokale Zeitzone konvertieren für zuverlässige Vergleiche
        last_updated = datetime.fromisoformat(last_updated_str).astimezone(local_tz)
        peak_start_time = datetime.fromisoformat(peak_start_str).astimezone(local_tz)
        peak_end_time = datetime.fromisoformat(peak_end_str).astimezone(local_tz)

        if last_updated.date() != now.date():
            self.log("Prognosedaten sind nicht von heute. Es werden keine Aktionen geplant.",  level="WARNING")
            return

        # --- Planung für den Start des Peak-Shaving ---
        if now < peak_start_time:
            self.log(
                f"Peak-Shaving-Start ({peak_start_time.strftime('%H:%M')}) liegt in der Zukunft. Plane Timer und rufe Pre-Limits auf.")
            self.timer_handles['start'] = self.run_at(self.setze_peak_shaving_limits, peak_start_time)
            self.setze_pre_peak_shaving_limits(None)
        else:
            self.log(f"Peak-Shaving-Startzeit ({peak_start_time.strftime('%H:%M')}) ist bereits vorbei.")
            # Wenn wir uns bereits im Fenster befinden, die Limits sofort setzen
            if now < peak_end_time:
                self.setze_peak_shaving_limits(None)

        # --- Planung für das Ende des Peak-Shaving ---
        if now < peak_end_time:
            self.log(f"Peak-Shaving-Ende ({peak_end_time.strftime('%H:%M')}) liegt in der Zukunft. Plane Timer.")
            self.timer_handles['end'] = self.run_at(self.clear_peak_shaving_limits, peak_end_time)
        else:
            self.log(
                f"Peak-Shaving-Endzeit ({peak_end_time.strftime('%H:%M')}) ist bereits vorbei. Rufe Clear-Funktion sofort auf.")
            self.clear_peak_shaving_limits(None)

    def setze_pre_peak_shaving_limits(self, kwargs):
        """Setzt die Limits für die Phase VOR dem Peak-Shaving-Fenster."""
        self.log("Aktion: Setze Pre-Peak-Shaving-Limits.")
        for battery in self.batterien:
            if battery.get("aktiv"):
                battery["einspeise_peak_shaving_aktiv"] = True  # Setzt Peak-Shaving auf aktiv
                new_max_ladeleistung = max(battery["min_ladeleistung"], MIN_LADELEISTUNG_PRE_PEAKSHAVING)
                self._set_max_charge_power(battery, new_max_ladeleistung, "Pre-Phase")

    def setze_peak_shaving_limits(self, kwargs):
        """Setzt die Limits für die Phase WÄHREND des Peak-Shaving-Fensters."""
        self.log("Aktion: Setze Peak-Shaving-Limits (Peak-Fenster beginnt).")

        local_tz = self.get_timezone()
        forecast_state = self.get_state(self.forecast_sensor, attribute="all")
        if not forecast_state or "attributes" not in forecast_state:
            self.error("Prognose-Sensor für Berechnung nicht gefunden.")
            return

        attrs = forecast_state["attributes"]
        peak_start_str = attrs.get("peak_window_start")
        peak_end_str = attrs.get("peak_window_end")

        if not peak_start_str or not peak_end_str:
            self.error("Zeitfenster-Attribute im Prognose-Sensor fehlen.")
            return

        peak_start_time = datetime.fromisoformat(peak_start_str).astimezone(local_tz)
        peak_end_time = datetime.fromisoformat(peak_end_str).astimezone(local_tz)
        now = datetime.now(local_tz)

        effective_start = peak_start_time
        if peak_start_time < now:
            effective_start = now
            self.log(
                f"Peak-Shaving-Start ({peak_start_time.strftime('%H:%M')}) liegt in der Vergangenheit. "
                f"Verwende aktuelle Zeit ({now.strftime('%H:%M')}) fuer Dauerberechnung.")

        duration_hours = (peak_end_time - effective_start).total_seconds() / 3600

        if duration_hours <= 0:
            self.log("Dauer des Peak-Fensters ist null oder negativ. Keine Leistungsanpassung moeglich.",  level="WARNING")
            return

        for battery in self.batterien:
            if battery.get("aktiv"):
                battery["einspeise_peak_shaving_aktiv"] = True  # Setzt Peak-Shaving auf aktiv

                soc = self._get_sensor_value(battery.get("soc_sensor"))
                if soc is None:
                    self.log(f"  -> SoC fuer '{battery['name']}' nicht verfuegbar. Ueberspringe Berechnung.",  level="WARNING")
                    continue

                battery["soc"] = soc

                rest_soc = battery["max_soc_ladung"] - soc
                rest_kapazitaet_wh = (rest_soc / 100) * battery["gesamtkapazitaet"]

                power_w = rest_kapazitaet_wh / duration_hours
                rounded_power_w = round(power_w / 100) * 100

                new_max_ladeleistung = max(battery["min_ladeleistung"], rounded_power_w)
                new_max_ladeleistung = min(battery["max_ladeleistung_preset"], new_max_ladeleistung)
                self.log(
                    f"  -> '{battery['name']}': Restkapazitaet {rest_kapazitaet_wh:.0f}Wh, Dauer {duration_hours:.2f}h.")
                self._set_max_charge_power(battery, new_max_ladeleistung, "Peak-Phase")

    def clear_peak_shaving_limits(self, kwargs):
        """Setzt alle Limits zurück auf den Normalzustand."""
        self.log("Aktion: Setze alle Peak-Shaving-Limits zurueck.")
        for battery in self.batterien:
            if battery.get("aktiv"):
                battery["einspeise_peak_shaving_aktiv"] = False
                new_max_ladeleistung = battery["max_ladeleistung_preset"]  # Wie angefordert
                self._set_max_charge_power(battery, new_max_ladeleistung, "Clear-Phase")

    def _set_max_charge_power(self, battery, power, phase):
        """Hilfsfunktion zum Setzen der maximalen Ladeleistung einer Batterie."""
        battery["max_ladeleistung"] = power
        self.log(f"  -> '{battery['name']}': max_ladeleistung auf {power}W gesetzt ({phase}).")
        # Service-Call zum Steuern der Batterie-Hardware momentan nur für E3DC implementiert
        # z.B. self.call_service("modbus/write_register", ...)
        #   action: e3dc_rscp.set_power_limits
        #   data:
        #   device_id: 2ebd6a796df261e0a689179d39cc51f3
        #   max_charge: 12500
        e3dc = (battery['typ'] is not None) and (battery['typ'] == "e3dc")
        if e3dc:
            self.call_service('e3dc_rscp/set_power_limits', device_id=battery['externe_device_id'],
                              max_charge=power)
            self.log(f"call service: e3dc_rscp/set_power_limits, device_id={battery['externe_device_id']}, max_charge={power})")


    def _set_max_discharge_power(self, battery, power, phase):
        """Hilfsfunktion zum Setzen der maximalen Entladeleistung einer Batterie."""
        battery["max_entladeleistung"] = power
        self.log(f"  -> '{battery['name']}': max_entladeleistung auf {power}W gesetzt ({phase}).")
        # Service-Call zum Steuern der Batterie-Hardware momentan nur für E3DC implementiert
        # z.B. self.call_service("modbus/write_register", ...)
        #   action: e3dc_rscp.set_power_limits
        #   data:
        #   device_id: 2ebd6a796df261e0a689179d39cc51f3
        #   max_charge: 12500
        e3dc = (battery['typ'] is not None) and (battery['typ'] == "e3dc")
        if e3dc:
            self.call_service('e3dc_rscp/set_power_limits', device_id=battery['externe_device_id'],
                              max_discharge=power)
            self.log(f"call service: e3dc_rscp/set_power_limits, device_id={battery['externe_device_id']}, max_discharge={power})")

    def _set_ac_charge_power_mode(self, battery, ac_charge_power_limit:int):
        """
        Sets the AC charge power mode for a given battery with a specific charge power
        limit.

        This method utilizes the 'e3dc_rscp/set_power_mode' service to configure the
        power mode and power value for the specified battery device. It also logs the
        details of the service call for debugging or auditing purposes.

        :param battery: A dictionary containing battery details. Expected to have the
            key 'externe_device_id' which is used to identify the device for the
            service call.
        :type battery: dict
        :param ac_charge_power_limit: The AC charge power limit to be applied. This
            value will be passed to the service under the parameter 'power_value'.
        :type ac_charge_power_limit: int
        :return: None
        """
        self.call_service(SERVICE_E3DC_RSCP_SET_POWER_MODE, device_id=battery['externe_device_id'], power_mode="4", power_value=ac_charge_power_limit)
        self.log(
            f"call service: e3dc_rscp/set_power_mode, device_id={battery['externe_device_id']},power_mode='4', power_value={ac_charge_power_limit})")

    def cancel_all_peak_shaving_timers(self):
        """Löscht alle aktiven Timer."""
        self.log("Loesche alle aktiven Peak-Shaving-Timer.")
        for handle in self.timer_handles.values():
            if handle:
                self.cancel_timer(handle)
        self.timer_handles = {}

    def _get_sensor_value(self, sensor_id, default=None):
        """Hilfsfunktion zum sicheren Auslesen und Konvertieren von Sensorwerten."""
        if not sensor_id:
            return default
        state = self.get_state(sensor_id)
        if state in [None, 'unavailable', 'unknown']:
            return default
        try:
            return float(state)
        except (ValueError, TypeError):
            return default

    def terminate_peak_shaving(self):
        """Wird aufgerufen, wenn AppDaemon die App beendet."""
        self.log("Peak Shaving Controller wird beendet.")
        self.cancel_all_peak_shaving_timers()


# Funktionen zur Berechnung der AC-Ladung beim Niedrigpreisladen
# /config/appdaemon/apps/charge_power_calculator.py


    # --- Konstanten ---
    AC_LADEN_TRIGGER_SENSOR_LADEZEIT = "sensor.ems_batterie_ladezeit"
    AC_LADEN_TRIGGER_SENSOR_NETZBEZUG_UEBERLAST = "sensor.ems_netzbezug_begrenzung"
    AC_LADEN_TRIGGER_BOOLEAN_NIEDRIGPREIS = "input_boolean.ems_ladesteuerung_niedrigpreis"
    AC_LADEN_OUTPUT_SENSOR = "sensor.ems_batterie_ac_laden"
    AC_LADEN_FRIENDLY_NAME = "Batterie AC Ladeleistung"

    def initialize_ac_laden(self):
        """Initialisiert die App und die Listener."""
        self.listen_state(self.on_charge_state_change, self.AC_LADEN_TRIGGER_SENSOR_LADEZEIT)
        self.listen_state(self.on_charge_state_change, self.AC_LADEN_TRIGGER_SENSOR_LADEZEIT, attribute = "lade_differenz")
        self.listen_state(self.on_grid_limit_change, self.AC_LADEN_TRIGGER_SENSOR_NETZBEZUG_UEBERLAST)
        self.listen_state(self.on_low_price_change, self.AC_LADEN_TRIGGER_BOOLEAN_NIEDRIGPREIS)
        self.log("AC_Ladesteuerung fuer 3 Trigger initialisiert.")

    def terminate_ac_laden(self):
        """Wird aufgerufen, wenn AppDaemon die App beendet."""
        self.log(f"Beende AC-Ladesteuerung. Setze '{self.AC_LADEN_OUTPUT_SENSOR}' auf 0.")
        self.clear_batterie_ac_ladeleistung(status_text="App beendet")

    def get_batterie_ac_ladeleistung(self):
        """
        Gibt die aktuelle AC-Ladeleistung zurueck.
        Im Fehlerfall oder wenn der Sensor nicht verfuegbar ist, wird 0 zurueckgegeben.
        """
        try:
            current_state = self.get_state(self.AC_LADEN_OUTPUT_SENSOR)
            return int(float(current_state))
        except (ValueError, TypeError):
            self.log(f"Konnte keinen gueltigen Wert von '{self.AC_LADEN_OUTPUT_SENSOR}' lesen, gebe 0 zurueck.", level="DEBUG")
            return 0

    @ad.app_lock
    def _update_output_sensor(self, state, attributes):
        """
        Synchronisierte zentrale Methode, um den Zustand des Ausgangssensors zu schreiben.
        """
        attribs = self.get_state(self.AC_LADEN_OUTPUT_SENSOR, attribute="all").get("attributes", {})
        attribs.update({"status": attributes.get("status")})
        self.set_state(self.AC_LADEN_OUTPUT_SENSOR, state=state)
        self.log(f"Sensor {self.AC_LADEN_OUTPUT_SENSOR} hat jetzt den Wert {state}")

    def clear_batterie_ac_ladeleistung(self, status_text="AC-Laden nicht aktiv"):
        """
        Bereitet die Attribute zum Zuruecksetzen vor und ruft die synchronisierte
        Update-Methode auf.
        """
        self.clear_discharging_stop_on_e3dc_after_ac_charge_mode()

        attributes = {
            "status": status_text
        }
        self._update_output_sensor(state=0, attributes=attributes)

    def on_charge_state_change(self, entity, attribute, old, new, kwargs):
        """Wird aufgerufen, wenn sich der Zustand des Ladezeit-Sensors aendert."""
        self.log(f"Ladezeit-Sensor '{entity}' hat sich geaendert {attribute} von {old} zu {new}. Aktualisiere AC-Ladeleistung.")
        if attribute == "state":
            if new == 'true':
                self.calculate_and_set_power()
                if self.get_batterie_ac_ladeleistung() > 0:
                    self.handle_e3dc_in_ac_charge_mode()
            else:
                self.clear_batterie_ac_ladeleistung()
        elif attribute == "lade_differenz":
            self.calculate_and_set_power()
            if self.get_batterie_ac_ladeleistung() > 0:
                self.handle_e3dc_in_ac_charge_mode()

    def on_grid_limit_change(self, entity, attribute, old, new, kwargs):
        """Wird aufgerufen, wenn sich der Netzbezug-Sensor aendert."""
        try:
            limit_value = float(new)
        except (ValueError, TypeError):
            return

        if limit_value > 0:
            self.log(f"Netzbezugsgrenze von {limit_value}W erkannt. Stoppe AC-Ladung.")
            self.clear_batterie_ac_ladeleistung(status_text=f"Netzbezugsgrenze aktiv ({limit_value}W)")

    def on_low_price_change(self, entity, attribute, old, new, kwargs):
        """Wird aufgerufen, wenn sich der Niedrigpreis-Schalter aendert."""
        if new == 'off':
            self.log("Niedrigpreis-Ladung wurde deaktiviert. Stoppe AC-Ladung.")
            self.clear_batterie_ac_ladeleistung(status_text="Niedrigpreis-Ladung beendet")

    def calculate_and_set_power(self):
        """Ermittelt Attribute und berechnet die Leistung."""
        try:
            # Sicherheitsabfragen
            grid_limit_state = self.get_state(self.AC_LADEN_TRIGGER_SENSOR_NETZBEZUG_UEBERLAST)
            if float(grid_limit_state) > 0:
                self.clear_batterie_ac_ladeleistung(status_text=f"Netzbezugsgrenze aktiv ({grid_limit_state}W)")
                return

            low_price_state = self.get_state(self.AC_LADEN_TRIGGER_BOOLEAN_NIEDRIGPREIS)
            if low_price_state == 'off':
                self.clear_batterie_ac_ladeleistung(status_text="Niedrigpreis-Ladung beendet")
                return

            # Berechnung
            attributes = self.get_state(self.AC_LADEN_TRIGGER_SENSOR_LADEZEIT, attribute="all")["attributes"]
            rest_stunden = float(attributes.get("rest_stunden_ladung"))
            lade_differenz_kw = float(attributes.get("lade_differenz"))

            if rest_stunden > 0:
                ladeleistung_w = (lade_differenz_kw * 1000) / rest_stunden
                final_power = int(round(ladeleistung_w, 0))
                if final_power <= 4000: # nur wenn midestens 4kW geladen werden soll wird ladestrom gesetzt
                    self.clear_batterie_ac_ladeleistung(status_text="Ladeleistung unter 3KW, beende Niedrigpreisladen")
                    final_power = 0

                active_attributes = {
                    "friendly_name": self.AC_LADEN_FRIENDLY_NAME,
                    "device_class": "power",
                    "unit_of_measurement": "W",
                    "status": "Laden aktiv",
                    "berechnete_restzeit_h": rest_stunden,
                    "berechnete_lademenge_kwh": lade_differenz_kw
                }
                self._update_output_sensor(state=final_power, attributes=active_attributes)
            else:
                self.clear_batterie_ac_ladeleistung(status_text="Fehler bei Berechnung (Zeit ist 0)")

        except (TypeError, ValueError, KeyError) as e:
            self.clear_batterie_ac_ladeleistung(status_text=f"Fehler: {e}")

    def handle_e3dc_in_ac_charge_mode(self):
        for battery in self.batterien:
            if battery.get("aktiv"):
                e3dc = (battery['typ'] is not None) and (battery['typ'] == "e3dc")
                if e3dc:
                    self.log(f"AC-Charge aktiviert, Entladen der E3DC {battery["name"]} wird so lange begrenzt "
                             f"und ggf. AC-Laden aktiviert!")
                    if battery["soc"] < 75:
                        self.log(
                            f"AC-Charge wird aktiviert, E3DC {battery["name"]}, soc {battery["soc"]} wird auf power_mode 4 mit ladeleistung {battery["max_ladeleistung"]} W gesetzt!")
                        self.call_service(SERVICE_E3DC_RSCP_SET_POWER_MODE, device_id=battery['externe_device_id'],
                                          power_mode="4", power_value=10000)
                    else:
                        self.log(
                            f"AC-Charge wird aktiviert, E3DC {battery["name"]}, soc {battery["soc"]} wird wegen hohem SOC auf power_mode 0 gesetzt!")
                        self._set_max_discharge_power(battery, battery["min_entladeleistung"], "AC-Charge")
                        self.call_service(SERVICE_E3DC_RSCP_SET_POWER_MODE, device_id=battery['externe_device_id'],
                                          power_mode="0")

    def clear_discharging_stop_on_e3dc_after_ac_charge_mode(self):
        for battery in self.batterien:
            if battery.get("aktiv"):
                e3dc = (battery['typ'] is not None) and (battery['typ'] == "e3dc")
                if e3dc:
                    self.log(f"AC-Charge wird deaktiviert, Entladen der E3DC {battery["name"]} wird wieder zugelassen!")
                    self._set_max_discharge_power(battery, battery["max_entladeleistung_preset"], "AC-Charge-Stop")
                    self.log(f"AC-Charge wird deaktiviert, E3DC {battery["name"]} wird wieder auf power_mode 0 gesetzt!")
                    self.call_service(SERVICE_E3DC_RSCP_SET_POWER_MODE, device_id=battery['externe_device_id'], power_mode="0")
## Init und Terminate-Logik für Batterien hinzufügen

    def initialize_batterien(self):
        self.log("Batterien werden initialisiert...")

        # Dictionary zum Speichern der Listener-Handles
        self.listener_handles = {}
        self.init_batteries()

    def terminate_batterien(self):
        """
        Wird von AppDaemon aufgerufen, wenn die App beendet wird.
        """
        self.log("Batterien werden in manuellen Modus versetzt...")
        # Deregistriert Listener UND ruft terminate_battery auf
        self.terminate_batteries()

    def __ist_list_of_dicts(self, variable):
        if not isinstance(variable, list):
            return False
        return all(isinstance(item, dict) for item in variable)

    # --- INITIALISIERUNGS-LOGIK ---

    def init_batteries(self):
        """
        Initialisiert alle Batterien und registriert die Prio-Listener.
        """
        self.log(f"Starte Initialisierung fuer {len(self.batterien)} Batterien.")

        for battery_config in self.batterien:
            # 1. Modbus-Register initialisieren
            self.init_battery(battery_config)

            # 2. Listener fuer diese Batterie registrieren
            name = battery_config.get("name", UNBEKANNTE_BATTERIE)
            prio_sensor = battery_config.get("prioritaet_sensor")

            if prio_sensor and isinstance(prio_sensor, str):
                self.log(f"{name}: Registriere Listener fuer Prioritaetssensor: {prio_sensor}")
                handle = self.listen_state(
                    self.battery_prio_changed,
                    prio_sensor,
                    battery_name=name,
                    battery_config=battery_config  # Uebergibt die Konfig an den Callback
                )
                # Speichere das Handle
                self.listener_handles[name] = handle
            else:
                self.log(
                    f"{name}: Kein 'prioritaet_sensor' (string) in der Konfiguration gefunden. Kein Listener registriert.",
                    level="INFO")
    def reinit_batteries(self, kwargs):
        """
        Initialisiert alle Batterien nach einer gewissen Zeit erneut, da bei einem Reload der Batterien, oft Probleme mit der Synchronisierung auftreten.
        """
        self.log(f"Starte Initialisierung fuer {len(self.batterien)} Batterien.")

        for battery_config in self.batterien:
            # 1. Modbus-Register reinitialisieren
            self.init_battery(battery_config)


    def init_battery(self, battery):
        """
        Initialisiert nur die Modbus-Register fuer eine Batterie.
        (Registriert KEINE Listener mehr)
        """
        name = battery.get("name", UNBEKANNTE_BATTERIE)

        init_register = battery.get("initialisierungs_register")

        if not self.__ist_list_of_dicts(init_register):
            self.log(
                f"Batterie '{name}' hat ein ungueltiges 'initialisierungs_register'. Es muss eine Liste von Dictionaries sein. Wird uebersprungen.",
                level="WARNING")
            return

        self.log(f"-> Initialisiere Batterie-Register: {name}")

        for element in init_register:
            try:
                self.__modbus_register_configuration(battery, element, force_write=True)
            except Exception as e:
                self.log(f"Ignoriere Fehler bei der Modbus-Konfiguration fuer {name} mit Element {element}: {e}", level="ERROR")

        # Der Listener-Code wurde von hier nach init_batteries verschoben

    # --- TERMINIERUNGS-LOGIK ---

    def terminate_batteries(self):
        """
        Deregistriert alle Listener und terminiert alle Batterien.
        """
        self.log("Deregistriere alle Listener...")

        # 1. Alle Listener deregistrieren
        for name, handle in self.listener_handles.items():
            try:
                self.cancel_listen_state(handle)
                self.log(f"Listener fuer {name} deregistriert.", level="INFO")
            except Exception as e:
                self.log(f"Konnte Listener-Handle {handle} fuer {name} nicht deregistrieren: {e}", level="WARNING")

        self.listener_handles.clear()

        # 2. Modbus-Terminierungsregister ausfuehren
        self.log(f"Starte Terminierung fuer {len(self.batterien)} Batterien.")
        for battery_config in self.batterien:
            self.terminate_battery(battery_config)

    def terminate_battery(self, battery):
        """
        Fuehrt die Modbus-Terminierungssequenz fuer eine Batterie aus.
        """
        name = battery.get("name", UNBEKANNTE_BATTERIE)

        term_register = battery.get("terminierungs_register")

        if not self.__ist_list_of_dicts(term_register):
            self.log(
                f"Batterie '{name}' hat ein ungueltiges 'terminierungs_register'. Es muss eine Liste von Dictionaries sein. Ueberspringe Terminierung.",
                level="WARNING")
            return

        self.log(f"-> Terminiere Batterie: {name}")

        for element in term_register:
            try:
                self.__modbus_register_configuration(battery, element)
            except Exception as e:
                self.log(f"Fehler bei der Modbus-Terminierung fuer {name} mit Element {element}: {e}", level="ERROR")

    # --- MODBUS-LOGIK (Gemeinsam genutzt) ---

    def __modbus_register_configuration(self, battery, element, force_write=False):
        name = battery.get('name', 'Unbekannt')
        hub = battery.get('modbus_hub')
        slave_id = battery.get('modbus_slave')

        if not hub or not isinstance(slave_id, int):
            self.log(
                f"Modbus-Hub/Slave fuer {name} nicht korrekt konfiguriert in {battery}. Ueberspringe Element {element}.",
                level="WARNING")
            return

        value_sensor = element.get('value_sensor')
        set_register = element.get('set_register')
        set_value = element.get('set_value')

        current_value = None
        read_result = None
        read_success = False

        if isinstance(value_sensor, str):
            self.log(f"{name}: Lese Sensor {value_sensor} für Modbus-Register ", level="DEBUG")

            read_result = self.__modbus_sensor_read(value_sensor)

            current_value_float = self.to_float_safe(read_result)

            if current_value_float is not None:
                current_value = int(current_value_float)
                read_success = True
                self.log(f"{name}: Register {value_sensor} hat Wert {current_value}", level="DEBUG")
            else:
                self.log(
                    f"{name}: Konnte Modbus-Wert '{read_result}' von Sensor {value_sensor} nicht in Zahl umwandeln.",
                    level="WARNING")

        if isinstance(set_register, int) and isinstance(set_value, int):

            perform_write = False

            if read_success:
                if current_value != set_value or force_write:
                    self.log(
                        f"{name}: Wert {current_value} (Sensor {value_sensor}) != Soll-Wert {set_value}. Schreibe Register {set_register}...")
                    perform_write = True
                else:
                    self.log(
                        f"{name}: Wert {current_value} (Sensor {value_sensor}) == Soll-Wert {set_value}. Kein Schreibvorgang.",
                        level="INFO")
            else:
                self.log(f"{name}: Schreibe {set_value} auf Register {set_register}...",
                         level="INFO")
                perform_write = True

            if perform_write:
                self.__modbus_register_write(hub, slave_id, set_register, set_value)
                if read_result is not None:
                    read_result_after = read_result
                    anz_versuche = 0
                    while (read_result == read_result_after):
                        read_result_after=self.__modbus_sensor_read(value_sensor)
                        anz_versuche += 1
                        if anz_versuche > 8:
                            break
                    self.log(f"After Write: actual register ({anz_versuche}) is {read_result_after}, before {read_result}", level="INFO")

        elif 'set_register' in element or 'set_value' in element:
            self.log(
                f"{name}: 'set_register' oder 'set_value' ist ungueltig in {element}. Ueberspringe Schreibvorgang.",
                level="INFO")

    def __modbus_sensor_read(self, sensor_name):
        self.log(f"Lese von Sensor {sensor_name}", level="INFO")
        return self.get_state(sensor_name)

    def __modbus_register_write(self, hub, slave, register, value):
        self.log(
            f"modbus/write_register, hub={hub}, slave={slave}, address={register}, value={value}")
        self.call_service("modbus/write_register", hub=hub, slave=slave,
                          address=register,
                          value=value)

    # --- CALLBACK-LOGIK ---

    def battery_prio_changed(self, entity, attribute, old, new, kwargs):
        battery_name = kwargs.get("battery_name", entity)
        battery_config = kwargs.get("battery_config", {})

        self.log(
            f"Prioritaetssensor-Update fuer {battery_name} (Sensor: {entity}): status ist jetzt {new} (vorher: {old})")

        if not battery_config:
            self.log(f"Fehler in battery_prio_changed fuer {battery_name}: 'battery_config' fehlt in kwargs.",
                     level="ERROR")
            return

        old_val = self.to_float_safe(old)
        new_val = self.to_float_safe(new)

        if old_val is None or new_val is None:
            self.log(
                f"Werte fuer {battery_name} konnten nicht in Zahlen umgewandelt werden (alt: {old}, neu: {new}). Aktion wird uebersprungen.",
                level="INFO")
            return

        # WICHTIG: Wir rufen hier nur noch init_battery oder terminate_battery auf.
        # Diese Methoden aendern NICHTS mehr an den Listenern,
        # was eine Endlosschleife oder doppelte Listener verhindert.

        if old_val == 0 and new_val != 0:
            self.log(f"Prioritaet fuer {battery_name} hat sich von 0 auf {new_val} geaendert. Starte 'init_battery'...")
            self.init_battery(battery_config)

        elif old_val != 0 and new_val == 0:
            self.log(
                f"Prioritaet fuer {battery_name} hat sich von {old_val} auf 0 geaendert. Starte 'terminate_battery'...")
            self.terminate_battery(battery_config)

