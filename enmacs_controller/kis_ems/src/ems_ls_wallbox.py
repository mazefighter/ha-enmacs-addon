from email.policy import default

import math
import uuid
import zoneinfo
from datetime import datetime, timedelta, time
from enum import Enum

import ha_wrapper as hass
import ha_wrapper as ad

REDUCABLE_W_PER_PHASE = 230.0
"""
Kleinste reduzierbare oder inkrementierbare Leistung in W pro Phase, 230 W in Europa (1A Ladestrom-Regulierung)
"""

ADJUST_DYN_CURRENT_EVERY_N_RUNS = 3
"""
Constant defining how often dynamic adjustments to current are made in a system or
process, measured in the number of runs. This value, when reached, triggers the
reassessment and potential modification of the current settings to adapt to
changing conditions or requirements.

Attributes:
    ADJUST_DYN_CURRENT_EVERY_N_RUNS (int): Number of runs after which the dynamic
    current adjustments are made.
"""

SENSOR_EMS_LS_WALLBOXES_AVERAGE_CHARGING_STATS = "sensor.ems_ls_wallboxes_average_charging_stats"
SENSOR_WALLBOX_DAILY_CHARGING_STATS = "sensor.ems_ls_wallboxes_daily_charging_stats"

MINIMAL_CHARGE_CURRENT = 6

EASEE_SERVICE_ACTION_COMMAND = "easee/action_command"
EASEE_CMD_SET_CHARGER_DYNAMIC_LIMIT = "easee/set_charger_dynamic_limit"

SENSOR_WORKING_DAY_TOMORROW = "sensor.working_day_tomorrow"

MAX_CURRENT_HYBRID_BOXES = 16
CURRENT_INCREASE_HYBRID_BOXES = 4

# Steuert die Aktivierung der Temperaturüberwachung aus den HA-Einstellungen
SENSOR_EMS_LADESTEUERUNG_WB_TEMPERATUR_AKTIV = "input_boolean.ems_ladesteuerung_wb_temperatur"
SENSOR_EMS_LADESTEUERUNG_WB_TEMPERATUR_STATUS = "sensor.ems_ladesteuerung_wb_temperatur_status"
# Steuert die Aktivierung der Ladesteuerung aus den HA-Einstellungen
SENSOR_EMS_LADESTEUERUNG_WB_LADESTEUERUNG_AKTIV = "input_number.ems_ladesteuerung_wb_prio"
SENSOR_EMS_LADESTEUERUNG_WB_LADESTEUERUNG_DYNAMIC_CURRENT = "input_boolean.ems_ladesteuerung_wb_dynamisch_regeln"
SENSOR_EMS_LADESTEUERUNG_WB_LADESTEUERUNG_CHARGE_LIMIT = "input_number.ems_ladesteuerung_wb_dynamisch_charge_limit"
SENSOR_EMS_LADESTEUERUNG_WB_AUTO_CHARGE_TIME_HELPER = "input_datetime.ems_ladesteuerung_wb_start_charging_ab"
SENSOR_EMS_LADESTEUERUNG_WB_AUTO_CHARGE_TIME_DEFAULT = "08:00:00"
SENSOR_EMS_LADESTEUERUNG_WB_MAIN_STATUS = "sensor.ems_ladesteuerung_wb_main_status"

# Abregelungslogik bei Ueberlast des Bezugs
SENSOR_NETZBEZUG_BEGRENZUNG = "sensor.ems_netzbezug_begrenzung"
SENSOR_AUSFUEHRUNG = "sensor.ems_netzbezug_begrenzung_ausfuehrung"
SENSOR_WALLBOX_NETZBEGRENZUNG_AKTIV = "sensor.ems_netzbezug_begrenzung_wallbox_aktiv"

# dynamisches Regeln der WB-Ladung mit Daten der Batterieladesteuerung
SENSOR_EMS_LADESTEUERUNG_BATTERIE_UEBERSCHUSS_IN_W = "sensor.ems_ladesteuerung_batterie_ueberschuss_geglaettet_in_w"
SENSOR_EMS_LADESTEUERUNG_AKTIVE_BATTERIEN_UNTERVERSORGUNG = "sensor.ems_ladesteuerung_aktive_batterien_unterversorgung_in_w"


TIMEZONE_BERLIN = zoneinfo.ZoneInfo("Europe/Berlin")

FULLY_CHARGED_REASON_NO_CURRENT="waiting_in_fully"

# Gibt die maximale Wallbock-Temperatur an, nach der runtergeregelt wird
MAX_WB_TEMPERATURE_IN_C = 68
# Gibt die maximale Wallbock-Temperatur an, nach der auf die Hälfte des Ladestroms runtergeregelt wird
MAX_WB_TEMPERATURE_TO_REDUCE_TO_HALF_IN_C = 72
# Default log level for this app
# Can be overridden in apps.yaml
DEFAULT_LOG_LEVEL = "INFO"

# Default maximum charging duration in minutes (can be overridden per wb_config)
DEFAULT_MAX_CHARGE_DURATION_MINUTES = 120

# --- Zur Berchnung ein/dreiphasig ---
VOLTAGE = 230  # Angenommene Spannung in Volt fuer eine einzelne Phase
TOLERANCE = 0.20  # 20% Toleranz fuer Leistungsfaktor (cos phi) und Messungenauigkeiten
MIN_CURRENT_THRESHOLD = 1.0  # Mindeststromstaerke in Ampere, um als "ladend" zu gelten
# Toleranzfaktor für schwache Laderegelung (2% Abweichung vom Sollwert erlaubt)
CREEPY_CHARGER_TOLERANCE_FACTOR = 0.98


class EmsWallboxSteuerung(hass.Hass):
    """
    Defines the EmsWallboxSteuerung class, WbPrio, WbStatus, and WbReasonNoCurrent
    enums, as well as a configuration map for managing Wallbox devices in Home
    Assistant. Provides functionality for configuring and managing multiple
    Wallbox devices, encapsulating related attributes such as device ID, status,
    and operational settings.

    Enums:
        WbPrio:
            Represents different priority levels for Wallbox operations.
        WbStatus:
            Represents various statuses of a Wallbox.
        WbReasonNoCurrent:
            Specifies reasons for the absence of current in a Wallbox state.

    Dictionary:
        wb_config_map:
            Stores configuration for multiple Wallbox devices, including names,
            device IDs, prefixes, status IDs, and additional attributes.
    """
    class WbPrio(Enum):
        KEINE = 0
        EINMAL = 1
        IMMER = 2

    class WbStatus(Enum):
        not_relevant = 0
        awaiting_authorization = 1
        awaiting_start = 2
        charging = 3
        completed = 4
        de_authorizing = 5
        disconnected = 6
        erratic_ev = 7
        error = 8
        offline = 9

    class WbReasonNoCurrent(Enum):
        not_relevant = 0
        waiting_in_fully = 1
        waiting_in_queue = 2
        limited = 3
        max_current_too_low = 4


    # Map der Wallbox-friendly names zu Homeassistant-Device-Ids, und weiterer Attribute. Generiert durch tools/gen_wallbox_dashboard.py
    wb_config_map = {
        "1GF": {
            "name": "1GF",
            "device_id": "09b1f02d9bec0d9ea0ff19fce62e4aca",
            "wb_entity_prefix": "sensor.geschaeftsfuehrung",
            "wb_status_id": "sensor.geschaeftsfuehrung_status",
            "ems_entity_prefix": "ems_ladesteuerung_wb_1gf",
            "is_schaltkreis_master": True,
            "is_hybrid_wb": False,
        }
        , "2EL": {
            "name": "2EL",
            "device_id": "7604f141c0d3bbe970427352978340a4",
            "wb_entity_prefix": "sensor.e_auto_seite_linke_ladebox",
            "wb_status_id": "sensor.e_auto_seite_linke_ladebox_status",
            "ems_entity_prefix": "ems_ladesteuerung_wb_2el",
            "is_schaltkreis_master": False,
            "is_hybrid_wb": False,
        }
        , "3EM": {
            "name": "3EM",
            "device_id": "0201062355c568ee57638b95d1bacd9c",
            "wb_entity_prefix": "sensor.e_auto_seite_mittlere_ladebox",
            "wb_status_id": "sensor.e_auto_seite_mittlere_ladebox_status",
            "ems_entity_prefix": "ems_ladesteuerung_wb_3em",
            "is_schaltkreis_master": True,
            "is_hybrid_wb": False,
        }
        , "4ER": {
            "name": "4ER",
            "device_id": "d3ea612fa1964fb0bec97eafb354813f",
            "wb_entity_prefix": "sensor.e_auto_seite_rechte_ladebox",
            "wb_status_id": "sensor.e_auto_seite_rechte_ladebox_status",
            "ems_entity_prefix": "ems_ladesteuerung_wb_4er",
            "is_schaltkreis_master": True,
            "is_hybrid_wb": False,
        }
        , "5SL": {
            "name": "5SL",
            "device_id": "2b90ea1ac6e322eb999d58c147792133",
            "wb_entity_prefix": "sensor.sudseite_linke_box",
            "wb_status_id": "sensor.sudseite_linke_box_status",
            "ems_entity_prefix": "ems_ladesteuerung_wb_5sl",
            "is_schaltkreis_master": False,
            "is_hybrid_wb": False,
        }
        , "6SR": {
            "name": "6SR",
            "device_id": "3603bf1ee7f62eed5949f152771389c9",
            "wb_entity_prefix": "sensor.sudseite_rechte_box",
            "wb_status_id": "sensor.sudseite_rechte_box_status",
            "ems_entity_prefix": "ems_ladesteuerung_wb_6sr",
            "is_schaltkreis_master": True,
            "is_hybrid_wb": False,
        }
        , "7P1L": {
            "name": "7P1L",
            "device_id": "9c9928649da26676c7f2cb596d581910",
            "wb_entity_prefix": "sensor.wb_parkplatz_1_links",
            "wb_status_id": "sensor.wb_parkplatz_1_links_status",
            "ems_entity_prefix": "ems_ladesteuerung_wb_7p1l",
            "is_schaltkreis_master": True,
            "is_hybrid_wb": False,
        }
        , "8P1R": {
            "name": "8P1R",
            "device_id": "1c4c98469ec1967179139463dceff38b",
            "wb_entity_prefix": "sensor.wb_parkplatz_1_rechts",
            "wb_status_id": "sensor.wb_parkplatz_1_rechts_status",
            "ems_entity_prefix": "ems_ladesteuerung_wb_8p1r",
            "is_schaltkreis_master": True,
            "is_hybrid_wb": False,
        }
        , "9P2L": {
            "name": "9P2L",
            "device_id": "94df8ad07836e6a778f20bd712826b3d",
            "wb_entity_prefix": "sensor.wb_parkplatz_2_links",
            "wb_status_id": "sensor.wb_parkplatz_2_links_status",
            "ems_entity_prefix": "ems_ladesteuerung_wb_9p2l",
            "is_schaltkreis_master": False,
            "is_hybrid_wb": False,
        }
        , "10P2R": {
            "name": "10P2R",
            "device_id": "1b0714fef064c265f429d0c214bf9688",
            "wb_entity_prefix": "sensor.wb_parkplatz_2_rechts",
            "wb_status_id": "sensor.wb_parkplatz_2_rechts_status",
            "ems_entity_prefix": "ems_ladesteuerung_wb_10p2r",
            "is_schaltkreis_master": False,
            "is_hybrid_wb": False,
        }
        , "11H1L": {
            "name": "11H1L",
            "device_id": "a142d780714b076cb47ec9852a03d979",
            "wb_entity_prefix": "sensor.hybrid_seite_ladebox_1_von_links",
            "wb_status_id": "sensor.hybrid_seite_ladebox_1_von_links_status",
            "ems_entity_prefix": "ems_ladesteuerung_wb_11h1l",
            "is_schaltkreis_master": False,
            "is_hybrid_wb": True,
        }
        , "12H2L": {
            "name": "12H2L",
            "device_id": "43c84d735335051a6cbc46d210a1a090",
            "wb_entity_prefix": "sensor.hybrid_seite_ladebox_2_von_links",
            "wb_status_id": "sensor.hybrid_seite_ladebox_2_von_links_status",
            "ems_entity_prefix": "ems_ladesteuerung_wb_12h2l",
            "is_schaltkreis_master": False,
            "is_hybrid_wb": True,
        }
        , "13H3L": {
            "name": "13H3L",
            "device_id": "47e67d87effb0794e59b150ed48206fd",
            "wb_entity_prefix": "sensor.hybrid_seite_ladebox_3_von_links",
            "wb_status_id": "sensor.hybrid_seite_ladebox_3_von_links_status",
            "ems_entity_prefix": "ems_ladesteuerung_wb_13h3l",
            "is_schaltkreis_master": True,
            "is_hybrid_wb": True,
        }
        , "14H4L": {
            "name": "14H4L",
            "device_id": "4ffc5931efd1e1ab3acfc2c646311e20",
            "wb_entity_prefix": "sensor.hybrid_seite_ladebox_4_von_links",
            "wb_status_id": "sensor.hybrid_seite_ladebox_4_von_links_status",
            "ems_entity_prefix": "ems_ladesteuerung_wb_14h4l",
            "is_schaltkreis_master": False,
            "is_hybrid_wb": True,
        }
        , "15H5L": {
            "name": "15H5L",
            "device_id": "48a16eaf4d646bbc98aacf4eeaa6a1d5",
            "wb_entity_prefix": "sensor.hybrid_seite_ladebox_5_von_links",
            "wb_status_id": "sensor.hybrid_seite_ladebox_5_von_links_status",
            "ems_entity_prefix": "ems_ladesteuerung_wb_15h5l",
            "is_schaltkreis_master": False,
            "is_hybrid_wb": True,
        }
        , "16H6L": {
            "name": "16H6L",
            "device_id": "40805db7cc0e786b4656d787be7fcc61",
            "wb_entity_prefix": "sensor.hybrid_seite_ladebox_6_von_links",
            "wb_status_id": "sensor.hybrid_seite_ladebox_6_von_links_status",
            "ems_entity_prefix": "ems_ladesteuerung_wb_16h6l",
            "is_schaltkreis_master": True,
            "is_hybrid_wb": True,
        }
        , "17B1L": {
            "name": "17B1L",
            "device_id": "1286d937af7c4185743ace32da7b1f9d",
            "wb_entity_prefix": "sensor.wb_besucher_1_links",
            "wb_status_id": "sensor.wb_besucher_1_links_status",
            "ems_entity_prefix": "ems_ladesteuerung_wb_17b1l",
            "is_schaltkreis_master": True,
            "is_hybrid_wb": False,
        }
        , "18B1R": {
            "name": "18B1R",
            "device_id": "50b0e0531873b9e82dc5970303f26e19",
            "wb_entity_prefix": "sensor.wb_besucher_1_rechts",
            "wb_status_id": "sensor.wb_besucher_1_rechts_status",
            "ems_entity_prefix": "ems_ladesteuerung_wb_18b1r",
            "is_schaltkreis_master": False,
            "is_hybrid_wb": False,
        }
        , "19B2L": {
            "name": "19B2L",
            "device_id": "339e5582dd155b6e922564187d32c3e0",
            "wb_entity_prefix": "sensor.wb_besucher_2_links",
            "wb_status_id": "sensor.wb_besucher_2_links_status",
            "ems_entity_prefix": "ems_ladesteuerung_wb_19b2l",
            "is_schaltkreis_master": False,
            "is_hybrid_wb": False,
        }
        , "20B2R": {
            "name": "20B2R",
            "device_id": "01d14639733e738430b720f16d44d8d9",
            "wb_entity_prefix": "sensor.wb_besucher_2_rechts",
            "wb_status_id": "sensor.wb_besucher_2_rechts_status",
            "ems_entity_prefix": "ems_ladesteuerung_wb_20b2r",
            "is_schaltkreis_master": False,
            "is_hybrid_wb": False,
        }
    }

    wb_sensors = {
        "status": {
            "prefix": None,
            "suffix": "_status",
        },
        "reason_no_current": {
            "prefix": None,
            "suffix": "_grund_fur_keinen_strom",
        },
        "power": {
            "prefix": None,
            "suffix": "_leistung",
        },
        "limit": {
            "prefix": None,
            "suffix": "_ausgabelimit",
        },
        "dyn_limit": {
            "prefix": None,
            "suffix": "_dynamisches_ladelimit",
        },
        "temperature": {
            "prefix": None,
            "suffix": "_interne_temperatur",
        },
        "current": {
            "prefix": None,
            "suffix": "_strom",
        },
    }

    ems_helper = {
        "max_charge_duration_minutes": {
            "prefix": "input_number",
            "suffix": "_max_charge_time",
        },
        "max_charge_current": {
            "prefix": "input_number",
            "suffix": "_charge_override",
        },
        "priority": {
            "prefix": "input_select",
            "suffix": "_prio",
        },
    }

    @staticmethod
    def to_float_safe(text, default=0.0):
        if isinstance(text, (int, float)):
            return float(text)
        if isinstance(text, str) and text.replace('.', '', 1).isdigit():
            return float(text)
        try:
            if text is None:
                return default
            else:
                return float(text)  # handles scientific notation, etc.
        except ValueError:
            return default

    def initialize(self):
        self.log("EmsWallboxSteuerung gestartet.")
        # Global log level for the app
        log_level = self.args.get("log_level", DEFAULT_LOG_LEVEL)
        self.set_log_level(log_level)
        self.log(f"Log level set to {log_level}", level="DEBUG")
        self.awaiting_disconnect_flags = {}  # Keyed by status_sensor
        self.charge_duration_timer_handles = {}  # Keyed by status_sensor
        self.charging_start_times = {}  # Keyed by status_sensor
        self.wallboxes = {}
        self.last_notification_check_wallbox_temperature_map = {}
        self.last_notification_check_wallbox_temperature_extreme_map = {}
        # Dictionary zum Speichern des letzten Log-Zeitpunkts pro Wallbox
        self.last_creepy_log_time = {}
        self.run_every(self.check_and_update_wallboxes, datetime.now(), 60)  # Alle 60 Sekunden (1 Minute) ausführen
        if self.get_state(SENSOR_EMS_LADESTEUERUNG_WB_TEMPERATUR_STATUS) is None:
            self.set_state(SENSOR_EMS_LADESTEUERUNG_WB_TEMPERATUR_STATUS, state="",
                           attributes={"device_id": str(uuid.uuid4()), "friendly_name": "Status der WB-Temperaturabregelung"})
            self.get_ad_api().run_in(self.check_and_update_wallboxes, 1)
        self.initialize_wallboxes_main()
        self.initialize_netzbezugsbegrenzung()
        self.run_daily(self.calculate_charging_history, "23:30:00")
        self.run_daily(self.calculate_charging_history, "17:00:00")
        if self.get_now().hour >=17:
            self.get_ad_api().run_in(self.calculate_charging_history, 30)
        self.run_count = 0



    def initialize_wallboxes_main(self):
        """Initialize the app for multiple wallboxes."""

        if not self.wb_config_map:
            self.log("Error: No 'wallboxes' map found in configuration.", level="ERROR")
            return

        self.charge_duration_timer_handles.clear()
        self.awaiting_disconnect_flags.clear()
        self.charging_start_times.clear()



        # "device_id": "d3ea612fa1964fb0bec97eafb354813f",
        # "wb_entity_prefix": "sensor.e_auto_seite_rechte_ladebox",
        # "wb_ems_entity_prefix": "ems_ladesteuerung_wb_1gf",
        # "is_schaltkreis_master": True,
        # "is_hybrid_wb": False,

        self.wallboxes = {
            value_dict['wb_status_id']: value_dict
            for value_dict in self.wb_config_map.values()
        }
        self.update_wallboxes()

        self.run_daily(self.cleanup_at_night, "23:30:00")  # Schedule for 23:30

        for status_sensor, wb_config in self.wallboxes.items():
            wb_name = wb_config["name"]

            self.charging_start_times[status_sensor] = None
            self.charge_duration_timer_handles[status_sensor] = None
            self.awaiting_disconnect_flags[status_sensor] = False

            # Listen for state changes on the status sensor for this wallbox
            self.listen_state(self.wallbox_status_changed, status_sensor)
            max_duration = self.get_max_charge_wenn_prio(wb_config)
            if max_duration and max_duration > 0.0:
                self.log(
                    f"[{wb_name}] Max Duration Monitoring initialized. Sensor: {status_sensor}, Max Duration: {max_duration} min.")

            # Check initial state
            initial_state = wb_config['status']
            self.log(f"[{wb_name}] Initial state is '{initial_state}'", level="DEBUG")
            if initial_state == self.WbStatus.charging:
                self.log(f"[{wb_name}] Wallbox is already charging on startup. Check if monitoring necessary.", level="INFO")
                # Simulate a transition to loading to start timers correctly
                # Pass entity to ensure context is correct
                self.wallbox_status_changed(status_sensor, "state", self.WbStatus.not_relevant.name, self.WbStatus.charging.name, {})

    @ad.app_lock
    def update_wallboxes(self):
        for status_sensor, wb_config in self.wallboxes.items():
            self._update_wallbox(wb_config)

    @ad.app_lock
    def _update_wallbox(self, wb_config):
        wb_name = wb_config["name"]
        wb_entity_prefix = wb_config.get("wb_entity_prefix")
        if not wb_entity_prefix:
            self.log(f"Error for {wb_name}: 'wb_entity_prefix' not specified. Skipping this wallbox.", )
            return
        ems_entity_prefix = wb_config.get("ems_entity_prefix")
        if not ems_entity_prefix:
            self.log(f"Error for {wb_name}: 'ems_entity_prefix' not specified. Skipping this wallbox.", )
            return
        device_id = wb_config.get("device_id")
        if not device_id:
            self.log(f"Error for {wb_name}: 'device_id' not specified. Skipping this wallbox.", )
            return

        # Update der Attribute aus Home-Assistant
        for attrib_name, attrib_config in self.wb_sensors.items():
            ha_sensor = wb_entity_prefix + attrib_config["suffix"]
            attrib_value = self.get_state(ha_sensor)
            if attrib_value:
                if attrib_name not in ['status', 'reason_no_current'] and isinstance(attrib_value, str):
                    attrib_value = self.to_float_safe(attrib_value)
                elif attrib_name == 'reason_no_current':
                    attrib_value = self.map_to_wb_reason_no_current(str(attrib_value))
                elif attrib_name == 'status':
                    attrib_value = self.map_to_wb_status(str(attrib_value))
                wb_config.update({attrib_name: attrib_value})
            else:
                self.log(f"WB {wb_name}: Kein Wert fuer Sensor: {ha_sensor} in ha gefunden.", )
                wb_config.update({attrib_name: None})

        for attrib_name, attrib_config in self.ems_helper.items():
            ha_sensor = self.get_ems_sensor_id(attrib_config, wb_config)
            attrib_value = self.get_state(ha_sensor)
            if attrib_value:
                if attrib_name != 'priority' and isinstance(attrib_value, str):
                    attrib_value = self.to_float_safe(attrib_value)
                else:
                    attrib_value = self.map_to_wb_prio(str(attrib_value))
                wb_config.update({attrib_name: attrib_value})
            else:
                self.log(f"WB {wb_name}: Kein Wert fuer Sensor: {ha_sensor} in ha gefunden.", )
                wb_config.update({attrib_name: None})

    def get_ems_sensor_id(self, attrib_config, wb_config):
        ems_entity_prefix = wb_config.get("ems_entity_prefix")
        return attrib_config["prefix"] + "." + ems_entity_prefix + attrib_config["suffix"]

    def get_max_charge_wenn_prio(self, wb_config):
        hat_prio = self.check_hat_prio(wb_config)
        max_duration = wb_config.get("max_charge_duration_minutes")
        return max_duration if hat_prio else 0.0

    def check_hat_prio(self, wb_config):
        prio = wb_config.get("priority")
        hat_prio = prio and prio != self.WbPrio.KEINE
        return hat_prio

    def wallbox_status_changed(self, entity, attribute, old_state, new_state, kwargs):
        """Callback for when a wallbox status sensor changes state."""
        status_sensor_id = entity  # 'entity' is the status_sensor that changed
        wb_config = self.wallboxes.get(status_sensor_id)

        if not wb_config:
            self.log(f"Received status change for unconfigured sensor: {status_sensor_id}. Ignoring.",
                     level="WARNING")
            return

        wb_name = wb_config["name"]
        self.log(f"[{wb_name}] Status changed: old='{old_state}', new='{new_state}'", level="DEBUG")

        # Transition TO 'charging' state
        new_state = self.map_to_wb_status(new_state)
        old_state = self.map_to_wb_status(old_state)
        if new_state == self.WbStatus.charging and old_state != self.WbStatus.charging:
            self.log(f"[{wb_name}] Started charging (previous state: '{old_state}').")

            if self.awaiting_disconnect_flags.get(status_sensor_id, False):
                self.log(
                    f"[{wb_name}] Started loading again without disconnecting after a previous timeout stop. Stopping charging immediately.",
                    level="WARNING")
                self.stop_charging_now(status_sensor_id, "Re-load after timeout without disconnect")

            self.charging_start_times[status_sensor_id] = datetime.now()

            if self.charge_duration_timer_handles.get(status_sensor_id):
                self.cancel_timer(self.charge_duration_timer_handles[status_sensor_id])
                self.log(f"[{wb_name}] Cancelled existing charge duration timer.", level="DEBUG")

            # Pass status_sensor_id to the callback via kwargs
            max_charge_duration = self.get_max_charge_wenn_prio(wb_config)
            if max_charge_duration > 0.0:
                timer_kwargs = {"status_sensor_id": status_sensor_id}
                self.charge_duration_timer_handles[status_sensor_id] = self.run_every(
                    self.check_charge_duration_callback,
                    datetime.now() + timedelta(seconds=5),
                    60,  # Interval in seconds
                    **timer_kwargs  # Pass kwargs to the callback
                )
                self.log(f"[{wb_name}] Started charge duration check timer.")

            # Ladestrom einstellen
            self._update_dynamic_wallbox_current(wb_config)

        # Transition FROM 'charging' state
        elif old_state == self.WbStatus.charging and new_state != self.WbStatus.charging:
            self.log(f"[{wb_name}] Stopped loading. New state: '{new_state}'.")
            if (new_state == self.WbStatus.disconnected or new_state == self.WbStatus.completed
                or (new_state == self.WbStatus.awaiting_start and wb_config[
                        "reason_no_current"] == self.WbReasonNoCurrent.waiting_in_fully)
            ) and self.charge_duration_timer_handles.get(status_sensor_id):
                self.cancel_timer(self.charge_duration_timer_handles[status_sensor_id])
                self.charge_duration_timer_handles[status_sensor_id] = None
                self.log(f"[{wb_name}] Cancelled charge duration check timer.")
            self.charging_start_times[status_sensor_id] = None

            if new_state == "disconnected":
                if self.awaiting_disconnect_flags.get(status_sensor_id, False):
                    self.log(
                        f"[{wb_name}] Is now 'disconnected'. Resetting 'awaiting_disconnect_after_timeout_stop' flag.",
                        level="INFO")
                self.awaiting_disconnect_flags[status_sensor_id] = False

        if new_state == "disconnected":  # General case for disconnect
            if self.awaiting_disconnect_flags.get(status_sensor_id, False):
                self.log(
                    f"[{wb_name}] Became '{new_state}'. Clearing 'awaiting_disconnect_after_timeout_stop' flag.",
                    level="INFO")
            self.awaiting_disconnect_flags[status_sensor_id] = False

    def check_charge_duration_callback(self, kwargs):
        """Periodically called to check charging duration for a specific wallbox."""
        status_sensor_id = kwargs.get("status_sensor_id")
        if not status_sensor_id:
            self.log("Error: check_charge_duration_callback called without status_sensor_id in kwargs.",
                     level="ERROR")
            return

        wb_config = self.wallboxes.get(status_sensor_id)
        if not wb_config:
            self.log(f"Error: check_charge_duration_callback for unknown sensor {status_sensor_id}.", level="ERROR")
            # Attempt to cancel timer if handle exists, to prevent repeated errors
            if self.charge_duration_timer_handles.get(status_sensor_id):
                self.cancel_timer(self.charge_duration_timer_handles[status_sensor_id])
                self.charge_duration_timer_handles[status_sensor_id] = None
            return

        wb_name = wb_config["name"]
        current_wallbox_state = self.get_state(status_sensor_id)

        if current_wallbox_state != self.WbStatus.charging.name:
            self.log(f"[{wb_name}] Charge duration check: No longer 'charging'. Stopping timer.", level="DEBUG")
            if self.charge_duration_timer_handles.get(status_sensor_id):
                self.cancel_timer(self.charge_duration_timer_handles[status_sensor_id])
                self.charge_duration_timer_handles[status_sensor_id] = None
            return

        if self.charging_start_times.get(status_sensor_id):
            duration_seconds = (datetime.now() - self.charging_start_times[status_sensor_id]).total_seconds()
            duration_minutes = duration_seconds / 60
            self.log(f"[{wb_name}] Charge duration check: Loading for {duration_minutes:.1f} minutes.",
                     level="DEBUG")

            max_duration = self.get_max_charge_wenn_prio(wb_config)
            if duration_minutes > max_duration > 0.0:
                self.log(
                    f"[{wb_name}] Max charge duration of {max_duration} min exceeded (actual: {duration_minutes:.1f} min). Stopping.",
                    level="WARNING")
                self.stop_charging_now(status_sensor_id, f"Timeout after {duration_minutes:.1f} minutes")

                self.awaiting_disconnect_flags[status_sensor_id] = True

                if self.charge_duration_timer_handles.get(status_sensor_id):
                    self.cancel_timer(self.charge_duration_timer_handles[status_sensor_id])
                    self.charge_duration_timer_handles[status_sensor_id] = None
                    self.log(f"[{wb_name}] Cancelled timer after sending stop command due to timeout.",
                             level="DEBUG")
        else:
            self.log(
                f"[{wb_name}] Charge duration check: 'loading_start_time' not set, but wallbox is 'charging'. Unexpected. Timer will be cancelled.",
                level="WARNING")
            if self.charge_duration_timer_handles.get(status_sensor_id):
                self.cancel_timer(self.charge_duration_timer_handles[status_sensor_id])
                self.charge_duration_timer_handles[status_sensor_id] = None

    def stop_charging_now(self, status_sensor_id, reason=""):
        """Calls the service to stop charging for a specific wallbox."""
        wb_config = self.wallboxes.get(status_sensor_id)
        if not wb_config:
            self.log(f"Error: stop_charging_now called for unknown sensor {status_sensor_id}.", level="ERROR")
            return

        wb_name = wb_config["name"]

        self.stop_charging_wb(wb_config, reason)
        self.log(f"[{wb_name}] Attempting to stop charging. Reason: {reason}", level="INFO")

    def terminate(self):
        """Clean up when AppDaemon stops this app."""
        self.log("Easee Multi Charge Monitor terminating...")
        if self.charge_duration_timer_handles:
            for status_sensor_id, timer_handle in self.charge_duration_timer_handles.items():
                wb_name = self.wallboxes.get(status_sensor_id, {}).get("name", status_sensor_id)
                if timer_handle:
                    self.cancel_timer(timer_handle)
                    self.log(f"[{wb_name}] Terminated, charge duration timer cancelled.")
        self.log("Easee Multi Charge Monitor terminated.")

    def check_and_update_wallboxes(self, kwargs):
        """
        Check and update the state of wallboxes by performing specific operations.

        This method integrates a sequence of operations required to update wallboxes,
        manage their states, and ensure their operational conditions, such as checking
        temperatures and dynamically updating current levels. It makes use of helper
        methods to achieve these tasks and processes any additional parameters
        provided through ``kwargs``.

        :param kwargs: Additional keyword arguments providing context or configuration
            for the operations performed on the wallboxes.
        :return: None
        """
        self.run_count += 1
        self.update_wallboxes()
        self.start_waiting_wallboxes()
        self.check_wallbox_temperatures(kwargs)
        self.update_dynamic_wallboxes_current()

    def check_wallbox_temperatures(self, kwargs):
        """
        Checks the internal temperatures of wallboxes and adjusts their charging current dynamically
        if the temperature exceeds a predefined maximum threshold. Sends notifications and logs actions
        for monitored wallboxes.

        :param kwargs: Arbitrary keyword arguments.
        :type kwargs: dict
        :return: None
        :rtype: None
        """
        temp_steuerung_wb_aktiv = self.get_state(SENSOR_EMS_LADESTEUERUNG_WB_TEMPERATUR_AKTIV)
        if temp_steuerung_wb_aktiv and (temp_steuerung_wb_aktiv == "on"):
            for entity_id, wb_config in self.wallboxes.items():
                if wb_config.get("status") == self.WbStatus.charging:

                    temperature = wb_config["temperature"]
                    max_charging_current = wb_config["limit"]
                    wallbox_name = wb_config["name"]
                    device_id = wb_config["device_id"]

                    self.log(f"check_wallbox_temperatures: Wallbox {wallbox_name} ({device_id}): Temperatur: {temperature}, Maximaler Ladestrom: {max_charging_current} (A)", level="DEBUG")
                    if temperature is not None and max_charging_current is not None and device_id is not None:
                        if int(temperature) > MAX_WB_TEMPERATURE_IN_C and wallbox_name not in self.last_notification_check_wallbox_temperature_map.keys():
                            new_max_charging_current = int(round(float(max_charging_current))) - 2
                            self.call_service(EASEE_CMD_SET_CHARGER_DYNAMIC_LIMIT, device_id=device_id, current=new_max_charging_current)
                            self.send_notification_status_temperature_wallbox(wallbox_name= wallbox_name, temperature=temperature,
                                                                              message=f"Wallbox {wallbox_name} wird abgeregelt, Temperatur ({temperature}), neues dynamisches Limit ({new_max_charging_current})")
                            self.log(f"Wallbox {wallbox_name} abgeregelt: Temperatur {temperature}, Neues Limit: {new_max_charging_current}")
                        elif (int(temperature) > MAX_WB_TEMPERATURE_TO_REDUCE_TO_HALF_IN_C
                              and wallbox_name not in self.last_notification_check_wallbox_temperature_extreme_map.keys()):
                            new_max_charging_current = int(round(float(max_charging_current)) +2) // 2
                            self.call_service(EASEE_CMD_SET_CHARGER_DYNAMIC_LIMIT, device_id=device_id, current=new_max_charging_current)
                            self.send_notification_status_temperature_wallbox(wallbox_name= wallbox_name, temperature=temperature,
                                                                              message=f"Wallbox {wallbox_name} wird weiter abgeregelt, Temperatur ({temperature}), neues dynamisches Limit ({new_max_charging_current})")
                            self.log(f"Wallbox {wallbox_name} weiter abgeregelt: Temperatur {temperature}, Neues Limit: {new_max_charging_current}")

                        else:
                            self.log(f"Wallbox {wallbox_name}: Temperatur {temperature} (OK oder bereits korrigiert)", level="DEBUG")
                    else:
                        self.log(f"Wallbox {wallbox_name}: Temperatur oder max_charging_current oder device_id nicht verfuegbar.", level="WARNING")

            wb_abgeregelt = ','.join(self.last_notification_check_wallbox_temperature_map.keys())
            if wb_abgeregelt and len(wb_abgeregelt) > 0:
                self.log(f"check_wallbox_temperatures: wegen Temperatur abgeregelte Wallboxen: {wb_abgeregelt}")
                wb_weiter_abgeregelt = ','.join(self.last_notification_check_wallbox_temperature_extreme_map.keys())
                if wb_weiter_abgeregelt and len(wb_weiter_abgeregelt) > 0:
                    self.log(f"check_wallbox_temperatures: wegen extremer Temperatur auf die Hälfte abgeregelte Wallboxen: {wb_weiter_abgeregelt}")

            attributes = self.get_state(SENSOR_EMS_LADESTEUERUNG_WB_TEMPERATUR_STATUS, attribute="all").get("attributes", {})
            attributes.update({"data": self.last_notification_check_wallbox_temperature_map})
            attributes.update({"extreme_data": self.last_notification_check_wallbox_temperature_extreme_map})
            self.set_state(SENSOR_EMS_LADESTEUERUNG_WB_TEMPERATUR_STATUS, state=wb_abgeregelt, attributes=attributes)

    @ad.app_lock
    def update_dynamic_wallboxes_current(self):
        """
        Updates the dynamic current limits for connected wallboxes. This function iterates through the
        available wallboxes, applies rules for priority and hybrid/non-hybrid configurations, and updates
        charge current limits dynamically based on the current conditions, such as manual settings and
        temperature control. The updated limits are sent to the wallbox systems via predefined service calls.

        :param self: The instance of the class containing this method.

        :raises TypeError: If an operation encounters incompatible types.
        """
        self.adjust_dynamic_wb_current_from_battery_load()
        for entity_state_id, wb_config in self.wallboxes.items():
            self._update_dynamic_wallbox_current(wb_config)

    @ad.app_lock
    def _update_dynamic_wallbox_current(self, wb_config):
        ist_ladesteuerung_wb_aktiv, new_current, new_current_hybrid = self.calculate_new_dynamic_current()
        self._update_wallbox(wb_config)
        if (wb_config.get("status") == self.WbStatus.charging
                or (wb_config.get("status") == self.WbStatus.awaiting_start
                    and wb_config.get("reason_no_current") == self.WbReasonNoCurrent.max_current_too_low)):

            current_limit = wb_config["limit"]
            wallbox_name = wb_config["name"]
            device_id = wb_config["device_id"]
            akt_leistung = wb_config["power"]
            dyn_limit = wb_config["dyn_limit"]
            hat_prio = self.check_hat_prio(wb_config)
            self.is_creepy_ev_charger(wb_config)

            if wallbox_name in self.last_notification_check_wallbox_temperature_map.keys():
                self.log(
                    f"update_dynamic_wallbox_current: WB {wallbox_name} wird von Temperatursteuerung begrenzt, ignoriere Wallbox fuer das Setzen der Limits.")
                return

            if hat_prio:
                max_charge_current = wb_config["max_charge_current"]
                if (dyn_limit and max_charge_current):
                    max_charge_current = max_charge_current + self._get_dyn_ladebegrenzung_aus_abregelung()
                    if dyn_limit != max_charge_current:
                        if device_id is not None:
                            self._set_charger_dyn_limit_pause_resume(device_id, dyn_limit, max_charge_current,
                                                                     wallbox_name)
                            self.log(
                                f"Prio Wallbox {wallbox_name} neu geregelt: Neues Limit: {int(max_charge_current)}")
                        else:
                            self.log(f"Wallbox {wallbox_name}: device_id nicht verfuegbar.", level="WARNING")
            else:
                if ist_ladesteuerung_wb_aktiv and new_current:
                    if device_id is not None:
                        new_max_charging_current = new_current_hybrid if self.is_charging_single_phase(wb_config) else new_current
                        if dyn_limit != new_max_charging_current:
                            self._set_charger_dyn_limit_pause_resume(device_id, dyn_limit, new_max_charging_current, wallbox_name)
                            self.log(
                                f"set_dynamic_wallbox_current: Wallbox {wallbox_name} ({device_id}): Leistung: {akt_leistung}, Ausgabelimit-aktuell: {current_limit} (A) neu geregelt: Neues Limit: {new_max_charging_current}",
                                level="INFO")
                    else:
                        self.log(f"Wallbox {wallbox_name}: device_id nicht verfuegbar.", level="WARNING")

    def pause_charging(self, device_id, wallbox_name):
        self.call_service(EASEE_SERVICE_ACTION_COMMAND, device_id=device_id,
                          action_command='pause')
        self.log(
            f"[{wallbox_name}] Successfully called service 'easee/action_command-pause' with data {device_id}.",
            level="INFO")

    def resume_charging(self, device_id, wallbox_name):
        self.call_service(EASEE_SERVICE_ACTION_COMMAND, device_id=device_id,
                          action_command='resume')
        self.log(
            f"[{wallbox_name}] Successfully called service 'easee/action_command-resume' with data {device_id}.",
            level="INFO")

    def _set_charger_dyn_limit_pause_resume(self, device_id, current_dyn_limit, new_charge_current,
                                            wallbox_name):
        is_currently_suspended = self.is_paused_on_dyn_limit_too_low(current_dyn_limit)
        will_be_suspended = self.is_paused_on_dyn_limit_too_low(new_charge_current)
        if not is_currently_suspended and will_be_suspended:
            self.pause_charging(device_id, wallbox_name)
        self.call_service(EASEE_CMD_SET_CHARGER_DYNAMIC_LIMIT, device_id=device_id,
                          current=int(new_charge_current))
        if is_currently_suspended and not will_be_suspended:
            self.resume_charging(device_id, wallbox_name)


    def calculate_new_dynamic_current(self):
        ist_ladesteuerung_wb_aktiv = self.ist_ls_wb_aktiv()
        new_current = (
                    self.to_float_safe(self.get_state(SENSOR_EMS_LADESTEUERUNG_WB_LADESTEUERUNG_CHARGE_LIMIT))
                    + self._get_dyn_ladebegrenzung_aus_abregelung())
        if ist_ladesteuerung_wb_aktiv and new_current > 0.0:
            new_current_hybrid = min(round(new_current) + CURRENT_INCREASE_HYBRID_BOXES, MAX_CURRENT_HYBRID_BOXES)
        else:
            new_current = 0
            new_current_hybrid = 0
        return ist_ladesteuerung_wb_aktiv, new_current, new_current_hybrid

    def adjust_dynamic_wb_current_from_battery_load(self):
        """
        Prueft bei jedem fuenften Durchlauf, ob der Wallbox-Ladestrom
        basierend auf dem Batterie-Ueberschuss angepasst werden muss.
        """
        if not self.ist_ls_wb_dynamic_current_aktiv():
            self.log(f"ist_ls_wb_dynamic_current_aktiv():{self.ist_ls_wb_dynamic_current_aktiv()}", level="INFO")
            return
        if self.run_count % ADJUST_DYN_CURRENT_EVERY_N_RUNS != 0:
            return
        if self.get_state(SENSOR_WALLBOX_NETZBEGRENZUNG_AKTIV, default="off") == "on":
            self.log(f"Ueberlastregelung ist aktiv, Ladelimit wird so lange nicht angepasst:{self.get_state(SENSOR_WALLBOX_NETZBEGRENZUNG_AKTIV)}", level="INFO")
            return # keine Regelung bei Überlaststeuerung

        try:
            self.update_wallboxes()
            anz_aktive_wb = self.anzahl_aktiver_non_prio_wallboxes()
            anz_aktive_wb_phasen = self.anzahl_aktiver_non_prio_wallbox_phasen()

            self.log(f"Anzahl aktiver Wallboxen, fuer die dyn. Ladesteuerun durchgefuehrt wird {anz_aktive_wb}, "
                     f"Phasen {anz_aktive_wb_phasen}", level="INFO")
            if anz_aktive_wb <= 0:
                return
            aktives_ladelimit = self.to_float_safe(self.get_state(SENSOR_EMS_LADESTEUERUNG_WB_LADESTEUERUNG_CHARGE_LIMIT))
            neues_strom_setting = aktives_ladelimit
            aktive_batterie_unterversorgung = self.to_float_safe(
                self.get_state(SENSOR_EMS_LADESTEUERUNG_AKTIVE_BATTERIEN_UNTERVERSORGUNG))
            aktiver_batterie_ueberschuss = self.to_float_safe(
                self.get_state(SENSOR_EMS_LADESTEUERUNG_BATTERIE_UEBERSCHUSS_IN_W))

            self.log(f"Fuehre adjust_dynamic_wb_current_from_battery_load aus ({self.run_count}), "
                     f"aktives_ladelimit({aktives_ladelimit}), "
                     f"aktive_batterie_unterversorgung({aktive_batterie_unterversorgung}), "
                     f"aktiver_batterie_ueberschuss({aktiver_batterie_ueberschuss}) .",
                     level="INFO")
            # --- 3. Bedingungen pruefen ---
            # Eine negative Unterversorgung bedeutet, es gibt einen Ueberschuss aus der Batterie.
            # Zusaetzlich muss der Gesamt-Batterieueberschuss einen Schwellenwert ueberschreiten.
            if aktiver_batterie_ueberschuss > ((anz_aktive_wb_phasen * 2.0 / 3.0) * REDUCABLE_W_PER_PHASE):

                if aktives_ladelimit >= MAX_CURRENT_HYBRID_BOXES:
                    self.log(f"aktives_ladelimit ({aktives_ladelimit}) >= {MAX_CURRENT_HYBRID_BOXES} (max. Ladelimit)")
                    return

                # --- 4. Verfuegbaren Ladestrom berechnen (Annahme: 3-phasig) ---
                # Formel: Strom (A) = Leistung (W) / Spannung (V)
                # Fuer 3 Phasen wird die Leistung durch 3 * 230V (ca. 690) geteilt.
                verfuegbarer_strom = round((aktiver_batterie_ueberschuss / anz_aktive_wb_phasen) / REDUCABLE_W_PER_PHASE, 0)
                neues_strom_setting = min(verfuegbarer_strom + aktives_ladelimit, MAX_CURRENT_HYBRID_BOXES)

                self.log(
                    f"Bedingungen fuer Ladeerhoehung erfuellt: Aktive Unterversorgung: {aktive_batterie_unterversorgung}W, "
                    f"Batterie-Ueberschuss: {aktiver_batterie_ueberschuss}W.,"
                    f"verfuegbarer_strom: {verfuegbarer_strom}A, neues_strom_setting: {neues_strom_setting}A",)

            if  aktiver_batterie_ueberschuss <  ((min(anz_aktive_wb_phasen,3) / 2) * -REDUCABLE_W_PER_PHASE):

                if aktives_ladelimit <= MINIMAL_CHARGE_CURRENT:
                    self.log(f"aktives_ladelimit ({aktives_ladelimit}) <= {MINIMAL_CHARGE_CURRENT} (min. Ladelimit)")
                    return
                reduzierbarer_strom = abs(round((aktiver_batterie_ueberschuss / anz_aktive_wb_phasen) / REDUCABLE_W_PER_PHASE, 0))
                if aktive_batterie_unterversorgung > 0:
                    reduzierbarer_strom += 1
                neues_strom_setting = max(aktives_ladelimit - reduzierbarer_strom, MINIMAL_CHARGE_CURRENT)

                self.log(
                    f"Bedingungen fuer Ladereduzierung erfuellt: Aktive Unterversorgung: {aktive_batterie_unterversorgung}W, Batterie-Ueberschuss: {aktiver_batterie_ueberschuss}W"
                    f"reduzierbarer_strom: {reduzierbarer_strom}A, neues_strom_setting: {neues_strom_setting}A.")

            if aktives_ladelimit != neues_strom_setting:
                self.log(f"Neues Ladelimit: {neues_strom_setting}A")
                self.call_service(
                    "input_number/set_value",
                    entity_id=SENSOR_EMS_LADESTEUERUNG_WB_LADESTEUERUNG_CHARGE_LIMIT,
                    value=neues_strom_setting
                )
        except Exception as e:
            self.log(f"Fehler beim Justieren des dyn. WB-Ladestroms: {e}", level="ERROR")

    def start_waiting_wallboxes(self):
        waiting_wallboxes = list(filter(
            lambda item: isinstance(item, dict) and item.get("status")
                         in [self.WbStatus.awaiting_start, self.WbStatus.awaiting_authorization]
            and (item.get("reason_no_current", self.WbReasonNoCurrent.not_relevant)
                 not in [self.WbReasonNoCurrent.waiting_in_fully, self.WbReasonNoCurrent.max_current_too_low])
            and not self.awaiting_disconnect_flags.get(item.get("wb_status_id"), False)
            and not self.is_paused_on_dyn_limit_too_low(item.get("reason_no_current",MINIMAL_CHARGE_CURRENT)),
            self.wallboxes.values()
        ))
        prioritized_waiting_wb = self.priorize_waiting_wallboxes(waiting_wallboxes)
        self.log(f"start_waiting_wallboxes (priorisiert): {prioritized_waiting_wb}", level="DEBUG")
        ist_ladesteuerung_wb_aktiv = self.ist_ls_wb_aktiv()
        if ist_ladesteuerung_wb_aktiv and prioritized_waiting_wb:
            self.start_charging_wb(prioritized_waiting_wb[0], "manuelle Ladevorgabe")

    def ist_ls_wb_aktiv(self):
        ladesteuerung_wb_aktiv = self.get_state(SENSOR_EMS_LADESTEUERUNG_WB_LADESTEUERUNG_AKTIV, default="0")
        ist_ladesteuerung_wb_aktiv = (ladesteuerung_wb_aktiv and (
                                      (isinstance(ladesteuerung_wb_aktiv, str) and ladesteuerung_wb_aktiv != "0")
                                      or (isinstance(ladesteuerung_wb_aktiv, int) and ladesteuerung_wb_aktiv != 0)))
        return ist_ladesteuerung_wb_aktiv

    def ist_ls_wb_dynamic_current_aktiv(self):
        ladesteuerung_wb_aktiv = self.get_state(SENSOR_EMS_LADESTEUERUNG_WB_LADESTEUERUNG_DYNAMIC_CURRENT, default="on")
        ist_ladesteuerung_wb_aktiv = ladesteuerung_wb_aktiv and (ladesteuerung_wb_aktiv == "on")
        return ist_ladesteuerung_wb_aktiv

    def priorize_waiting_wallboxes(self, wallboxes_to_sort: list[dict[str, any]]) -> list[dict[str, any]]:
        """
        Sortiert eine uebergebene Liste von Wallboxen nach Prioritaet und
        historischer durchschnittlicher Ladezeit.

        Args:
            wallboxes_to_sort: Eine Liste von Wallbox-Dicts, die sortiert werden soll.

        Returns:
            Die sortierte Liste der Wallbox-Dicts.
        """
        self.log("Starte Priorisierung der wartenden Wallboxen.", level="DEBUG")

        # Lade die Durchschnitts-Statistiken
        avg_stats_sensor = self.get_state("%s" % SENSOR_EMS_LS_WALLBOXES_AVERAGE_CHARGING_STATS, attribute="average_stats")

        avg_ctm_map = {}
        if avg_stats_sensor:
            for wb_name, stats in avg_stats_sensor.items():
                avg_ctm_map[wb_name] = stats.get('avg_ctm', 0)
        else:
            self.log(
                "Warnung: Sensor '%s' nicht gefunden. Sortiere nur nach Prio." % SENSOR_EMS_LS_WALLBOXES_AVERAGE_CHARGING_STATS)

        # Sortiere die uebergebene Liste
        def sort_key(wallbox):
            # Primaeres Kriterium: Prioritaet, absteigend (hoeherer Wert zuerst)
            priority_val = wallbox.get('priority', self.WbPrio.KEINE).value

            # Sekundaeres Kriterium: Durchschnittliche Ladezeit, absteigend
            wb_name = wallbox.get('name')
            avg_ctm = avg_ctm_map.get(wb_name, 0)

            # Rueckgabe eines Tuples fuer die Sortierung.
            # Negative Werte, um eine absteigende Reihenfolge zu erreichen.
            return -priority_val, -avg_ctm

        sorted_wallboxes = sorted(wallboxes_to_sort, key=sort_key)

        return sorted_wallboxes

    def cleanup_at_night(self, kwargs):
        """
        Resets certain settings and configurations related to wallbox management to
        their default or minimal states during nighttime. This method acts as part
        of an automated schedule to ensure the system adheres to predefined rules
        and frees itself of temporary states that are no longer necessary.

        :param kwargs: Dictionary of additional parameters that may influence the
            cleanup process or provide context-based configurations.
        :return: None
        """
        # Setze manuelle Ladesteuerung wieder auf das Minimum zurück
        ist_ladesteuerung_wb_aktiv = self.ist_ls_wb_aktiv()
        current_set = self.to_float_safe(self.get_state(SENSOR_EMS_LADESTEUERUNG_WB_LADESTEUERUNG_CHARGE_LIMIT))
        if ist_ladesteuerung_wb_aktiv and current_set:
            attributes = self.get_state(SENSOR_EMS_LADESTEUERUNG_WB_LADESTEUERUNG_CHARGE_LIMIT, attribute="all").get("attributes", {})
            charge_current_tomorrow=MINIMAL_CHARGE_CURRENT
            if self.get_state(SENSOR_WORKING_DAY_TOMORROW) and self.get_state(SENSOR_WORKING_DAY_TOMORROW) != 'True':
                # an Feiertagen und am Wochenende auf maximalen Ladestrom setzen
                charge_current_tomorrow = MAX_CURRENT_HYBRID_BOXES
            self.set_state(SENSOR_EMS_LADESTEUERUNG_WB_LADESTEUERUNG_CHARGE_LIMIT, state=charge_current_tomorrow, attributes=attributes)
            self.log(f'setze {SENSOR_EMS_LADESTEUERUNG_WB_LADESTEUERUNG_CHARGE_LIMIT} fuer morgen auf {charge_current_tomorrow}')

        # Lösche Wallboxen mit Temperaturproblemen
        self.last_notification_check_wallbox_temperature_map.clear()

        # Setze einmalige Prios an den einzelnen Wallboxen zurück
        self.update_wallboxes()
        for wb_config in self.wallboxes.values():
            if self.check_hat_prio(wb_config) and wb_config.get("priority") == self.WbPrio.EINMAL:
                prio_sensor_id = self.get_ems_sensor_id(self.ems_helper["priority"], wb_config)
                attributes = self.get_state(prio_sensor_id, attribute="all").get("attributes", {})
                self.set_state(prio_sensor_id, state=self.WbPrio.KEINE.name.lower(), attributes=attributes)

    def get_wb_device_id(self, wallbox_name):
        """
        Fetches the device ID for a given wallbox name.

        This method retrieves the corresponding device ID for the wallbox specified
        by the given name from the wb_config_map attribute. The wb_config_map is
        expected to be a dictionary mapping wallbox names to their associated device
        information, where the device ID is stored under the "device_id" key.

        :param wallbox_name: The name of the wallbox for which the device ID is
            to be retrieved.
        :type wallbox_name: str
        :return: The device ID corresponding to the specified wallbox name.
        :rtype: str
        """
        if wallbox_name in self.wb_config_map:
            device_info = self.wb_config_map[wallbox_name]
            if device_info:
                return device_info["device_id"]
        return None

    def send_notification_status_temperature_wallbox(self, wallbox_name, temperature, message):
        now = datetime.now()
        if int(temperature) > MAX_WB_TEMPERATURE_TO_REDUCE_TO_HALF_IN_C:
            if wallbox_name in self.last_notification_check_wallbox_temperature_extreme_map:
                wb_data = self.last_notification_check_wallbox_temperature_extreme_map[wallbox_name]
                last_notification = wb_data["last_notification"]
            else:
                self.last_notification_check_wallbox_temperature_extreme_map[wallbox_name] = {"name": wallbox_name, "last_notification": now}
                last_notification = now - timedelta(hours=25)
        else:
            if wallbox_name in self.last_notification_check_wallbox_temperature_map:
                wb_data = self.last_notification_check_wallbox_temperature_map[wallbox_name]
                last_notification = wb_data["last_notification"]
            else:
                self.last_notification_check_wallbox_temperature_map[wallbox_name] = {"name": wallbox_name, "last_notification": now}
                last_notification = now - timedelta(hours=25)
        if (now - last_notification).total_seconds() > 3600 * 24:  # nur 1 Nachricht pro Tag
            self.call_service("script/notify_admins_with_label_admin_notification",
                               message_id=f"wb_{wallbox_name}_temperature_too_high",
                               title=f"Wallbox {wallbox_name} wurde wegen zu hoher Temperatur in Ladeleistung begrenzt",
                               message=message)
            self.log(f"send_notification_status_temperature_wallbox: Benachrichtigung fuer WB {wallbox_name}: {temperature} gesendet.")

    def map_to_wb_reason_no_current(self, attrib_value: str):
        if not attrib_value:
            self.log(f"map_to_wb_status: Unknown status value: {attrib_value}, return {self.WbReasonNoCurrent.not_relevant}", level="DEBUG")
            return self.WbReasonNoCurrent.not_relevant
        try:
            if attrib_value.startswith("max"):
                return self.WbReasonNoCurrent.max_current_too_low
            if attrib_value.startswith("limited"):
                return self.WbReasonNoCurrent.limited
            if attrib_value.startswith("not_requesting"):
                return self.WbReasonNoCurrent.not_relevant
            return self.WbReasonNoCurrent[attrib_value]
        except KeyError:
            self.log(f"map_to_wb_reason_no_current: Unknown status value: {attrib_value}, return {self.WbReasonNoCurrent.not_relevant}", level="DEBUG")
            return self.WbReasonNoCurrent.not_relevant

    def map_to_wb_status(self, attrib_value):
        """
        Maps given attribute value to its corresponding WbStatus enumeration value.

        This method attempts to match the input attribute value to a key in the WbStatus
        enumeration. If the attribute value is invalid (either as None or a value not
        present in the enumeration), the method logs a warning message and returns
        the `not_relevant` enumeration value.

        :param attrib_value: The attribute value to be mapped to WbStatus.
        :type attrib_value: str
        :return: Corresponding WbStatus enumeration value if mapping is successful,
                 otherwise returns WbStatus.not_relevant.
        :rtype: WbStatus
        """
        if not attrib_value:
            self.log(f"map_to_wb_status: Unknown status value: {attrib_value}, return {self.WbStatus.not_relevant}", level="DEBUG")
            return self.WbStatus.not_relevant
        try:
            return self.WbStatus[attrib_value]
        except KeyError:
            self.log(f"map_to_wb_status: Unknown status value: {attrib_value}, return {self.WbStatus.not_relevant}", level="DEBUG")
            return self.WbStatus.not_relevant

    def map_to_wb_prio(self, attrib_value:str):
        """
        Maps given attribute value to its corresponding WbStatus enumeration value.

        This method attempts to match the input attribute value to a key in the WbStatus
        enumeration. If the attribute value is invalid (either as None or a value not
        present in the enumeration), the method logs a warning message and returns
        the `not_relevant` enumeration value.

        :param attrib_value: The attribute value to be mapped to WbStatus.
        :type attrib_value: str
        :return: Corresponding WbStatus enumeration value if mapping is successful,
                 otherwise returns WbStatus.not_relevant.
        :rtype: WbStatus
        """
        if not attrib_value:
            return self.WbPrio.KEINE
        try:
            return self.WbPrio[attrib_value.upper()]
        except KeyError:
            self.log(f"map_to_wb_prio: Unknown prio value: {attrib_value}, return {self.WbPrio.KEINE}", level="WARNING")
            return self.WbPrio.KEINE

    def stop_charging_wb(self, wb_config, reason):
        """
        Stops the charging process for a wallbox device using the provided configuration and reason.
        Logs the process of stopping the charging, including the success or any error encountered.

        :param wb_config: Configuration for the wallbox, containing necessary details such as
                          device ID and name to identify the specific wallbox.
        :type wb_config: dict
        :param reason: The reason for stopping the charging process.
        :type reason: str
        :return: None
        """
        wb_name = wb_config["name"]
        self.log(f"[{wb_name}] Attempting to stop charging. Reason: {reason}", level="INFO")
        try:
            device_id = wb_config["device_id"]
            self.call_service(EASEE_SERVICE_ACTION_COMMAND, device_id=device_id, action_command='stop')

            self.log(f"[{wb_name}] Successfully called service 'easee/action_command-stop' with data {device_id}.",
                     level="INFO")
        except Exception as e:
            self.log(f"[{wb_name}] Error calling service to stop charging: {e}", level="ERROR")


    def start_charging_wb(self, wb_config, reason):
        """
        Attempts to initiate the charging process for a Wallbox based on its configuration and a given reason. The method
        verifies priority conditions before executing commands to start charging or override scheduling through specified
        services.

        :param wb_config: Configuration dictionary for the Wallbox. The dictionary must include details such as the Wallbox
            name (str), device ID (str), and its charging status (enum or equivalent representation).
        :param reason: The reason for attempting to start charging. This is a string representation that is logged for
            contextual tracking purposes.
        :return: None
        """
        wb_name = wb_config["name"]
        self.log(f"[{wb_name}] Attempting to start charging. Reason: {reason}", level="INFO")
        try:
            device_id = wb_config["device_id"]
            status = wb_config["status"]
            if (self.check_hat_prio(wb_config)
                    or datetime.now(tz=TIMEZONE_BERLIN).time() >=
                    self.read_time_from_helper(SENSOR_EMS_LADESTEUERUNG_WB_AUTO_CHARGE_TIME_HELPER,SENSOR_EMS_LADESTEUERUNG_WB_AUTO_CHARGE_TIME_DEFAULT )):
                if status == self.WbStatus.awaiting_start:
                    self.call_service(EASEE_SERVICE_ACTION_COMMAND, device_id=device_id, action_command='override_schedule')
                    self.log(
                        f"[{wb_name}] Successfully called service 'easee/action_command-override_schedule' with data {device_id}.",
                        level="INFO")
                    return
                if status == self.WbStatus.awaiting_authorization:
                    self.call_service(EASEE_SERVICE_ACTION_COMMAND, device_id=device_id,
                                      action_command='start')
                    self.log(
                        f"[{wb_name}] Successfully called service 'easee/action_command-start' with data {device_id}.",
                        level="INFO")
                    return
        except Exception as e:
            self.log(f"[{wb_name}] Error calling service to start charging: {e}", level="ERROR")


    def read_time_from_helper(self, helper_entity_id:str, default:str):
        time_str = self.get_state(helper_entity_id)

        if not time_str or time_str is None:
            self.log(f"WARN: Entity {helper_entity_id} not found or its state is None, return default {default}.", level="WARNING")
            return datetime.strptime(default, "%H:%M:%S").time()

        if time_str == "unavailable":
            self.log(f"Warning: Entity {helper_entity_id} is unavailable.", level="WARNING")
            return datetime.strptime(default, "%H:%M:%S").time()

        try:
            parsed_time_object = datetime.strptime(time_str, "%H:%M:%S").time()
            self.log(f"Parsed time string '{time_str}' into time object: {parsed_time_object}")
            self.log(
                f"Hour: {parsed_time_object.hour}, Minute: {parsed_time_object.minute}, Second: {parsed_time_object.second}")
            return parsed_time_object
        except ValueError as e:
            self.log(f"Error parsing time string '{time_str}': {e}, returning{default} instead ", level="WARNING")
            return datetime.strptime(default, "%H:%M:%S").time()

    def is_charging_single_phase(self, wb_config) -> bool:
        """
        Ermittelt, ob ein Fahrzeug einphasig laedt, basierend auf Leistung und Stromstaerke.

        Args:
            wb_config: Ein Dictionary, das den aktuellen Zustand der Wallbox enthaelt.

        Returns:
            True, wenn die Wallbox einphasig laedt, andernfalls False.
        """

        # --- Eingabevalidierung ---
        # Stellt sicher, dass alle benoetigten Schluessel im Dictionary vorhanden sind
        required_keys = ['status', 'power', 'current']
        if not all(key in wb_config and wb_config[key] is not None for key in required_keys):
            self.log(f"Wallbox '{wb_config.get('name', 'Unbekannt')}' fehlen benoetigte Daten ({required_keys}).", level="WARNING")
            return False

        # --- Logik ---
        # 1. Pruefen, ob die Wallbox tatsaechlich laedt und Strom zieht
        if wb_config['status'] != self.WbStatus.charging or wb_config['current'] < MIN_CURRENT_THRESHOLD:
            return False

        # 2. Umwandlung der Leistung von kW in W fuer die Berechnung
        actual_power_w = wb_config['power'] * 1000

        # 3. Berechnung der theoretisch erwarteten Leistung fuer eine einzelne Phase
        theoretical_power_w = VOLTAGE * wb_config['current']

        # Vermeidet eine Division durch Null, falls der Strom zwar fliesst, die theoretische Leistung aber 0 ist
        if theoretical_power_w == 0:
            return False

            # 4. Pruefen, ob die tatsaechliche Leistung im erwarteten Bereich fuer eine einphasige Ladung liegt
        lower_bound = theoretical_power_w * (1 - TOLERANCE)
        upper_bound = theoretical_power_w * (1 + TOLERANCE)

        if lower_bound <= actual_power_w <= upper_bound:
            # Die tatsaechliche Leistung ist konsistent mit einer einphasigen Ladung
            return True
        else:
            # Die tatsaechliche Leistung ist wahrscheinlich zu hoch (z.B. dreiphasig) oder zu niedrig (Fehler)
            return False

    def is_paused_on_dyn_limit_too_low(self, check_dyn_limit: float) -> bool:
        """
        Prueft, ob ein Ladelimit unter den minimal notwendigen Ladestrom gefallen ist.

        Args:
            wallbox: Das Dictionary-Objekt der zu pruefenden Wallbox.
            check_dyn_limit (optional): Ein spezifisches Limit, das anstelle des
                                       Limits aus dem Wallbox-Objekt geprueft werden soll.

        Returns:
            True, wenn das Limit zu niedrig ist, ansonsten False.
        """
        try:
            # Bestimmt, welcher Wert ueberprueft werden soll.
            # Wenn new_dyn_limit uebergeben wird, hat es Vorrang.
            if check_dyn_limit is not None:

                dyn_limit = float(check_dyn_limit)
                if dyn_limit < MINIMAL_CHARGE_CURRENT:
                    return True
        except (ValueError, TypeError):
            # Falls der Wert kein gueltiger Wert ist, wird False zurueckgegeben.
            return False
        return False

    def is_creepy_ev_charger(self, wallbox: dict[str, any]) -> bool | None:
        """
        Prueft, ob ein Ladegeraet "schleichend" laedt, d.h. deutlich weniger als
        die erwartete Leistung basierend auf dem dynamischen Limit.
        Diese Funktion erwartet, dass das uebergebene 'wallbox'-dict aktuelle
        Live-Daten enthaelt.

        Args:
            wallbox: Ein Dictionary, das eine Wallbox repraesentiert und die
                     Schluessel 'power' (in kW), 'current' (in A) und 'dyn_limit' (in A) enthaelt.

        Returns:
            True, wenn die Ladeleistung mindestens 5% unter der Erwartung liegt,
            ansonsten False oder None falls nicht ermittelbar (nicht im Status Charging)
            :rtype: object
        """

        try:
            power_kw = float(wallbox.get('power', 0))
            dyn_limit_a = float(wallbox.get('dyn_limit', 0))
            current_a = float(wallbox.get('current', 0))
            wb_name = wallbox.get('name', "")

            # Pruefung nur durchfuehren, wenn ein Limit gesetzt ist, Leistung fliesst
            # und die Abweichung zwischen Soll- und Ist-Strom gering ist.
            if dyn_limit_a > 0 and power_kw > 0 and abs(dyn_limit_a - current_a) < 1:
                actual_power_w = power_kw * 1000

                # Erwartete Leistung bei einphasiger Ladung berechnen
                expected_power_w = VOLTAGE * dyn_limit_a

                # Pruefen, ob die tatsaechliche Leistung unter dem Toleranzwert liegt
                if actual_power_w < (expected_power_w * CREEPY_CHARGER_TOLERANCE_FACTOR):
                    # Logik, um das Loggen auf einmal pro Stunde zu beschraenken
                    now = self.datetime(aware=True)
                    last_log_time = self.last_creepy_log_time.get(wb_name, now)

                    if not last_log_time or (now - last_log_time) > timedelta(hours=1):
                        self.log(f"Schleichender Lader erkannt bei '{wallbox.get('name')}': "
                                 f"Erwartet: >{expected_power_w * CREEPY_CHARGER_TOLERANCE_FACTOR:.0f} W, "
                                 f"Tatsaechlich: {actual_power_w:.0f} W")
                        self.last_creepy_log_time[wb_name] = now

                    return True
                else:
                    return False

        except (TypeError, ValueError) as e:
            self.error(f"Fehler bei der Pruefung auf schleichenden Lader fuer '{wallbox.get('name')}': {e}")
            return None

        return None

    def calculate_charging_averages(self, structured_daily_stats):
        """
        Berechnet die Durchschnittswerte fuer Ladezeit, Blockierzeit, Energie und
        Ladezeiten pro Wochentag fuer jede Wallbox.
        Beruecksichtigt nur Tage, an denen tatsaechlich geladen wurde.

        Args:
            structured_daily_stats: Eine Map mit den Verlaufsdaten aller Wallboxen.

        Returns:
            Eine Map mit den Durchschnittswerten pro Wallbox.
        """
        averages_map = {}
        for wb_name, daily_stats_list in structured_daily_stats.items():
            # Filtert die Tage heraus, an denen keine Energie geladen wurde
            valid_charge_days = [item for item in daily_stats_list if item.get('enr', 0) > 0]

            if not valid_charge_days:
                continue  # Ueberspringt Wallboxen ohne Ladehistorie

            total_ctm = sum(item['ctm'] for item in valid_charge_days)
            total_btm = sum(item['btm'] for item in valid_charge_days)
            total_enr = sum(item['enr'] for item in valid_charge_days)

            count = len(valid_charge_days)

            # Berechnung der durchschnittlichen Zeiten pro Wochentag 0 = Montag, 6 = Sonntag
            weekday_times = {i: {'fct': [], 'ldt': []} for i in range(7)}

            for item in valid_charge_days:
                # Datumsobjekt aus dem ISO-String erstellen, um den Wochentag zu bekommen
                day_obj = datetime.fromisoformat(item['dt']).date()
                weekday = day_obj.weekday()

                # Umwandlung der Zeit-Strings in Minuten seit Mitternacht
                if item.get('fct'):
                    try:
                        h, m = map(int, item['fct'].split(':'))
                        weekday_times[weekday]['fct'].append(h * 60 + m)
                    except (ValueError, AttributeError):
                        pass  # Ignoriert fehlerhafte oder leere Zeit-Strings

                if item.get('ldt'):
                    try:
                        h, m = map(int, item['ldt'].split(':'))
                        weekday_times[weekday]['ldt'].append(h * 60 + m)
                    except (ValueError, AttributeError):
                        pass

            # Berechnung der Durchschnitte und Umwandlung zurueck in HH:MM
            blocking_times_by_weekday = {}
            for day_num, times in weekday_times.items():
                avg_fct_str = None
                if times['fct']:
                    avg_fct_min = sum(times['fct']) / len(times['fct'])
                    avg_h, avg_m = divmod(avg_fct_min, 60)
                    avg_fct_str = f"{int(avg_h):02d}:{int(round(avg_m)):02d}"

                avg_ldt_str = None
                if times['ldt']:
                    avg_ldt_min = sum(times['ldt']) / len(times['ldt'])
                    avg_h, avg_m = divmod(avg_ldt_min, 60)
                    avg_ldt_str = f"{int(avg_h):02d}:{int(round(avg_m)):02d}"

                # Nur Wochentage hinzufuegen, an denen es auch Daten gab
                if avg_fct_str or avg_ldt_str:
                    blocking_times_by_weekday[str(day_num)] = {
                        "avg_fct": avg_fct_str,
                        "avg_ldt": avg_ldt_str
                    }

            # Hinzufuegen der neuen Daten zur Haupt-Map
            averages_map[wb_name] = {
                "avg_ctm": round(total_ctm / count) if count > 0 else 0,
                "avg_btm": round(total_btm / count) if count > 0 else 0,
                "avg_enr": round(total_enr / count, 2) if count > 0 else 0,
                "avg_btms": blocking_times_by_weekday  # NEUE SUB-MAP
            }
        return averages_map

    def calculate_charging_history(self, kwargs):
        """
        Ermittelt die historischen Daten fuer alle konfigurierten Wallboxen
        und speichert sie in den Sensoren.
        """
        self.log("Starte Berechnung der Lade-Historie fuer alle Wallboxen.")

        try:
            local_tz = self.get_timezone()
            now = self.datetime(aware=True)
            today = now.date()
            history_duration_days = 30

            structured_daily_stats = {}

            for wb_key, wb_config in self.wallboxes.items():
                wb_name = wb_config['name']
                status_entity_id = wb_config['wb_status_id']
                energy_entity_id = wb_config['wb_entity_prefix'] + '_gesamtenergie'
                self.log(f"Verarbeite Daten fuer: {wb_name}")

                # Lade- und Blockierzeit
                status_history = self.get_history(entity_id=status_entity_id, days=history_duration_days)
                daily_charging_seconds = {}
                daily_blocking_seconds = {}
                # NEU: Dictionaries fuer die erste Verbindung und letzte Trennung
                daily_first_connection_time = {}
                daily_last_disconnection_time = {}

                if status_history and status_history[0]:
                    for i, state_data in enumerate(status_history[0]):
                        start_time_local = (state_data['last_updated']).astimezone(local_tz)
                        day_key = start_time_local.date()
                        current_time_only = start_time_local.time()
                        current_state = state_data['state']

                        if current_state != 'disconnected':
                            # Wenn fuer diesen Tag noch keine Verbindungszeit gespeichert wurde, ist dies die erste.
                            if day_key not in daily_first_connection_time:
                                daily_first_connection_time[day_key] = current_time_only

                        if current_state == 'disconnected':
                            # Ueberschreibt immer die letzte bekannte Trennungszeit, da die Historie chronologisch ist.
                            daily_last_disconnection_time[day_key] = current_time_only

                        if i + 1 < len(status_history[0]):
                            end_time_local = (
                                status_history[0][i + 1]['last_updated']).astimezone(local_tz)
                            # Bei mehrtägigen Blockierungen wird nur die Zeit bis zum ersten Tagesende gezählt
                            if end_time_local.date() != day_key:
                                end_time_local = datetime.combine(day_key, time.max).astimezone(local_tz)
                        else:
                            end_time_local = self.datetime(aware=True)
                        duration_seconds = (end_time_local - start_time_local).total_seconds()

                        if current_state == 'charging':
                            daily_charging_seconds[day_key] = daily_charging_seconds.get(day_key, 0) + duration_seconds
                        if current_state != 'disconnected':
                            daily_blocking_seconds[day_key] = daily_blocking_seconds.get(day_key, 0) + duration_seconds

                # Geladene Energie in kWh
                energy_history = self.get_history(entity_id=energy_entity_id, days=history_duration_days)
                daily_energy_readings = {}
                if energy_history and energy_history[0]:
                    for state_data in energy_history[0]:
                        try:
                            value = float(state_data['state'])
                            timestamp = (state_data['last_updated']).astimezone(local_tz)
                            day_key = timestamp.date()
                            daily_energy_readings.setdefault(day_key, []).append(value)
                        except (ValueError, TypeError):
                            continue
                daily_energy_kwh = {day: max(readings) - min(readings) for day, readings in
                                    daily_energy_readings.items() if len(readings) > 1}
                daily_energy_kwh_filtered = {key: value for key, value in daily_energy_kwh.items() if value > 0}

                # Ergebnisse formatieren
                wallbox_stats_list = []
                all_days = set(daily_energy_kwh_filtered.keys())
                for day in sorted(list(all_days), reverse=True):
                    # NEU: Zeiten abrufen und formatieren
                    first_conn_time = daily_first_connection_time.get(day)
                    last_disconn_time = daily_last_disconnection_time.get(day)

                    wallbox_stats_list.append({
                        "dt": day.isoformat(),
                        "ctm": round(daily_charging_seconds.get(day, 0) / 60),
                        "btm": round(daily_blocking_seconds.get(day, 0) / 60),
                        "enr": round(daily_energy_kwh_filtered.get(day, 0), 2),
                        "fct": first_conn_time.strftime('%H:%M') if first_conn_time else None,
                        "ldt": last_disconn_time.strftime('%H:%M') if last_disconn_time else None,
                    })
                if len(wallbox_stats_list) > 0:
                    structured_daily_stats[wb_name] = wallbox_stats_list

            # Prüfen, ob für den aktuellen Tag und jeden wb_namen ein Eintrag vorhanden ist
            for wb_key, wb_config in self.wallboxes.items():
                wb_name = wb_config['name']
                today_entry = None
                if wb_name in structured_daily_stats:
                    today_entry = next(
                        (item for item in structured_daily_stats[wb_name] if item['dt'] == today.isoformat()),
                        None
                    )
                if today_entry is None:
                    self.log(f"Kein Eintrag fuer heute ({today.isoformat()}) fuer Wallbox '{wb_name}' vorhanden.")

                self.speichere_heutige_statistiken(wb_name, today_entry)

            average_stats = self.calculate_charging_averages(structured_daily_stats)

            total_average_charge_time = 0
            if average_stats:
                total_average_charge_time = sum(item.get('avg_ctm', 0) for item in average_stats.values())

            avg_sensor_id = SENSOR_EMS_LS_WALLBOXES_AVERAGE_CHARGING_STATS
            self.set_state(avg_sensor_id,
                           state=total_average_charge_time,
                           attributes={
                               "friendly_name": "Wallboxen durchschnittliche Ladestatistik",
                               "unit_of_measurement": "min",
                               "icon": "mdi:chart-bar",
                               "average_stats": average_stats
                           })
            self.log(f"Durchschnitts-Sensor '{avg_sensor_id}' wurde aktualisiert.")

            # Gesamte Ladezeit heute für den Sensor-State berechnen
            total_todays_charge_time = 0
            for wb_name, daily_data in structured_daily_stats.items():
                for item in daily_data:
                    if item['dt'] == today.isoformat():
                        total_todays_charge_time += item['ctm']

            # Zielsensor aktualisieren
            target_sensor_id = SENSOR_WALLBOX_DAILY_CHARGING_STATS
            self.set_state(target_sensor_id,
                           state=total_todays_charge_time,
                           attributes={
                               "friendly_name": "Wallboxen taegliche Ladedauer",
                               "unit_of_measurement": "min",
                               "icon": "mdi:chart-timeline-variant",
                               "daily_stats": structured_daily_stats
                           })
            self.log(f"Historie fuer Sensor '{target_sensor_id}' wurde aktualisiert.")
        except Exception as e:
            self.error(f"Fehler bei der Berechnung der Lade-Historie: {e}")

    def speichere_heutige_statistiken(self, wb_name, today_entry):
        """Setzt die heutigen Statistik-Sensoren fuer eine Wallbox."""
        btm = today_entry['btm'] if today_entry else 0
        enr = today_entry['enr'] if today_entry else 0

        self.set_state(f"sensor.wallbox_{wb_name}_blockierzeit_heute",
                       state=btm,
                       attributes={"unit_of_measurement": "min", "icon": "mdi:timer-outline",
                                   "state_class": "measurement"})
        self.set_state(f"sensor.wallbox_{wb_name}_energie_heute",
                       state=enr,
                       attributes={"unit_of_measurement": "kWh", "icon": "mdi:lightning-bolt",
                                   "state_class": "measurement"})

    def initialize_netzbezugsbegrenzung(self):
        """Initialisiert die App und richtet den Listener ein."""
        self.log("Wallbox Ueberlast-Steuerung wird initialisiert.")

        # Interne Variablen zum Speichern von Zuständen
        self.request_id_aktuell = None
        self.charge_limit_vor_abregelung = None

        if self.get_state(SENSOR_WALLBOX_NETZBEGRENZUNG_AKTIV) is None:
            self.set_state(SENSOR_WALLBOX_NETZBEGRENZUNG_AKTIV, state="off")


        # Listener, der auf jede Änderung des Sensors (Zustand oder Attribut) reagiert.
        self.listen_state(self.on_netzbezug_change, SENSOR_NETZBEZUG_BEGRENZUNG, attribute="all")

        self.log("Listener fuer {} ist aktiv.".format(SENSOR_NETZBEZUG_BEGRENZUNG))

        self.listen_event(self.reset_ueberlast_regelung_service, event="call_service",
                          service="ems_ls_wallbox_reset_ueberlast_regelung")
        self.log("Lausche auf Service-Calls zu  ems_ls_wallbox_reset_ueberlast_regelung in HA"
                 " zum manuellen Deaktivieren der Abregelung.")

    def on_netzbezug_change(self, entity, attribute, old, new, kwargs):
        """Wird aufgerufen, wenn sich der Sensor 'sensor.ems_netzbezug_begrenzung' aendert."""
        if new is None:
            return

        new_state = new.get("state")
        new_attributes = new.get("attributes", {})
        timestamp = new_attributes.get("timestamp")

        if new_state is None or timestamp is None:
            self.log("Zustand oder Timestamp in den neuen Daten nicht gefunden. Breche ab.", level="WARNING")
            return

        try:
            leistungswert_kw = float(new_state)
        except (ValueError, TypeError):
            self.log(
                f"Ungueltiger Wert vom Sensor empfangen: '{new_state}'. Konnte nicht in float umgewandelt werden.",
                level="ERROR")
            return

        self.log(f"Aenderung erkannt: Leistung={leistungswert_kw} kW, Timestamp={timestamp}")

        if leistungswert_kw > 0:
            self.setze_ueberlast_regelung(leistungswert_kw, timestamp)
        else:
            self.reset_ueberlast_regelung(leistungswert_kw, timestamp)

    def setze_ueberlast_regelung(self, leistung_einzusparen_kw, timestamp):
        """Aktiviert die Ueberlastregelung und berechnet das neue Ladelimit."""
        self.log(f"Starte Ueberlastregelung: {leistung_einzusparen_kw} kW muessen eingespart werden.")

        war_bisher_inaktiv = self.get_state(SENSOR_WALLBOX_NETZBEGRENZUNG_AKTIV) == "off"
        self.set_state(SENSOR_WALLBOX_NETZBEGRENZUNG_AKTIV, state="on")
        self.request_id_aktuell = timestamp

        if war_bisher_inaktiv:
            charge_limit = self.get_state(SENSOR_EMS_LADESTEUERUNG_WB_LADESTEUERUNG_CHARGE_LIMIT, default=16)
            self.charge_limit_vor_abregelung = float(charge_limit)
            self.log(f"Regelung war inaktiv. Speichere aktuelles Ladelimit: {self.charge_limit_vor_abregelung} A")
            self.set_state(SENSOR_WALLBOX_NETZBEGRENZUNG_AKTIV, state="on",
                           attributes={"charge_limit_vor_abregelung": self.charge_limit_vor_abregelung})
            bestehende_reduzierung=0
        else:
            bestehende_reduzierung=int(self.get_state(SENSOR_WALLBOX_NETZBEGRENZUNG_AKTIV, attribute="wb_dyn_ladelimit_reduzieren_a", default=0))

        self.call_service("input_boolean/turn_on", entity_id=SENSOR_EMS_LADESTEUERUNG_WB_LADESTEUERUNG_DYNAMIC_CURRENT)
        self.update_wallboxes()

        if not self.wallboxes:
            self.log("Keine Wallboxen gefunden. Breche Regelung ab.", level="WARNING")
            return

        wb_leistung_aktiv_gesamt_in_kw = 0
        for sensor_id, wb_config in self.wallboxes.items():
            if wb_config.get("status") == self.WbStatus.charging:
                leistung_kw = wb_config.get("power", 0.0)
                wb_leistung_aktiv_gesamt_in_kw += leistung_kw
                self.log(f"Wallbox '{wb_config.get('name')}' laedt aktiv mit {leistung_kw} kW.")

        if wb_leistung_aktiv_gesamt_in_kw <= 0:
            self.log("Keine Wallbox laedt aktiv oder Gesamtleistung ist 0. Keine Regelung moeglich.",
                     level="WARNING")
            return

        aktuelles_limit_a = float(self.get_state(SENSOR_EMS_LADESTEUERUNG_WB_LADESTEUERUNG_CHARGE_LIMIT, default="0")) + bestehende_reduzierung;
        ladedelta = math.ceil((leistung_einzusparen_kw / wb_leistung_aktiv_gesamt_in_kw) * aktuelles_limit_a)
        ladedelta = max(ladedelta, aktuelles_limit_a)
        self.log(f"math.ceil(({leistung_einzusparen_kw} / {wb_leistung_aktiv_gesamt_in_kw}) * {aktuelles_limit_a})) = {ladedelta}")
        ladedelta_neu = bestehende_reduzierung - ladedelta
        self.log(f"Berechnetes Ladedelta: {ladedelta:.2f} -> erweitert mit bisherigem Delta {bestehende_reduzierung} auf {ladedelta_neu} A")

        self.set_state(SENSOR_WALLBOX_NETZBEGRENZUNG_AKTIV, state="on", attributes={
            "charge_limit_vor_abregelung": self.charge_limit_vor_abregelung,
            "wb_dyn_ladelimit_reduzieren_a": ladedelta_neu
        })

        self.update_dynamic_wallboxes_current()
        self.set_state(SENSOR_AUSFUEHRUNG, state=leistung_einzusparen_kw, attributes={
            "source_timestamp": self.request_id_aktuell
        })
        self.log("Ueberlastregelung erfolgreich ausgefuehrt und bestaetigt.")

    def reset_ueberlast_regelung(self, leistungswert_w, timestamp):
        """Setzt die Ueberlastregelung zurueck und hebt die Ladelimit-Reduzierung auf."""
        self._reset_ueberlast_regelung_update_currency()
        self.set_state(SENSOR_AUSFUEHRUNG, state=leistungswert_w, attributes={
            "source_timestamp": timestamp
        })
        self.log(f"Reset-Ausfuehrung bestaetigt mit Wert {leistungswert_w} und Timestamp {timestamp}.")

    def _reset_ueberlast_regelung_update_currency(self):
        self.log("Reset der Ueberlastregelung wird ausgefuehrt.")
        self.set_state(SENSOR_WALLBOX_NETZBEGRENZUNG_AKTIV, state="off", attributes={
            "wb_dyn_ladelimit_reduzieren_a": 0
        })
        self.log(f"Sensor '{SENSOR_WALLBOX_NETZBEGRENZUNG_AKTIV}' auf 'off' und Reduzierung auf 0 gesetzt.")
        self.update_dynamic_wallboxes_current()

    def reset_ueberlast_regelung_service(self, event_name, data, kwargs):
        """
        Dieser Service wird aufgerufen, um die Abregelung zurückzusetzen.
        Er setzt den Zustand des Sensors auf 'off' und das Ladelimit-Attribut auf 0.
        """

        self.log(f"Service '{event_name}' aufgerufen. Setze Abregelung fuer {SENSOR_WALLBOX_NETZBEGRENZUNG_AKTIV} zurueck.")
        self._reset_ueberlast_regelung_update_currency()

    def _get_dyn_ladebegrenzung_aus_abregelung(self):
        """
        Hilfsmethode, die das Ladelimit-Delta zurueckgibt, wenn die BezugsAbregelung aktiv ist.

        Returns:
            int: Der Wert aus dem Attribut 'wb_dyn_ladelimit_reduzieren_a' (z.B. -3)
                 oder 0, wenn die Regelung inaktiv ist.
        """
        if self.get_state(SENSOR_WALLBOX_NETZBEGRENZUNG_AKTIV) == "on":
            # self.get_entity() ist robust und gibt ein Objekt zurueck, das wir abfragen koennen
            status_sensor = self.get_entity(SENSOR_WALLBOX_NETZBEGRENZUNG_AKTIV)
            # .attributes.get() ist sicher, da es None zurueckgibt, wenn das Attribut fehlt
            reduzierung_a = status_sensor.attributes.get("wb_dyn_ladelimit_reduzieren_a", 0)
            return int(reduzierung_a)
        else:
            return 0

    def anzahl_aktiver_non_prio_wallboxes(self):
        result = 0
        for entity_id, wb_config in self.wallboxes.items():
            if wb_config.get("status") == self.WbStatus.charging and not self.check_hat_prio(wb_config):
                result += 1
        return result

    def anzahl_aktiver_non_prio_wallbox_phasen(self):

        result = 0
        for entity_id, wb_config in self.wallboxes.items():
            if wb_config.get("status") == self.WbStatus.charging and not self.check_hat_prio(wb_config):
                if self.is_charging_single_phase(wb_config):
                    result += 1
                else:
                    result += 3
        return result
