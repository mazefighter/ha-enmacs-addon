from datetime import datetime, timedelta
import time
import ha_wrapper as hass
import sys, os

BEZUGS_UEBERWACHUNG_AKTUELLE_LEISTUNG_DEFAULT = "sensor.grid_leistung_gesamt_in_w"
BEZUGS_UEBERWACHUNG_SIMULATOR_SCHALTER = "input_boolean.ems_netzbezug_ueberwachung_simulator_aktiv"
BEZUGS_UEBERWACHUNG_SIMULATOR_LEISTUNG = "input_number.ems_netzbezug_begrenzung_simulator_leistung"

BEZUGS_UEBERWACHUNG_ZUSTAND = "sensor.ems_netzbezug_begrenzung" 
# BEZUGS_UEBERWACHUNG_BESTAETIGUNG_ZUSTAND = "sensor.ems_netzbezug_wallbox_begrenzung_bestaetigung" 
BEZUGS_UEBERWACHUNG_AUSFUEHRUNG_ZUSTAND = "sensor.ems_netzbezug_begrenzung_ausfuehrung" 
BEZUGS_UEBERWACHUNG_SCHALTER = "input_boolean.ems_netzbezug_ueberwachung_aktiv"
BEZUGS_UEBERWACHUNG_LIMIT = "input_number.ems_netzbezug_begrenzung_limit"
BEZUGS_UEBERWACHUNG_LIMIT_ZUSAETZLICHER_PUFFER = 0 

UEBERWACHUNGS_INTERVALL = 60 # prüfe alle x sekunde den netzbezug
RESET_INTERVALL = 3600 # nehme alle x sekunden eine beschraenkung wieder raus
TIMEOUT_INTERVALL = 120 # warte x sekunden auf bestaetigung

# Default log level for this app
# Can be overridden in apps.yaml
# DEFAULT_LOG_LEVEL = "DEBUG"
DEFAULT_LOG_LEVEL = "INFO"

class EmsBezugsueberwachung(hass.Hass):
    
    last_unconfirmed_job = None
    last_confirmed_job = None
    
    def initialize(self):
        self.log("Bezugssueberwachung: Start neu")
        self.adapi = self.get_ad_api()

        self.sensor_grid_leistung = self.args.get("sensor_grid_leistung", BEZUGS_UEBERWACHUNG_AKTUELLE_LEISTUNG_DEFAULT)
        self.notify_targets = self.args.get("notify_targets", [])

        # Global log level for the app
        log_level = self.args.get("log_level", DEFAULT_LOG_LEVEL)
        self.set_log_level(log_level)
        self.log(f"Log level set to {log_level}", level="DEBUG")

        # einspeise abregelung: init and reset sensor
        if self.get_state(BEZUGS_UEBERWACHUNG_ZUSTAND) is None:
            self.reset_zustand()
        # self.log('Achtung: Zustand nach Neustart NICHT zurueckgesetzt!', log_level='WARNING')
        self.reset_zustand()
        
        self.adapi.run_in(self.ueberwache_bezug, 10)
        self.ausfuehrung = self.get_entity(BEZUGS_UEBERWACHUNG_AUSFUEHRUNG_ZUSTAND)
        self.ausfuehrung.listen_state(self.ueberwache_ausfuehrung, attribute="source_timestamp")
        self.ausfuehrung_schalter = self.get_entity(BEZUGS_UEBERWACHUNG_SCHALTER)
        self.ausfuehrung_schalter.listen_state(self.ueberwache_ueberwachungschalter, new="off")
        
    def notify_all(self, title: str, message: str = ""):
        """Sendet eine Benachrichtigung an alle konfigurierten Geräte."""
        for target in self.notify_targets:
            self.call_service(target, title=title, message=str(message))

    def reset_zustand(self):
        self.log("Reset Zustand")
        
        self.last_unconfirmed_job = None
        self.last_confirmed_job = None

        self.set_state(BEZUGS_UEBERWACHUNG_ZUSTAND, state=0)
        attributes = self.get_state(BEZUGS_UEBERWACHUNG_ZUSTAND, attribute="all").get("attributes", {})
        attributes.update({"aktiviert": False})
        attributes.update({"aktiviert_um": ""})
        attributes.update({"aktiviert_bis": ""})
        attributes.update({"timestamp": ""})
        attributes.update({"timestamp_bestaetigt": ""})
        # attributes.update({"bezug_aktuell": self.get_entity_int_safe(BEZUGS_UEBERWACHUNG_AKTUELLE_LEISTUNG)})
        attributes.update({"bezug_limit": self.get_entity_int_safe(BEZUGS_UEBERWACHUNG_LIMIT)})
        # attributes.update({"bezug_schalter": self.get_entity(BEZUGS_UEBERWACHUNG_SCHALTER).get_state()})
        self.set_state(BEZUGS_UEBERWACHUNG_ZUSTAND, state=0, attributes=attributes)

        self.set_state(BEZUGS_UEBERWACHUNG_AUSFUEHRUNG_ZUSTAND, state=0)
        attributes = self.get_state(BEZUGS_UEBERWACHUNG_AUSFUEHRUNG_ZUSTAND, attribute="all").get("attributes", {})
        attributes.update({"source_timestamp": ""})
        self.set_state(BEZUGS_UEBERWACHUNG_AUSFUEHRUNG_ZUSTAND, state=0, attributes=attributes)
        
    def ueberwache_ueberwachungschalter(self, entity, attribute, old, new, **kwargs):
        self.reset_zustand()

    def ueberwache_ausfuehrung(self, entity, attribute, old, new, **kwargs):
        if self.last_unconfirmed_job is not None:
            # bereits ein job ausstehend, prüfe ob genau dieser bestätigt wurde
            if self.last_unconfirmed_job == new:
                # genau dieser job wurde bestätigt
                self.last_confirmed_job = self.last_unconfirmed_job
                self.last_unconfirmed_job = None
                attributes = self.get_state(BEZUGS_UEBERWACHUNG_ZUSTAND, attribute="all").get("attributes", {})
                attributes.update({"timestamp_bestaetigt": self.last_confirmed_job})
                self.log(f'Wallbox-Begrenzung wurde erfolgreich aktiviert')
                self.set_state(BEZUGS_UEBERWACHUNG_ZUSTAND, attributes=attributes)
                self.notify_all("Wallbox-Begrenzung wurde erfolgreich aktiviert")
            else:
                self.notify_all("Wallbox-Steuerung hat falschen Job bestaetigt",
                    "Falscher Job bestätigt, sollte nicht vorkommen, TODO: eskalieren oder einfach nochmals versuchen?")
                self.log(f'Falscher Job bestätigt, sollte nicht vorkommen, TODO: eskalieren oder einfach nochmals versuchen?', level="WARNING")
        else:
            self.log(f'Job bestätigt, aber keiner wurde beauftragt, sollte nicht vorkommen, TODO: eskalieren oder einfach nochmals versuchen?', level="WARNING")
            self.notify_all("Wallbox-Steuerung hat nicht vorhanden Job bestaetigt",
                "Job bestätigt, aber keiner wurde beauftragt, sollte nicht vorkommen, TODO: eskalieren oder einfach nochmals versuchen?")

    def ueberwache_bezug(self, cb_args):
        try:
            # automatische steuerung nur wenn eingeschaltet
            if self.get_entity(BEZUGS_UEBERWACHUNG_SCHALTER).get_state() == 'off':
                self.log(f'Nicht aktiv', level="DEBUG")
                self.adapi.run_in(self.ueberwache_bezug, UEBERWACHUNGS_INTERVALL)
                return

            # parameter für einspeiseüberwachung
            limit = self.get_entity_int_safe(BEZUGS_UEBERWACHUNG_LIMIT)
            bezug_aktuelle_leistung_in_kw = self.get_bezug_aktuelle_leistung()
            # self.log(f'ssss {bezug_aktuelle_leistung}.   b {bezug_aktuelle_leistung/1000}')
            # bezug_aktuelle_leistung_in_kw = int(int(bezug_aktuelle_leistung) / 1000)

            self.log(f'{self.last_confirmed_job} ? {self.last_unconfirmed_job}', level="DEBUG")
            # ein job bereits aktiv, prüfe ob der schon zu lange aussteht
            if self.last_unconfirmed_job is not None:
                x_secs_ago = datetime.now().timestamp() - TIMEOUT_INTERVALL
                if self.last_unconfirmed_job < x_secs_ago:
                    # letzte ausführung ist schon länger als 60 sek her, gehe davon aus, dass sie fehlgeschlagen ist
                    self.log(f'Wallbox-Steuerung hat fuer 60 sekunden nicht bestaetigt, sende Warnung ({self.last_unconfirmed_job})', level="WARNING")
                    self.notify_all("Wallbox-Steuerung hat fuer 60 sekunden nicht bestaetigt",
                        f"Manueller Eingriff notwendig ({self.last_unconfirmed_job})")
                    self.call_service("script/notify_admins_with_label_admin_notification", 
                        message_id=f"bezugsueberwachung_wallboxsteuerung_ohne_antwort",
                        title = "Wallbox-Steuerung hat fuer 60 sekunden nicht bestaetigt", 
                        message = f"Manueller Eingriff notwendig ({self.last_unconfirmed_job})")
                    self.last_unconfirmed_job = None
                else:
                    self.log(f'Letzte Wallbox-Begrenzung noch nicht bestaetigt, warte weiter ({self.last_unconfirmed_job})')
                    self.adapi.run_in(self.ueberwache_bezug, 10)
                    return
                
            # letzte erfolgreiche begrenzung ist schon länger als eine stunde her, setze alles zurück
            one_hour_ago = datetime.now().timestamp() - RESET_INTERVALL
            if self.last_confirmed_job is not None and self.last_unconfirmed_job is None and self.last_confirmed_job < one_hour_ago:
                self.setze_neue_begrenzung(limit, bezug_aktuelle_leistung_in_kw, -1)
                self.log(f'Bezug stabil ok für über eine Stunde, zurücksetzen')
                self.adapi.run_in(self.ueberwache_bezug, UEBERWACHUNGS_INTERVALL)
                self.notify_all("Bezug stabil ok für über eine Stunde, zurücksetzen",
                    "Bezug stabil ok für über eine Stunde, zurücksetzen")
                return

            bezug_zu_viel = bezug_aktuelle_leistung_in_kw - (limit - BEZUGS_UEBERWACHUNG_LIMIT_ZUSAETZLICHER_PUFFER)
            self.log(f'bezug_aktuelle_leistung_in_kw: {bezug_aktuelle_leistung_in_kw} bezug_zu_viel: {bezug_zu_viel} limit: {limit}' , level="DEBUG" )
            if bezug_zu_viel > 0 and self.last_unconfirmed_job is None:
                # kein ausstehender job, führe neuen job aus
                # bezug zu hoch, wallbox begrenzen
                self.setze_neue_begrenzung(limit, bezug_aktuelle_leistung_in_kw, bezug_zu_viel)
                self.adapi.run_in(self.ueberwache_bezug, UEBERWACHUNGS_INTERVALL)
                self.notify_all("Aktiviere Begrenzung Strombezug",
                    f'bezug_aktuelle_leistung_in_kw: {bezug_aktuelle_leistung_in_kw}\nbezug_zu_viel: {bezug_zu_viel} \nlimit: {limit}')
                return

            # self.log('Was soll ich machen wenn Bezug ok ist? Im Moment einfach nichts...')
                
        except Exception as e:
            self.notify_all("Fehler in ems_bezugsueberwachung", str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.log(exc_type, fname, exc_tb.tb_lineno)
            self.log(f'Fehler: {e}', level="ERROR")
            # self.log_exception(e)

        self.adapi.run_in(self.ueberwache_bezug, UEBERWACHUNGS_INTERVALL)

    def setze_neue_begrenzung(self, limit, bezug_aktuelle_leistung, bezug_zu_viel):
        # self.log("setze_neue_begrenzung aktuell deaktiviert!")
        # return

        self.last_unconfirmed_job = datetime.now().timestamp() # merke timestamp des jobs
        # self.last_confirmed_job = None
        
        self.log(f'new last_unconfirmed_job {self.last_unconfirmed_job} mit {bezug_zu_viel}')
        attributes = self.get_state(BEZUGS_UEBERWACHUNG_ZUSTAND, attribute="all").get("attributes", {})
        if bezug_zu_viel == -1:
            self.log(f'Deaktiviere Wallbox-Begrenzung, da Bezug {bezug_aktuelle_leistung} kW <= Limit {limit} kW - Puffer {BEZUGS_UEBERWACHUNG_LIMIT_ZUSAETZLICHER_PUFFER} kW - bezug_zu_viel {bezug_zu_viel}')
            attributes.update({"aktiviert": False})
        else:
            self.log(f'Aktiviere Wallbox-Begrenzung, da Bezug {bezug_aktuelle_leistung} kW > Limit {limit} kW - Puffer {BEZUGS_UEBERWACHUNG_LIMIT_ZUSAETZLICHER_PUFFER} kW - bezug_zu_viel {bezug_zu_viel}')
            attributes.update({"aktiviert": True})

        attributes.update({"aktiviert_um": time.strftime("%d.%m.%Y %H:%M", time.localtime())})
        try:
            # attributes.update({"aktiviert_bis": time.strftime("%d.%m.%Y %H:%M", time.localtime() + timedelta(seconds=RESET_INTERVALL))})
            attributes.update({"aktiviert_bis": (datetime.now() + timedelta(seconds=RESET_INTERVALL)).strftime("%d.%m.%Y %H:%M")})

        except Exception as e:
            self.notify_all("Fehler in ems_bezugsueberwachung (aktiviert_bis)", str(e))
            self.log(e)
        
        attributes.update({"timestamp": self.last_unconfirmed_job})
        attributes.update({"timestamp_bestaetigt": ""})
        # attributes.update({"bezug_aktuell": bezug_aktuelle_leistung})
        attributes.update({"bezug_limit": limit})
        # attributes.update({"bezug_schalter": self.get_entity_int_safe(BEZUGS_UEBERWACHUNG_SCHALTER)})
        self.set_state(BEZUGS_UEBERWACHUNG_ZUSTAND, state=bezug_zu_viel, attributes=attributes)

    def get_bezug_aktuelle_leistung(self):
        bezug_aktuelle_leistung = -999999
        if self.get_entity(BEZUGS_UEBERWACHUNG_SIMULATOR_SCHALTER).get_state() == 'on':
            self.log("Simulierte Bezugsleistung (anstelle der echten!) wird genommen")
            bezug_aktuelle_leistung = self.get_entity_int_safe(BEZUGS_UEBERWACHUNG_SIMULATOR_LEISTUNG)
        else:
            try:
                bezug_aktuelle_leistung = self.int_safe(self.get_max_value(self.sensor_grid_leistung, 60))
                bezug_aktuelle_leistung = int(bezug_aktuelle_leistung / 1000)
            except Exception as e:
                self.notify_all("Fehler in ems_bezugsueberwachung (Leistung)", str(e))
                self.log(e)
            if bezug_aktuelle_leistung == -999999:
                # fallback to most recent value (just in case something goes wrong with the historic calc)
                bezug_aktuelle_leistung = int(self.get_entity_int_safe(self.sensor_grid_leistung) / 1000)
        self.log(f'bezug_aktuelle_leistung: {bezug_aktuelle_leistung}; {self.sensor_grid_leistung}: {self.get_max_value(self.sensor_grid_leistung, 60)}')
        return bezug_aktuelle_leistung

    def sensor_attributes_to_state_dict(self, state_sensor):
        # hole aktuellen zustand aus entity
        attributes = self.get_state(state_sensor, attribute="all").get("attributes", {})
        zustand = {}
        zustand["aktiv"] = attributes.get("aktiv")
        zustand["aktiv_seit"] = attributes.get("aktiv_seit")
        zustand["aktiv_bis"] = attributes.get("aktiv_bis")
        zustand["letzte_aenderung"] = attributes.get("letzte_aenderung")
        zustand["betrag"] = attributes.get("betrag")
        zustand["prozent"] = attributes.get("prozent")
        zustand["neuestes_delta"] = attributes.get("neuestes_delta")

        zustand["grid_einspeisung_aktuelle_leistung"] = attributes.get("grid_einspeisung_aktuelle_leistung")
        zustand["pv1_pv2_aktuelle_leistung"] = attributes.get("pv1_pv2_aktuelle_leistung")
        zustand["pv_vert_remaining_capacity"] = attributes.get("pv_vert_remaining_capacity")

        return zustand

    def update_sensor_attributes_from_state_dict(self, state_sensor, zustand):
        attributes = self.get_state(state_sensor, attribute="all").get("attributes", {})
        attributes.update({"aktiv": zustand["aktiv"], "aktiv_seit": zustand["aktiv_seit"], "aktiv_bis": zustand["aktiv_bis"], "letzte_aenderung": zustand["letzte_aenderung"], "betrag": zustand["betrag"], "prozent": zustand["prozent"], "neuestes_delta": zustand["neuestes_delta"], "grid_einspeisung_aktuelle_leistung": zustand["grid_einspeisung_aktuelle_leistung"], "pv1_pv2_aktuelle_leistung": zustand["pv1_pv2_aktuelle_leistung"], "pv_vert_remaining_capacity": zustand["pv_vert_remaining_capacity"]})
        self.set_state(state_sensor, state=0, attributes=attributes)

    def get_max_value(self, entity_id, period_in_seconds): 
        end_time = datetime.now()
        start_time = end_time - timedelta(seconds=period_in_seconds)

        max_value = -1

        # Fetch history data
        history = self.get_history(entity_id=entity_id, start_time=start_time, end_time=end_time)
        
        if history and len(history[0]) > 0:
            values = []
            for state in history[0]:
                try:
                    values.append(self.float_safe(state['state']))
                except ValueError:
                    pass  # Skip non-numeric states

            if values:
                max_value = max(values)
                # self.log(f"Max value of {entity_id} over last 90 seconds: {max_value}")
            else:
                self.log(f"No numeric values found for {entity_id}")
        else:
            self.log(f"No history data found for {entity_id}")
        
        return max_value

    @staticmethod
    def float_safe(f):
        r = 0
        try:
            r = float(f)
        except Exception:
            r = 0
        return r
    
    @staticmethod
    def int_safe(f):
        r = 0
        try:
            r = int(EmsBezugsueberwachung.float_safe(f))
        except Exception:
            r = 0
        return r
    
    def get_entity_int_safe(self, e):
        if self.get_entity(e) is None:
            self.log(f'Entity {e} not found, returning 0')
            return 0
        else:
            return EmsBezugsueberwachung.int_safe(self.get_entity(e).get_state())    
    
    def get_state_attribute(self, s, a):
        if self.get_state(s) is None:
            self.log(f'State {s} not found, returning None')
            return None
        else:
            attributes = self.get_state(s, attribute="all").get("attributes", {})
            return attributes.get(a)
        
