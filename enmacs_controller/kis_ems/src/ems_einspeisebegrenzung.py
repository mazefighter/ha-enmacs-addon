from datetime import datetime, timedelta
import time
import ha_wrapper as hass
import sys, os

GRID_LEISTUNG_EINSPEISUNG_IN_W_DEFAULT = "sensor.grid_leistung_einspeisung_in_w"
GRID_EINSPEISUNG_ABREGELUNG_ZUSTAND = "sensor.ems_einspeisesteuerung_zustand"
MAX_PV_LEISTUNG_DEFAULT = 3 * 17.5  # kW
UEBERWACHUNGS_INTERVALL = 60 # prüfe alle x sekunden die einspeiseleistung
FREIGABE_INTERVALL = 600 #secs

EINSPEISE_LIMIT="input_number.ems_einspeisebegrenzung_limit"
EINSPEISE_LIMIT_ZUSAETZLICHER_PUFFER=5000
PV_ANLAGE_1_2_GESAMTLEISTUNG_IN_W_DEFAULT="sensor.sensor_pv_anlage_1_2_gesamtleistung_in_w"
SCRIPT_PV_SET_LIMIT_DEFAULT = "script/froniuspvanlage1wr1setwmaxlim_pct"
SCRIPT_PV_ACTIVATE_LIMIT_DEFAULT = "script/pvanlage3wraktivierewmaxlim"

EINSPEISE_UEBERWACHUNG_SCHALTER="input_boolean.ems_einspeisebegrenzung_aktiv"
EINSPEISE_UEBERWACHUNG_SIMULATOR_SCHALTER="input_boolean.ems_einspeisebegrenzung_simulator_aktiv"
EINSPEISE_UEBERWACHUNG_SIMULATOR_LEISTUNG="input_number.ems_einspeisebegrenzung_simulator_leistung"

class EmsEinspeisebegrenzung(hass.Hass):
    def initialize(self):
        self.log("Init")
        self.adapi = self.get_ad_api()

        self.sensor_grid_einspeisung = self.args.get("sensor_grid_einspeisung", GRID_LEISTUNG_EINSPEISUNG_IN_W_DEFAULT)
        self.sensor_pv_anlage_1_2 = self.args.get("sensor_pv_anlage_1_2", PV_ANLAGE_1_2_GESAMTLEISTUNG_IN_W_DEFAULT)
        self.max_pv_leistung = float(self.args.get("max_pv_leistung_kw", MAX_PV_LEISTUNG_DEFAULT))
        self.script_pv_set_limit = self.args.get("script_pv_set_limit", SCRIPT_PV_SET_LIMIT_DEFAULT)
        self.script_pv_activate_limit = self.args.get("script_pv_activate_limit", SCRIPT_PV_ACTIVATE_LIMIT_DEFAULT)
        self.notify_targets = self.args.get("notify_targets", [])

        # einspeise abregelung: init and reset sensor
        if self.get_state(GRID_EINSPEISUNG_ABREGELUNG_ZUSTAND) is None:
            self.reset_zustand()

        self.ausfuehrung_schalter = self.get_entity(EINSPEISE_UEBERWACHUNG_SCHALTER)
        self.ausfuehrung_schalter.listen_state(self.ueberwache_ueberwachungschalter, new="off")

        self.adapi.run_in(self.ueberwache_einspeisung, 10)

    def reset_zustand(self):
        self.log("Reset Zustand")
        self.set_state(GRID_EINSPEISUNG_ABREGELUNG_ZUSTAND, state=0)
        attributes = self.get_state(GRID_EINSPEISUNG_ABREGELUNG_ZUSTAND, attribute="all").get("attributes", {})
        attributes.update({ \
            "aktiv": False, \
            "aktiv_seit": None, \
            "aktiv_bis": None, \
            "letzte_aenderung": None, \
            "betrag": 0, \
            "prozent": 0, \
            "neuestes_delta": 0, \
            "grid_einspeisung_aktuelle_leistung": 0, \
            "pv1_pv2_aktuelle_leistung": 0, \
            "pv_vert_remaining_capacity": 0 \
        })
        self.set_state(GRID_EINSPEISUNG_ABREGELUNG_ZUSTAND, state=0, attributes=attributes)

    def ueberwache_ueberwachungschalter(self, entity, attribute, old, new, **kwargs):
        self.log("reset")
        self.reset_zustand()

    def ueberwache_einspeisung(self, cb_args):
        try:
            # automatische steuerung nur wenn eingeschaltet
            if self.get_entity(EINSPEISE_UEBERWACHUNG_SCHALTER).get_state() == 'off':
                self.adapi.run_in(self.ueberwache_einspeisung, UEBERWACHUNGS_INTERVALL)
                return

            # parameter für einspeiseüberwachung
            # grid_einspeisung_max_leistung = (int(self.float_safe(self.get_entity("input_number.abregelung_ab_max_leistung").get_state())) * 1000)
            grid_einspeisung_max_leistung = (int(self.float_safe(self.get_entity(EINSPEISE_LIMIT).get_state())) * 1000)
            
            grid_einspeisung_aktuelle_leistung = -1
            try:
                grid_einspeisung_aktuelle_leistung = int(self.float_safe(self.get_max_value(self.sensor_grid_einspeisung, UEBERWACHUNGS_INTERVALL)))
            except Exception as e:
                self.log(e)
            if grid_einspeisung_aktuelle_leistung == -1:
                # fallback to most recent value (just in case something goes wrong with the historic calc)
                grid_einspeisung_aktuelle_leistung = int(self.float_safe(self.get_entity(self.sensor_grid_einspeisung).get_state()))

            if self.get_entity(EINSPEISE_UEBERWACHUNG_SIMULATOR_SCHALTER).get_state() == 'on':
                self.log("Simulierte Einspeiseleistung (anstelle der echten!) wird genommen", log_level='WARNING')
                grid_einspeisung_aktuelle_leistung = self.int_safe(self.get_entity(EINSPEISE_UEBERWACHUNG_SIMULATOR_LEISTUNG).get_state())*1000

            # hole aktuellen zustand aus entity
            zustand = self.sensor_attributes_to_state_dict(GRID_EINSPEISUNG_ABREGELUNG_ZUSTAND)

            pv_1_2_gesamtleistung = int(self.float_safe(self.get_entity(self.sensor_pv_anlage_1_2).get_state()))
            self.berechne_sollwert_einspeisung_neu(zustand, grid_einspeisung_aktuelle_leistung, grid_einspeisung_max_leistung, EINSPEISE_LIMIT_ZUSAETZLICHER_PUFFER, pv_1_2_gesamtleistung)

            self.update_sensor_attributes_from_state_dict(GRID_EINSPEISUNG_ABREGELUNG_ZUSTAND, zustand)

            aktuelle_wlb = int(self.float_safe('sensor.pv_anlage_3_wirkleistungsbegrenzung'))

            if zustand["aktiv"]:

                # irgendjemand hat von aussen die WLB gesetzt und ueberschrieben
                if aktuelle_wlb != int(self.float_safe(zustand["prozent"])):
                    # irgendjemand hat von aussen die WLB gesetzt, obwohl die automatische einspeisebegrenzung was anderes gesetzt hat
                    pass

                a = zustand["aktiv_bis"]
                if a < datetime.now().timestamp():
                    self.beende_abregelung()
                else:
                    self.log(f'Einspeisungabregelung erhoehen (+) oder zurueckregeln (-) um weitere {zustand["neuestes_delta"]} W (delta) oder um {zustand["betrag"]} W insgesamt')
                    
                    if zustand["neuestes_delta"] > 0:
                        # weitere abregelung erfolderlich
                        # self.log(f'Einspeisungabregelung erhoehen (+) oder zurueckregeln (-) um weitere {zustand["neuestes_delta"]} W (delta) oder um {zustand["betrag"]} W insgesamt')
                        # self.log(f'TODO: WR weiter abregeln um {zustand["neuestes_delta"]} W (delta), auf {zustand["prozent"]} % oder um {zustand["betrag"]} W insgesamt')
                        self.call_service(self.script_pv_set_limit, ems_wirkleistungslimit_pct=zustand["prozent"] )
                        self.call_service(self.script_pv_activate_limit, ems_wirkleistungslimit_act=1 )

                        # m = f'Einspeise-Leistung: {int(self.float_safe(grid_einspeisung_aktuelle_leistung)/1000)} kW\nEinspeise-Limit: {int(self.float_safe(grid_einspeisung_max_leistung)/1000)} kW\nNeues WR Limit: {int(self.float_safe(zustand["prozent"]))} %\nNeue WR Reduzierung: {int(self.float_safe(zustand["betrag"])/1000)} kW'
                        m = f'Einspeise-Leistung: {int(self.float_safe(grid_einspeisung_aktuelle_leistung)/1000)} kW\n' \
                            f'Einspeise-Limit: {int(self.float_safe(grid_einspeisung_max_leistung)/1000)} kW\n' \
                            f'Neues WR Limit: {int(self.float_safe(zustand["prozent"]))} %\n' \
                            f'Neue WR Reduzierung: {int(self.float_safe(zustand["betrag"])/1000)} kW\n' \
                            f'Aktiv seit: {zustand["aktiv_seit"]}\n' \
                            f'Aktiv bis: {zustand["aktiv_bis"]}\n' \
                            f'Aktuell: {time.strftime("%d.%m.%Y %H:%M", time.localtime())}'
                        self.notify_abregelung(m)
                    elif zustand["neuestes_delta"] < 0:
                        self.log(f'WR wieder hochregeln um {zustand["neuestes_delta"]} W (delta), auf {zustand["prozent"]} % oder um {zustand["betrag"]} W insgesamt')
                        if zustand["prozent"] == 100:
                            self.beende_abregelung()
                        else:
                            m = f'Einspeise-Leistung: {int(self.float_safe(grid_einspeisung_aktuelle_leistung)/1000)} kW\n' \
                                f'Einspeise-Limit: {int(self.float_safe(grid_einspeisung_max_leistung)/1000)} kW\n' \
                                f'Neues WR Limit: {int(self.float_safe(zustand["prozent"]))} %\n' \
                                f'Neue WR Reduzierung: {int(self.float_safe(zustand["betrag"])/1000)} kW\n' \
                                f'Aktiv seit: {zustand["aktiv_seit"]}\n' \
                                f'Aktiv bis: {zustand["aktiv_bis"]}\n' \
                                f'Aktuell: {time.strftime("%d.%m.%Y %H:%M", time.localtime())}'
                            self.call_service(self.script_pv_set_limit, ems_wirkleistungslimit_pct=zustand["prozent"] )
                            self.call_service(self.script_pv_activate_limit, ems_wirkleistungslimit_act=1 )
                            self.notify_hochregelung(m)

            else:
                self.log(f'Einspeisung muss nicht abregelt werden: {grid_einspeisung_aktuelle_leistung} W with {zustand}')
                if aktuelle_wlb < 100:
                    # irgendjemand hat von aussen die WLB gesetzt, obwohl die automatische einspeisebegrenzung gar nicht aktiv ist
                    pass
                
            # self.log(f'einspeiselimit neu: {grid_einspeisung_aktuelle_leistung} W with {zustand}')

        except Exception as e:
            self.log(f'enmacs_exception with grid_einspeisung_aktuelle_leistung')
            self.log(e)
            self.notify_all("Fehler in ems_einspeisebegrenzung", str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.log(f"Exception in {fname}:{exc_tb.tb_lineno} ({exc_type.__name__})", level="ERROR")

        self.adapi.run_in(self.ueberwache_einspeisung, UEBERWACHUNGS_INTERVALL)

    def berechne_sollwert_einspeisung_neu(self, zustand, grid_einspeisung_aktuelle_leistung, grid_einspeisung_max_leistung, grid_einspeisung_max_leistung_puffer, pv1_pv2_aktuelle_leistung):
        if (grid_einspeisung_max_leistung) <= (grid_einspeisung_aktuelle_leistung + grid_einspeisung_max_leistung_puffer):
            # erste oder weitere Abregelung notwendig
            # remaining_capacity = grid_einspeisung_max_leistung - pv1_pv2_aktuelle_leistung        
            pv_vert_remaining_capacity = grid_einspeisung_max_leistung - pv1_pv2_aktuelle_leistung - grid_einspeisung_max_leistung_puffer
            if pv_vert_remaining_capacity < 1:
                pv_vert_remaining_capacity = 0
            pv_vert_allowed_output = max(0.0, min((self.max_pv_leistung * 1000), pv_vert_remaining_capacity))
            pv_vert_zuviel = (self.max_pv_leistung * 1000) - pv_vert_allowed_output

            if zustand["aktiv"]:
                # abregelung schon aktiv, erhöhe abregelung
                zustand["letzte_aenderung"] = datetime.now().timestamp()
                zustand["betrag"] += pv_vert_zuviel
                zustand["pv_vert_maximal_erlaubt"] = pv_vert_allowed_output
                zustand["neuestes_delta"] = pv_vert_zuviel
                zustand["prozent"] = 100 - (zustand["betrag"] / (self.max_pv_leistung * 1000) * 100)
                if zustand["prozent"] < 2:
                    zustand["prozent"] = 2
                zustand["grid_einspeisung_aktuelle_leistung"] = grid_einspeisung_aktuelle_leistung
                zustand["pv1_pv2_aktuelle_leistung"] = pv1_pv2_aktuelle_leistung
                zustand["pv_vert_remaining_capacity"] = pv_vert_remaining_capacity
                self.log("abregelung war schon aktiv, weditere anpassung")
            else:
                # abregelung noch nicht aktiv, setze abregelung
                zustand["aktiv"] = True
                zustand["aktiv_seit"] = datetime.now().timestamp()
                zustand["aktiv_bis"] = self.endzeitpunkt_der_abregelung()
                zustand["letzte_aenderung"] = datetime.now().timestamp()
                zustand["betrag"] = pv_vert_zuviel
                zustand["pv_vert_maximal_erlaubt"] = pv_vert_allowed_output
                zustand["neuestes_delta"] = pv_vert_zuviel
                zustand["prozent"] = 100 - (zustand["betrag"] / (self.max_pv_leistung * 1000) * 100)
                if zustand["prozent"] < 2:
                    zustand["prozent"] = 2
                zustand["grid_einspeisung_aktuelle_leistung"] = grid_einspeisung_aktuelle_leistung
                zustand["pv1_pv2_aktuelle_leistung"] = pv1_pv2_aktuelle_leistung
                zustand["pv_vert_remaining_capacity"] = pv_vert_remaining_capacity
                self.log("erstmalige abregelung)")
        else:
            # schrittweise verringerung der abregelung
            zustand["neuestes_delta"] = 0

            # self.log(f'zustand["letzte_aenderung"]: {zustand["letzte_aenderung"]} | timedelta(seconds=FREIGABE_INTERVALL):{timedelta(seconds=FREIGABE_INTERVALL)}')
            if zustand["aktiv"] and (datetime.fromtimestamp(zustand["letzte_aenderung"]) + timedelta(seconds=FREIGABE_INTERVALL)).timestamp() < datetime.now().timestamp():
                # wenn schon lange genug keine Veraenderung, dann erhoehe schrittweise
                self.log("Einspeisung 30 min stabil unter Limit, gebe wieder etwas frei")
                # zustand["aktiv_seit"] = datetime.now().timestamp()
                # zustand["aktiv_bis"] = self.endzeitpunkt_der_abregelung()
                zustand["letzte_aenderung"] = datetime.now().timestamp()
                alter_betrag = zustand["betrag"]
                # halbiere die abregelung, z.b. von 2% auf 51%, von 50% auf 75%
                zustand["prozent"] = zustand["prozent"] + ((100 - zustand["prozent"]) / 2)
                if zustand["prozent"] > 90:
                    zustand["prozent"] = 100
                zustand["betrag"] = self.max_pv_leistung * (zustand["prozent"] / 100) * 1000
                zustand["neuestes_delta"] = alter_betrag - zustand["betrag"] # should be negative

                zustand["grid_einspeisung_aktuelle_leistung"] = grid_einspeisung_aktuelle_leistung
                zustand["pv1_pv2_aktuelle_leistung"] = pv1_pv2_aktuelle_leistung
                # zustand["pv_vert_maximal_erlaubt"] = pv_vert_allowed_output
                # zustand["pv_vert_remaining_capacity"] = pv_vert_remaining_capacity
            else:
                pass
                # self.log(f'Im Moment nix zu tun')

    def endzeitpunkt_der_abregelung(self):
        # differenz zwischen jetzt und nächstem sonnenuntergang geteilt durch 2
        # ==> daemlich einfache regel, um zu bestimmen, wann die abregelung wieder aufgehoben werden kann
        next_dusk = self.adapi.convert_utc(self.get_entity("sensor.sun_next_dusk").get_state())
        now2 = datetime.now().astimezone()
        delta = ((next_dusk - now2) / 2)
        aktiv_bis = now2 + delta
        return aktiv_bis.replace(tzinfo=None).timestamp()

    def beende_abregelung(self):
        # genug zeit vergangen, hebe abregelung auf
        self.log("Hebe Abregelung fuer heute wieder auf")
        self.reset_zustand()

        self.call_service(self.script_pv_set_limit, ems_wirkleistungslimit_pct=100 )
        self.call_service(self.script_pv_activate_limit, ems_wirkleistungslimit_act=1 )

        self.notify_aufhebung()                

    def notify_all(self, title: str, message: str = ""):
        """Sendet eine Benachrichtigung an alle konfigurierten Geräte."""
        for target in self.notify_targets:
            self.call_service(target, title=title, message=str(message))

    def notify_aufhebung(self):
        self.notify_all("Hebe Einspeise-Abregelung auf", "Hebe Einspeise-Abregelung auf")

    def notify_abregelung(self, m):
        self.notify_all("Erhöhe Einspeise-Abregelung", m)

    def notify_hochregelung(self, m):
        self.notify_all("Verringere Einspeise-Abregelung", m)

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
        attributes.update({"aktiv": zustand["aktiv"], \
            "aktiv_seit": zustand["aktiv_seit"], \
            "aktiv_bis": zustand["aktiv_bis"], \
            "letzte_aenderung": zustand["letzte_aenderung"], \
            "betrag": zustand["betrag"], \
            "prozent": zustand["prozent"], \
            "neuestes_delta": zustand["neuestes_delta"], \
            "grid_einspeisung_aktuelle_leistung": zustand["grid_einspeisung_aktuelle_leistung"], \
            "pv1_pv2_aktuelle_leistung": zustand["pv1_pv2_aktuelle_leistung"], \
            "pv_vert_remaining_capacity": zustand["pv_vert_remaining_capacity"]})
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
                # self.log(f"Max value of {entity_id} over last {UEBERWACHUNGS_INTERVALL} seconds: {max_value}")
            else:
                self.log(f"No numeric values found for {entity_id}")
        else:
            self.log(f"No history data found for {entity_id}")
        
        return max_value

    def float_safe(self, f):
        r = 0
        try:
            r = float(f)
        except ValueError:
            r = 0
        return r

    def int_safe(self, f):
        r = 0
        try:
            r = int(float(f))
        except ValueError:
            r = 0
        return r
