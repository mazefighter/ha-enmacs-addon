import zoneinfo

import ha_wrapper as hass
from datetime import timezone, timedelta, datetime, time

CALENDAR_WERKSFERIEN = 'calendar.werksferien'

MAX_WORKING_DAYS = 5

RUN_EVERY_SEC = 15

TIMEDELTA_CHECK_SENSORS_IN_S = 1200

# Zeitabschnitt für Glättung der Grid-Energie (min. 30 Sekunden, bzw. 2 * RUN_EVERY_SEC)
TIMEDELTA_GRID_ENERGIE_GLAETTUNG = timedelta(seconds=30)
# Zeitabschnitt für Glättung der Batterie-Energie für Batterien, die nicht über das EMS, sondern extern gesteuert werden (min. min. 30 Sekunden, bzw. 2 * RUN_EVERY_SEC)
TIMEDELTA_EXTERN_GESTEUERTE_BATTERIEN_ENERGIE_GLAETTUNG = timedelta(seconds=30)
import ha_wrapper as hass
import ha_wrapper as hass
AppDaemon = hass.Hass
import holidays


SENSOR_GRID_ENERGY_CONSUMPTION = "sensor.ems_grid_energie_bezug"
SENSOR_GRID_ENERGY_EXPORT = "sensor.ems_grid_energie_einspeisung"
SENSOR_EXTBATTERY_ENERGY_DISCHARGE = "sensor.e3dc_batterie_entladeenergie"
SENSOR_EXTBATTERY_ENERGY_CHARGE = "sensor.e3dc_batterie_ladeenergie"
SENSOR_EMS_LADESTEUERUNG_GRID_LEISTUNG_IN_W = "sensor.ems_ladesteuerung_grid_leistung_in_w"
SENSOR_EMS_LADESTEUERUNG_EXTERNAL_BATTERY_LEISTUNG_IN_W = "sensor.ems_ladesteuerung_external_battery_leistung_in_w"
SENSOR_WORKING_DAY_TOMORROW = "sensor.working_day_tomorrow"
SENSOR_WORKING_DAY_TODAY = "sensor.working_day_today"

SENSOR_EMS_LADESTEUERUNG_BATTERIE_UEBERSCHUSS_IN_W = "sensor.ems_ladesteuerung_batterie_ueberschuss_in_w"
SENSOR_EMS_LADESTEUERUNG_BATTERIE_UEBERSCHUSS_GEGLAETTET_IN_W = "sensor.ems_ladesteuerung_batterie_ueberschuss_geglaettet_in_w"
LADEUEBERSCHUSS_GLAETTUNGSWINDOW_SEC: int = 180

TIME_ZERO = time(0, 0, 0)
TIMEZONE_BERLIN = zoneinfo.ZoneInfo("Europe/Berlin")



class EmsLadesteuerung(hass.Hass):

    def initialize(self):
        self.log("Ems Ladesteuerung gestartet")
        if self.get_state(SENSOR_EMS_LADESTEUERUNG_GRID_LEISTUNG_IN_W) is None:
            self.set_state(SENSOR_EMS_LADESTEUERUNG_GRID_LEISTUNG_IN_W, state=0,
                           attributes={"unit_of_measurement": 'W', "device_class": "power", "friendly_name": "Gridleistung in W geglättet (EMS-Ladesteuerung)"})
        self.grid_export_history = []
        self.grid_import_history = []
        self.run_every(self.calculate_grid_energy, datetime.now(), RUN_EVERY_SEC)

        if self.get_state(SENSOR_EMS_LADESTEUERUNG_EXTERNAL_BATTERY_LEISTUNG_IN_W) is None:
            self.set_state(SENSOR_EMS_LADESTEUERUNG_EXTERNAL_BATTERY_LEISTUNG_IN_W, state=0,
                           attributes={"unit_of_measurement": 'W', "device_class": "power", "friendly_name": "Batterieladeleistung (extern) in W geglättet (EMS-Ladesteuerung)"})
        self.extbattery_export_history = []
        self.extbattery_import_history = []
        self.run_every(self.calculate_extbattery_energy, datetime.now(), RUN_EVERY_SEC)


        self.last_notification_check_sensors = datetime.now(timezone.utc)
        self.sensor_ids = [SENSOR_GRID_ENERGY_CONSUMPTION, SENSOR_GRID_ENERGY_EXPORT]
        #self.run_every(self.check_sensors, datetime.now(), 99)

        self.initialize_working_day_checker()
        self.initialize_ueberschuss_glaettung()


    def calculate_grid_energy(self, kwargs):
        export_value = self.get_state(SENSOR_GRID_ENERGY_EXPORT)
        import_value = self.get_state(SENSOR_GRID_ENERGY_CONSUMPTION)

        if export_value is not None and import_value is not None:
            export_value = self.to_float_safe(export_value)
            import_value = self.to_float_safe(import_value)

            now = datetime.now()
            self.grid_export_history.append((now, export_value))
            self.grid_import_history.append((now, import_value))
            vor_zeit_delta = now - TIMEDELTA_GRID_ENERGIE_GLAETTUNG
            heute_null_uhr = datetime.combine(now.date(),TIME_ZERO, tzinfo=now.tzinfo)
            self.log(f"Zaehlerreset um: {heute_null_uhr}, falls Tageszaehler für Energie", level="DEBUG")

            # Alte Daten entfernen (>5 Minuten)
            self.grid_export_history = [(time, value) for time, value in self.grid_export_history if (time >= vor_zeit_delta)]
            self.grid_import_history = [(time, value) for time, value in self.grid_import_history if (time >= vor_zeit_delta)]

            if (len(self.grid_export_history) >= 2) and (len(self.grid_import_history) >= 2):

                # Berechne Überschuss oder Grid-bezug der letzten 5 Minuten
                export_leistung = self.calculate_leistung_pro_zeitfenster(self.grid_export_history, "grid_export")
                import_leistung = self.calculate_leistung_pro_zeitfenster(self.grid_import_history, "grid_import")
                self.log(f"durchschn. Einspeisungsleistung (letzte {TIMEDELTA_GRID_ENERGIE_GLAETTUNG.total_seconds()/60} Min.): {export_leistung} W", level="DEBUG")
                self.log(f"durchschnittliche Bezugsleistung (letzte {TIMEDELTA_GRID_ENERGIE_GLAETTUNG.total_seconds()/60} Min.): {import_leistung} W", level="DEBUG")
                result = import_leistung - export_leistung
                if result >= 0:
                    self.log(f"auszugleichende Bezugsleistung: {result} W")
                else:
                    self.log(f"auszugleichende Einspeiseleistung: {abs(result)} W")

                attributes = self.get_state(SENSOR_EMS_LADESTEUERUNG_GRID_LEISTUNG_IN_W, attribute="all").get("attributes", {})
                attributes.update({"letzte_aenderung": now, "leistungsueberhang": result})
                self.set_state(SENSOR_EMS_LADESTEUERUNG_GRID_LEISTUNG_IN_W, state=result,
                               attributes=attributes)



        else:
            self.log("Grid-Energie-Sensoren liefern keine gueltigen Werte.", level="WARNING")


    def calculate_extbattery_energy(self, kwargs):
        export_value = self.get_state(SENSOR_EXTBATTERY_ENERGY_CHARGE)
        import_value = self.get_state(SENSOR_EXTBATTERY_ENERGY_DISCHARGE)

        if export_value is not None and import_value is not None:
            export_value = self.to_float_safe(export_value)
            import_value = self.to_float_safe(import_value)

            now = self.datetime().now()
            self.extbattery_export_history.append((now, export_value))
            self.extbattery_import_history.append((now, import_value))
            three_minutes_ago = now - TIMEDELTA_EXTERN_GESTEUERTE_BATTERIEN_ENERGIE_GLAETTUNG

            # Alte Daten entfernen (>3 Minuten)
            self.extbattery_export_history = [(time, value) for time, value in self.extbattery_export_history if (time >= three_minutes_ago)]
            self.extbattery_import_history = [(time, value) for time, value in self.extbattery_import_history if (time >= three_minutes_ago)]

            if (len(self.extbattery_export_history) > 1) and (len(self.extbattery_import_history) > 1):

                # Berechne Lade oder Entlade-Leistung der letzten 5 Minuten
                export_leistung = self.calculate_leistung_pro_zeitfenster(self.extbattery_export_history, "extbattery_charge")
                import_leistung = self.calculate_leistung_pro_zeitfenster(self.extbattery_import_history, "extbattery_discharge")
                self.log(f"durchschnittliche Ladeleistung (letzte {TIMEDELTA_EXTERN_GESTEUERTE_BATTERIEN_ENERGIE_GLAETTUNG.total_seconds()/60} Min.): {export_leistung} W", level="DEBUG")
                self.log(f"durchschnittliche Entladeleistung (letzte {TIMEDELTA_EXTERN_GESTEUERTE_BATTERIEN_ENERGIE_GLAETTUNG.total_seconds()/60} Min.): {import_leistung} W", level="DEBUG")
                result = export_leistung -import_leistung
                if result >= 0:
                    self.log(f"zu beruecksichtigende Ladeleistung extern gesteuerter Batterien: {result} W")
                else:
                    self.log(f"zu beruecksichtigende Entladeleistung extern gesteuerter Batterien: {abs(result)} W")

                attributes = self.get_state(SENSOR_EMS_LADESTEUERUNG_EXTERNAL_BATTERY_LEISTUNG_IN_W, attribute="all").get("attributes", {})
                attributes.update({"letzte_aenderung": now, "ladeueberhang": result})
                self.set_state(SENSOR_EMS_LADESTEUERUNG_EXTERNAL_BATTERY_LEISTUNG_IN_W, state=result,
                               attributes=attributes)



        else:
            self.log("Energie-Sensoren der extern gesteuerten Batterie liefern keine gueltigen Werte.", level="WARNING")


    def calculate_leistung_pro_zeitfenster(self, energy_history, leistung_type:str):
        """
        Berechnet die durchschnittliche Leistung des Zeitfensters
        :rtype: float
        """
        if energy_history and (len(energy_history) > 1):
            oldest_energy_data = energy_history[0]
            latest_energy_data = energy_history[-1]
            energie_differenz = (latest_energy_data[1] - oldest_energy_data[1]) * 1000 # in Watt
            if energie_differenz != 0:
                zeit_differenz_sekunden = (latest_energy_data[0] - oldest_energy_data[0]).total_seconds()
                zeit_differenz_stunden_faktor = 3600 / zeit_differenz_sekunden
                leistung = round(energie_differenz * zeit_differenz_stunden_faktor, 0)
                self.log(f"{leistung_type}: Zeitdifferenz: {zeit_differenz_sekunden}, Energiedifferenz: {energie_differenz}, Leistung: {leistung} W", level="DEBUG")
            else:
                leistung = 0
        else:
            leistung = 0
        return leistung

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



    def check_sensors(self, kwargs):
        now = datetime.now(timezone.utc)
        outdated = True
        for sensor_id in self.sensor_ids:
            last_updated = self.get_state(sensor_id, attribute="last_updated")
            if last_updated is None:
                self.log(f"Sensor {sensor_id} hat kein 'last_updated'-Attribut.", level="WARNING")
                return
            last_updated = datetime.strptime(last_updated, "%Y-%m-%dT%H:%M:%S.%f%z")
            if (now - last_updated).total_seconds() <= TIMEDELTA_CHECK_SENSORS_IN_S:  # 10 Minuten
                outdated = False
                break


        if outdated:
            if self.get_ad_api().sun_up() and (now - self.last_notification_check_sensors).total_seconds() > 3600:  # nur 1 Nachricht pro Stunde
                self.call_service("notify/persistent_notification", message=f"Die Sensoren {', '.join(self.sensor_ids)} wurden seit über 10 Minuten nicht aktualisiert.")
                self.last_notification_check_sensors = now
                self.log("Check Sensoren: Benachrichtigung gesendet.")


    def initialize_working_day_checker(self):
        """
        Handles the logic for determining whether a particular date is a working day or a holiday.

        This class is utilized within a Home Assistant environment to assess if particular dates
        (todays and tomorrows) are working days or holidays based on a specified region, subregion,
        and customizable working days of the week. It updates corresponding sensors with the
        resultant state.

        :ivar region: Code representing the country or region for determining holidays.
        :type region: str
        :ivar subregion: Code representing a specific subdivision of the region for holidays.
        :type subregion: str
        :ivar working_days: List of integers representing the weekdays considered as working days,
                            where Monday is 0 and Sunday is 6.
        :type working_days: list of int
        """
        self.region = self.args.get('region', 'DE')
        self.subregion = self.args.get('subregion', 'NW')
        self.working_days = self.args.get('working_days', [0, 1, 2, 3, 4])

        self.log(f"WorkingDayCheck gestartet: Region: {self.region}, Subregion: {self.subregion}, Working Days: {self.working_days}")

        self.run_daily(self.check_workingdays, time(0, 0, 1))
        self.get_ad_api().run_in(self.check_workingdays, 1)

    def check_workingdays(self, kwargs):
        today = datetime.now()
        tomorrow = today + timedelta(days=1)
        # Werksferienkalender
        kal_events = self.get_werkferien_events(today.date(), no_of_days=MAX_WORKING_DAYS)
        target_date_str = today.date().strftime('%Y-%m-%d')
        if kal_events and kal_events.get(target_date_str, {}).get('ist_werksferien', False):
            ferien_event_today =  kal_events.get(target_date_str, {}).get('name', 'Unknown')
            self.check_workingday(today, SENSOR_WORKING_DAY_TODAY, ferien_event = ferien_event_today)
        else:
            self.check_workingday(today, SENSOR_WORKING_DAY_TODAY)

        target_date_str = tomorrow.date().strftime('%Y-%m-%d')
        if kal_events and kal_events.get(target_date_str, {}).get('ist_werksferien', False):
            ferien_event_tom =  kal_events.get(target_date_str, {}).get('name', 'Unknown')
            self.check_workingday(tomorrow, SENSOR_WORKING_DAY_TOMORROW, ferien_event = ferien_event_tom)
        else:
            self.check_workingday(tomorrow, SENSOR_WORKING_DAY_TOMORROW)

    def check_workingday(self, check_date, sensor_id, ferien_event=None):
        """
        Checks whether the given date (`check_date`) is a working day according to predefined
        working days, holiday calendars, and specific configurations. Updates the state of
        the specified sensor with the result, which could indicate whether the date is a
        working day, a holiday, a weekend, or if there has been an error.

        :param ferien_event: Name des Kalendereintrags aus dem Werksferienkalender, falls vorhanden.
        :param check_date: The date to be checked for being a working day.
        :type check_date: datetime.datetime
        :param sensor_id: The unique identifier of the sensor to update with the check results.
        :type sensor_id: str
        :return: None
        :raises ValueError: If `check_date` is not a `datetime` object, `sensor_id` is not a valid string,
            or `self.working_days` is not a valid list of integers representing weekdays.
        :raises Exception: For any unexpected errors during the check process.
        """
        try:
            # Eingabeverifikation
            if not isinstance(check_date, datetime):
                raise ValueError("Provided 'check_date' must be a datetime object")
            if not isinstance(sensor_id, str) or not sensor_id.strip():
                raise ValueError("Sensor ID must be a non-empty string")
            if not isinstance(self.working_days, list) or not all(
                    isinstance(x, int) and 0 <= x <= 6 for x in self.working_days):
                raise ValueError("Working days must be a list of integers (0-6)")

            # Zeitzone sicherstellen (optional)
            check_date = check_date.astimezone(TIMEZONE_BERLIN).date()

            # Feiertage überprüfen
            holiday_obj = holidays.country_holidays(self.region, subdiv=self.subregion)
            holiday_name = holiday_obj.get(check_date)

            # Feiertag oder Arbeitstag bestimmen
            if holiday_name:
                self.set_state(sensor_id, state="False", attributes={"holiday_name": holiday_name})
                self.log(f"{check_date} ist Feiertag: {holiday_name}")
            elif ferien_event:
                self.set_state(sensor_id, state="False", attributes={"event_name": ferien_event})
                self.log(f"{check_date} ist in Werksferien: {ferien_event}")
            elif check_date.weekday() in self.working_days:
                self.log(f"{check_date} ist Arbeitstag")
                self.set_state(sensor_id, state="True")
            else:
                self.log(f"{check_date} ist Wochenende: {check_date.weekday()}")
                self.set_state(sensor_id, state="False")

        except Exception as e:
            self.log(f"Error checking day {check_date}: {e}", level="ERROR")
            self.set_state(sensor_id, state="ERROR")

    def get_werkferien_events(self, start_date: datetime.date, no_of_days=1):
        """
        Prüft den Kalender für den heutigen Tag und die nächsten 4 Tage auf Ganztages-Einträge, die auf Werksferien hindeuten
        Gibt ein Dictionary der Ergebnisse zurück. mit Tag als Key und Map von Attributen: ist_werksferien (bool), name (str)
        """
        # 1. Zeitrahmen definieren
        today_date = start_date

        # 2. Kalender-Events abrufen (Verwendung der AppDaemon/Hass API)
        # Hinweis: Hier wird die interne API verwendet, um alle Events im Zeitraum zu erhalten.
        try:
            events = (self.get_plugin_api("HASS")).get_calendar_events(
                entity_id=CALENDAR_WERKSFERIEN,
                days=no_of_days
            )
            self.log(f"Folgende Kalender-Events gefunden in {CALENDAR_WERKSFERIEN}: {events}")
        except Exception as e:
            self.error(f"Fehler beim Abrufen der Kalender-Events: {e}")
            return {}

        # 3. Ergebnis-Dictionary initialisieren
        result_dict = {}

        # 4. Über jeden der nächsten 5 Tage iterieren
        for i in range(no_of_days):
            target_date = today_date + timedelta(days=i)
            target_date_str = target_date.strftime('%Y-%m-%d')

            is_present = False
            event_name = ""

            # 5. Prüfen, ob ein Event den aktuellen Tag komplett abdeckt
            for event in events:
                # Konvertiere Start/Ende der Events in date-Objekte zur einfacheren Prüfung
                event_start = event['start'].date()
                event_end = event['end'].date()  # Enddatum ist exklusiv

                # Wenn das Kalender-Event als 'all_day' markiert ist,
                # muss das Startdatum <= Zieltag und das Enddatum > Zieltag sein.
                # (Home Assistant speichert Ganztages-Enddaten als den Tag nach dem letzten Ferientag)
                if event_start <= target_date and event_end > target_date:
                    is_present = True
                    event_name = event['summary']
                    break  # Sobald ein Event gefunden wurde, zum nächsten Tag gehen

            # 6. Ergebnis speichern
            result_dict[target_date_str] = {
                'ist_werksferien': is_present,
                'name': event_name
            }
        self.log(f"get_werkferien_events: {result_dict}")
        return result_dict

    def initialize_ueberschuss_glaettung(self):
        # Konfiguration aus apps.yaml auslesen
        self.window_seconds = LADEUEBERSCHUSS_GLAETTUNGSWINDOW_SEC

        # Liste zur Speicherung der Messwerte als Tupel (timestamp, value)
        self.glaettung_measurements = []

        # Listener, der auf Änderungen des Eingangssensors reagiert
        self.listen_state(self.on_new_ueberschuss_measurement, SENSOR_EMS_LADESTEUERUNG_BATTERIE_UEBERSCHUSS_IN_W)

        self.log(
            f"Glaettung der Werte fuer '{SENSOR_EMS_LADESTEUERUNG_BATTERIE_UEBERSCHUSS_IN_W}' initialisiert. "
            f"Zeitfenster: {LADEUEBERSCHUSS_GLAETTUNGSWINDOW_SEC}s."
        )

    def on_new_ueberschuss_measurement(self, entity, attribute, old, new, kwargs):
        """Wird bei jeder neuen Messung des Eingangssensors aufgerufen."""
        try:
            new_value = float(new)
        except (ValueError, TypeError):
            self.log(f"Ungueltiger Wert vom Sensor {SENSOR_EMS_LADESTEUERUNG_BATTERIE_UEBERSCHUSS_IN_W} erhalten: {new}", level="WARNING")
            return

        current_time = self.datetime(aware = True)

        # 1. Alte Messungen aus der Liste entfernen
        # Wir definieren den ältesten zulässigen Zeitstempel
        cutoff_time = current_time - timedelta(seconds=LADEUEBERSCHUSS_GLAETTUNGSWINDOW_SEC)
        # Wir behalten nur die Messungen, die neuer als der Cutoff-Zeitpunkt sind
        self.glaettung_measurements = [
            (ts, val) for ts, val in self.glaettung_measurements if ts >= cutoff_time
        ]

        # 2. Neue Messung zur Liste hinzufügen
        self.glaettung_measurements.append((current_time, new_value))

        # 3. Durchschnitt berechnen
        if not self.glaettung_measurements:
            # Sollte nicht passieren, aber sicher ist sicher
            return

        # Extrahiere nur die reinen Werte für die Berechnung
        values = [val for ts, val in self.glaettung_measurements]
        average = sum(values) / len(values)

        # 4. Zustand des neuen Sensors setzen
        self.set_state(
            SENSOR_EMS_LADESTEUERUNG_BATTERIE_UEBERSCHUSS_GEGLAETTET_IN_W,
            state=f"{average:.0f}",
            attributes={
                "friendly_name": "Geglätteter Ladeueberschuss nach Batterieladesteuerung",
                "device_class": "power",
                "unit_of_measurement": "W",
                "source_sensor": SENSOR_EMS_LADESTEUERUNG_BATTERIE_UEBERSCHUSS_IN_W,
                "measurement_count": len(self.glaettung_measurements),
                "last_updated": self.datetime(aware = True).isoformat()
            }
        )
