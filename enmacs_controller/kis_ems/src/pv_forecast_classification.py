import zoneinfo

import ha_wrapper as hass
from datetime import datetime, date, timedelta
import sys

CLASSIFICATION_HIGH = "hoch"
CLASSIFICATION_MEDIUM = "mittel"
CLASSIFICATION_LOW = "gering"

EMS_PEAK_SHAVING_SENSOR_ID = "input_boolean.ems_ladesteuerung_peak_shaving"

# Anzahl Sonensekunden im Peak-Intervall(5h um Sonnenhöchststand), ab denen das Peak-Shaving für den Tag eingeschaltet wird
SUNSHINE_THRESHOLD_SECONDS_PEAKSHAVING = 4500
# Mindest Batteriestand, ab dem das Einspeise-Peak-Shaving für den Tag eingeschaltet wird
BATTERIEN_SOC_THRESHOLD_PEAKSHAVING = 15.0
# Mindest-PV-Forecast-Klassifizierung, ab dem das Einspeise-Peak-Shaving für den Tag eingeschaltet wird
PV_FORECAST_CLASSIFICATION_THRESHOLD = CLASSIFICATION_HIGH

SENSOR_PV_FORECAST_SUNSHINE_PEAKTIME = "sensor.pv_forecast_sunshine_peaktime_in_s"

SENSOR_PV_FORECAST_CLASSIFICATION = "sensor.pv_forecast_classified_current_value"
SENSOR_PV_FORECAST_SUNSHINE_TODAY = "sensor.pv_forecast_sunshine_today_in_s"
SENSOR_PV_FORECAST_SUNSHINE_TOMORROW = "sensor.pv_forecast_sunshine_tomorrow_in_s"
SENSOR_PVFORECAST_TODAY = "sensor.vrm_victron_pv_forecast_today"
SENSOR_PVFORECAST_SUNSHINE = "sensor.schwerte_sonnenscheindauer"
SENSOR_BATTERIEN_GESAMT_SOC = "sensor.ems_ladesteuerung_aktive_batterien_soc"
SENSOR_CONSUMPTION_FORECAST_TODAY = "sensor.vrm_victron_consumption_forecast_today"
PARAMETER_PV_ERTRAG_GERING_BIS="input_number.ems_solarprognose_pv_ertrag_bis_low"
PARAMETER_PV_ERTRAG_MITTEL_BIS="input_number.ems_solarprognose_pv_ertrag_bis_mid"

class PvForecastClassifier(hass.Hass):

    def initialize(self):
        self.log("PV Forecast Bewertung init")
        self.run_daily(self.save_forecasts, "05:00:00")
        self.adapi = self.get_ad_api()
        self.adapi.run_in(self.save_forecasts, 15)
        self.listen_state(self.on_pv_forcast_change, SENSOR_PVFORECAST_TODAY)
        #self.adapi.run_in(self.calculate_peak_sunshine, 15)
        self.log("PV Forecast Bewertung gestartet")

    def get_sunrise_time(self):
        sunrise = self.get_state("sun.sun", attribute="next_rising")
        return datetime.strptime(sunrise, "%Y-%m-%dT%H:%M:%S.%f%z").astimezone(self.get_timezone())

    def get_classification_parameters(self) -> list[dict]:
        pv_ertrag_gering_bis = PvForecastClassifier.to_float_safe(self.get_state(PARAMETER_PV_ERTRAG_GERING_BIS), 0.0)
        pv_ertrag_mittel_bis = PvForecastClassifier.to_float_safe(self.get_state(PARAMETER_PV_ERTRAG_MITTEL_BIS), 200.0)
        return [
            { "classification": CLASSIFICATION_LOW, "maxvalue": pv_ertrag_gering_bis},
            { "classification": CLASSIFICATION_MEDIUM, "maxvalue": pv_ertrag_mittel_bis},
            { "classification": CLASSIFICATION_HIGH, "maxvalue": sys.float_info.max}
        ]

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


    def save_forecasts(self, kwargs):
        self.save_forecast(kwargs)
        self.save_sunshine_forecast(kwargs)
        self.calculate_peak_sunshine(kwargs)

    def on_pv_forcast_change(self, entity, attribute, old, new, kwargs):
        """Wird aufgerufen, wenn sich der Zustand des PV-Forecasts ändert."""
        if self.get_now().hour <= 10: #nur bis maximal 11Uhr aktualisieren
            self.get_ad_api().run_in(self.save_forecasts, 5)
        else:
            self.log("PV-Forecast wird nicht aktualisiert, da es nach 11 Uhr ist.")

    def save_forecast(self, kwargs):

        forecast_value = self.get_state(SENSOR_PVFORECAST_TODAY)
        result_classification = None
        if forecast_value is not None:
            classification_parameters = self.get_classification_parameters()
            for classification_parameter in classification_parameters:
                if self.to_float_safe(forecast_value) <= classification_parameter["maxvalue"]:
                    result_classification = classification_parameter["classification"]
                    break
            self.set_state("%s" % SENSOR_PV_FORECAST_CLASSIFICATION, state=result_classification,
                           attributes={"friendly_name": "PV Forecast Bewertung", "base_value": forecast_value,
                                       "next_sunrise": (self.get_sunrise_time() - timedelta(hours=1)).isoformat()})
            self.log(f"PV Forecast Bewertung gespeichert ({result_classification}): {forecast_value}")
        else:
            self.log("PV Forecast Bewertung ist None!!!", level="WARNING")

    def save_sunshine_forecast(self, kwargs):
        today = datetime.now().date()
        tomorrow = today + timedelta(days=1)
        sum_today_in_s = 0
        sum_tomorrow_in_s = 0
        attributes = self.get_state(SENSOR_PVFORECAST_SUNSHINE, attribute="all").get("attributes", {})
        if attributes is not None and attributes["data"]:
            for data_entry in attributes["data"]:
                if datetime.fromisoformat(data_entry['datetime']).date() == today:
                    sum_today_in_s += self.to_float_safe(data_entry['value'])
                if datetime.fromisoformat(data_entry['datetime']).date() == tomorrow:
                    sum_tomorrow_in_s += self.to_float_safe(data_entry['value'])
            self.save_sensor_sunshine(attributes, today, round(sum_today_in_s))
            self.save_sensor_sunshine(attributes, tomorrow, round(sum_tomorrow_in_s))
            self.log(f"PV Forecast Berechnung Sonnenscheindauer gespeichert ([{sum_today_in_s}, {sum_tomorrow_in_s}])")
        else:
            self.log("PV Forecast Bewertung Sonnenscheindauer ist None!!!", level="WARNING")

    def save_sensor_sunshine(self, attributes, day:date, sum_sunshine_in_s):
        self.set_state("%s" % SENSOR_PV_FORECAST_SUNSHINE_TODAY if day == date.today() else SENSOR_PV_FORECAST_SUNSHINE_TOMORROW,
                       state=sum_sunshine_in_s,
                       attributes={
                           "friendly_name": f"Prognose Sonnenscheindauer {'heute' if day == date.today() else 'morgen'} (DWD)",
                           "forecast_time": f"{attributes['forecast_time']}",
                           "latest_update": f"{attributes['latest_update']}",
                           "station_id": f"{attributes['station_id']}",
                           "attribution": f"{attributes['attribution']}",
                           "icon": "mdi:weather-sunset",
                           "unit_of_measurement": "s",
                       })

    def calculate_peak_sunshine(self, kwargs):
        """
        Ermittelt die Sonnenscheindauer im 5-Stunden-Intervall um den Sonnenhöchststand.
        """
        self.log("Starte Berechnung der Peak-Sonnenscheindauer...")

        try:
            # Lokale Zeitzone aus Home Assistant holen, um Konsistenz zu gewährleisten
            local_tz = self.get_timezone()

            # 1. Sonnenhöchststand für den aktuellen Tag ermitteln
            # Wir nehmen den nächsten Sonnenhöchststand, der um 4:30 Uhr immer der des aktuellen Tages ist.
            sun_state = self.get_state("sun.sun", attribute="all")
            if not sun_state or "next_noon" not in sun_state.get("attributes", {}):
                self.error("Entitaet 'sun.sun' oder Attribut 'next_noon' nicht gefunden.")
                return

            noon_time_str = sun_state["attributes"]["next_noon"]
            # Umwandlung des ISO-Strings und explizite Konvertierung in die lokale Zeitzone
            noon_time = datetime.fromisoformat(noon_time_str).astimezone(local_tz)

            # 2. Fünf-Stunden-Intervall berechnen (2.5 Stunden vor und nach dem Höchststand)
            start_window = noon_time - timedelta(hours=2.5)
            end_window = noon_time + timedelta(hours=2.5)
            if start_window and start_window.minute >= 30:
                start_window += timedelta(hours=1)
                end_window += timedelta(hours=1)
            start_window = start_window.replace(minute=0, second=0, microsecond=0)
            end_window = end_window.replace(minute=0, second=0, microsecond=0)

            self.log(
                f"Sonnenhoechststand um {noon_time.strftime(' %d.%m %H:%M:%S')}. Analysefenster (lokale Zeit): {start_window.strftime('%H:%M:%S')} - {end_window.strftime('%H:%M:%S')}")

            # 3. Daten aus dem Quellsensor auslesen
            source_sensor_id = "sensor.schwerte_sonnenscheindauer"
            source_data = self.get_state(source_sensor_id, attribute="data")

            if source_data is None:
                self.error(f"Sensor '{source_sensor_id}' oder dessen 'data'-Attribut nicht gefunden oder leer.")
                return

            if not isinstance(source_data, list):
                self.error(f"Das 'data'-Attribut von '{source_sensor_id}' ist keine Liste.")
                return

            # 4. Werte im Intervall summieren
            total_sunshine_seconds = 0
            for item in source_data:
                # Sicherstellen, dass die benötigten Schlüssel im Dictionary vorhanden sind
                if not isinstance(item, dict) or "datetime" not in item or "value" not in item:
                    self.warning(f"Ueberspringe ungueltigen Eintrag in den Sensordaten: {item}")
                    continue

                item_time_str = item["datetime"]
                sunshine_value = item["value"]

                try:
                    # Umwandlung des Zeitstempels der Daten und explizite Konvertierung in die lokale Zeitzone für den Vergleich
                    item_time = datetime.fromisoformat(item_time_str).astimezone(local_tz)

                    # Prüfen, ob der Zeitstempel im berechneten Fenster liegt
                    if start_window <= item_time < end_window:
                        total_sunshine_seconds += int(sunshine_value)
                        self.log(
                            f"  -> Addiere {sunshine_value}s fuer das Intervall beginnend um {item_time.strftime('%H:%M')}", level="INFO")
                except (ValueError, TypeError) as e:
                    self.warning(f"Konnte Eintrag nicht verarbeiten: {item}. Fehler: {e}")

            # 5. Ergebnis in einen neuen Sensor schreiben
            target_sensor_id = SENSOR_PV_FORECAST_SUNSHINE_PEAKTIME
            friendly_name = "Sonnenscheindauer Peak in Sekunden"

            self.set_state(
                target_sensor_id,
                state=total_sunshine_seconds,
                attributes={
                    "friendly_name": friendly_name,
                    "unit_of_measurement": "s",
                    "icon": "mdi:weather-sunny",
                    "peak_window_start": start_window.isoformat(),
                    "peak_window_end": end_window.isoformat(),
                    "last_updated": self.datetime().isoformat()
                }
            )
            self.log(
                f"Berechnung abgeschlossen. Gesamtsumme: {total_sunshine_seconds}s. Sensor '{target_sensor_id}' wurde aktualisiert.")

            # 6. Peak-Shaving-Schalter für den aktuellen Tag setzen
            batterien_gesamt_soc = self.to_float_safe(self.get_state(SENSOR_BATTERIEN_GESAMT_SOC), default=100)
            pv_classification = self.get_state(SENSOR_PV_FORECAST_CLASSIFICATION, default=CLASSIFICATION_LOW)
            pv_forecast_today = self.to_float_safe(self.get_state(SENSOR_PVFORECAST_TODAY), default=0.0)
            consumption_forecast_today = self.to_float_safe(self.get_state(SENSOR_CONSUMPTION_FORECAST_TODAY), default=0.0)
            pv_ueberschuss = pv_forecast_today > consumption_forecast_today
            peakshaving_sensor_id = EMS_PEAK_SHAVING_SENSOR_ID
            if total_sunshine_seconds:
                peakshaving_state = "off"
                if (total_sunshine_seconds >= SUNSHINE_THRESHOLD_SECONDS_PEAKSHAVING
                        and batterien_gesamt_soc >= BATTERIEN_SOC_THRESHOLD_PEAKSHAVING
                        and pv_classification == PV_FORECAST_CLASSIFICATION_THRESHOLD
                        and pv_ueberschuss):
                    peakshaving_state = "on"
                self.log(
                    f"Sonnenscheindauer ist {total_sunshine_seconds}s, Batterie-SOC: ({batterien_gesamt_soc}/ Schwellwert:{BATTERIEN_SOC_THRESHOLD_PEAKSHAVING}), "
                    f"Klassifizierung: {pv_classification}/ Schwelle: {PV_FORECAST_CLASSIFICATION_THRESHOLD}, "
                    f"PV-Prognose heute: {pv_forecast_today} > Verbrauchsprognose heute: {consumption_forecast_today} = {pv_ueberschuss}, "
                    f"daher wird der Sensor '{peakshaving_sensor_id}' auf '{peakshaving_state}' aktualisiert (Schwellwert: {SUNSHINE_THRESHOLD_SECONDS_PEAKSHAVING}s).",
                    level="INFO")
                self.set_state(peakshaving_sensor_id, state=peakshaving_state)
            else:
                self.log(f"Sonnenscheindauer ist nicht gesetzt, daher wird der Sensor '{peakshaving_sensor_id}' nicht aktualisiert.", level="WARNING")


        except Exception as e:
            self.error(f"Ein unerwarteter Fehler ist aufgetreten: {e}", stack_info=True)

