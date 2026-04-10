import ha_wrapper as hass
from datetime import datetime, time, timedelta

SENSOR_LS_NIEDRIGPREISLIMIT_IN_PROZENT = "input_number.ems_ls_niedrigpreis_preislimit_in_prozent"

SENSOR_VRM_VICTRON_PV_FORECAST_TODAY = "sensor.vrm_victron_pv_forecast_today"

SENSOR_VRM_VICTRON_CONSUMPTION_FORECAST_TODAY = "sensor.vrm_victron_consumption_forecast_today"

SENSOR_EMS_DYNAMIC_POWER_PRICE = "sensor.ems_dynamischer_strompreis"

NIEDRIGPREIS_THRESHOLD = 93
""" Gibt die Abweichung vom Durchschnittspreis an, der als würdiger Strompreis gewertet wird, um ein AC-Laden durchzuführen. """
SENSOR_EMS_BATTERIE_LADEZEIT = "sensor.ems_batterie_ladezeit"
SENSOR_EMS_LADESTEUERUNG_AKTIVE_BATTERIEN_UNTERVERSORGUNG = "sensor.ems_ladesteuerung_aktive_batterien_unterversorgung_in_w"


class PreisbewertungLaden(hass.Hass):

    def initialize(self):

        now =  self.get_now()
        quarter_hour = now.minute // 15 * 15
        next_run = now.replace(minute=quarter_hour, second=5, microsecond=0)

        if now >= next_run:
            next_run = next_run + timedelta(minutes=15)

        self.log(f"QuarterHourlyApp: erster Lauf geplant fuer {next_run.isoformat()}")
        self.run_every(self.viertelstunden_check, next_run, 900)

    def viertelstunden_check(self, kwargs):
        """Ueberprueft die Strompreise zu jeder Viertelstunde, da von Tibber mittlerweile so häufig geliefert und die der PV-Vorhersage."""
        self.log("Beginne viertelstuendlichen Preis-Check...")

        # Sensor-Daten abrufen
        try:
            price_data = self.get_state(SENSOR_EMS_DYNAMIC_POWER_PRICE, attribute="data")
            consumption_forecast = self.get_state(SENSOR_VRM_VICTRON_CONSUMPTION_FORECAST_TODAY, attribute="data")
            pv_forecast = self.get_state(SENSOR_VRM_VICTRON_PV_FORECAST_TODAY, attribute="data")
            battery_missing_capacity_in_kw = self.to_float_safe(self.get_state(SENSOR_EMS_LADESTEUERUNG_AKTIVE_BATTERIEN_UNTERVERSORGUNG)) / 1000.0

            if not price_data or 'today' not in price_data or 'hourly_prices' not in price_data['today']:
                self.log(f"Keine gueltigen Preisdaten von Sensor {SENSOR_EMS_DYNAMIC_POWER_PRICE} erhalten. Abbruch.")
                self.set_state(SENSOR_EMS_BATTERIE_LADEZEIT, state="false", attributes={"rest_stunden_ladung": 0})
                return

            if not consumption_forecast:
                self.log(
                    f"Keine gueltigen Verbrauchsdaten von Sensor {SENSOR_VRM_VICTRON_CONSUMPTION_FORECAST_TODAY} erhalten. Abbruch.")
                self.set_state(SENSOR_EMS_BATTERIE_LADEZEIT, state="false", attributes={"rest_stunden_ladung": 0})
                return

            if not pv_forecast:
                self.log(
                    f"Keine gueltigen PV-Daten von Sensor {SENSOR_VRM_VICTRON_PV_FORECAST_TODAY} erhalten. Abbruch.")
                self.set_state(SENSOR_EMS_BATTERIE_LADEZEIT, state="false", attributes={"rest_stunden_ladung": 0})
                return

            quarterhourly_prices = price_data['today']['hourly_prices']

        except Exception as e:
            self.log(f"Fehler beim Abrufen der Sensordaten: {e}")
            self.set_state(SENSOR_EMS_BATTERIE_LADEZEIT, state="false", attributes={"rest_stunden_ladung": 0})
            return

        current_quarterhour_time = self.get_quarterhour_time(datetime.now())

        aktuelle_abweichung = None
        for quarterhour_data in quarterhourly_prices:
            if datetime.fromisoformat(quarterhour_data.get('timestamp')).time() == current_quarterhour_time:
                aktuelle_abweichung = quarterhour_data.get('abweichung')
                break

        if aktuelle_abweichung is None:
            self.log("Abweichung fuer die aktuelle Stunde konnte nicht gefunden werden. Setze Sensor auf 'false'.")
            self.set_state(SENSOR_EMS_BATTERIE_LADEZEIT, state="false", attributes={"rest_stunden_ladung": 0})
            return

        if aktuelle_abweichung > self.get_niedrigpreis_threshold():
            self.log(f"Aktuelle Abweichung von {aktuelle_abweichung} ist > {self.get_niedrigpreis_threshold()}. Sensor wird auf 'false' gesetzt.")
            self.set_state(SENSOR_EMS_BATTERIE_LADEZEIT, state="false", attributes={"rest_stunden_ladung": 0})

        else:
            self.log(f"Aktuelle Abweichung von {aktuelle_abweichung} ist <= {self.get_niedrigpreis_threshold()}. Beginne PV-Check.")

            total_consumption = 0
            total_pv = 0
            guenstige_viertelstunden_count = 0

            for quarterhour_data in quarterhourly_prices:
                timestamp_hour = datetime.fromisoformat(quarterhour_data.get('timestamp')).hour
                quarterhour_time = self.get_quarterhour_time(datetime.fromisoformat(quarterhour_data.get('timestamp')))

                if quarterhour_time >= current_quarterhour_time:
                    if quarterhour_data.get('abweichung', 1.0) <= self.get_niedrigpreis_threshold():
                        guenstige_viertelstunden_count += 1

                    for consumption_hour in consumption_forecast:
                        if datetime.fromisoformat(consumption_hour.get('ts_from')).hour == timestamp_hour:
                            total_consumption += consumption_hour.get('fc_val', 0) / 4
                            break
                    for pv_hour in pv_forecast:
                        if datetime.fromisoformat(pv_hour.get('ts_from')).hour == timestamp_hour:
                            total_pv += pv_hour.get('fc_val', 0) / 4
                            break

            self.log(f"Kumulierte PV-Prognose: {total_pv} kW. Kumulierte Verbrauchs-Prognose: {total_consumption} kW.")
            lade_differenz_kw = total_consumption - total_pv + battery_missing_capacity_in_kw
            if lade_differenz_kw <= 0:
                self.log(f"PV-Prognose({total_pv}) ist groesser oder gleich Verbrauchs-Prognose ({total_consumption}) plus "
                         f"Batterie-Unterversorgung ({battery_missing_capacity_in_kw}). Sensor wird auf 'false' gesetzt.")
                self.set_state(SENSOR_EMS_BATTERIE_LADEZEIT, state="false", attributes={"rest_stunden_ladung": 0, "lade_differenz": lade_differenz_kw})

            else:
                self.log(
                    f"Verbrauchs-Prognose({total_consumption}) plus Batterie-Unterversorgung({battery_missing_capacity_in_kw}) ist "
                    f"groesser als PV-Prognose({total_pv}). Sensor bleibt auf 'true'. Lade_Differenz: {lade_differenz_kw}")
                self.set_state(SENSOR_EMS_BATTERIE_LADEZEIT, state="true", attributes={
                    "rest_stunden_ladung": guenstige_viertelstunden_count / 4,
                    "lade_differenz": lade_differenz_kw
                })

    def get_quarterhour_time(self, ts: datetime) -> time:
        """
        Rounds down the given timestamp to the nearest 15-minute increment and
        returns the time portion of the given timestamp.

        The function calculates the quarter-hour interval by dividing the minute
        value of the input timestamp by 15, flooring the result, and multiplying
        back by 15. It ensures that the resulting time is always snapped to the
        nearest lower quarter-hour mark.

        :param ts: A datetime object representing the timestamp to be adjusted.
        :type ts: datetime
        :return: A time object representing the snapped quarter-hour time.
        :rtype: time
        """
        return time(hour=ts.hour, minute=ts.minute // 15 * 15)

    def get_niedrigpreis_threshold(self):
        return self.to_float_safe(self.get_state(SENSOR_LS_NIEDRIGPREISLIMIT_IN_PROZENT), NIEDRIGPREIS_THRESHOLD) / 100.0

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
                return float(text)
        except ValueError:
            return default
