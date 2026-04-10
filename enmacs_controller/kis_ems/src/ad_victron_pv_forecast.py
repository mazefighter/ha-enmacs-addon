import uuid
import zoneinfo

import ha_wrapper as hass
from datetime import time, datetime, timedelta
import requests

PVFORECAST_TODAY = "sensor.vrm_victron_pv_forecast_today"
PVFORECAST_TOMORROW = "sensor.vrm_victron_pv_forecast_tomorrow"
CONSUMPTIONFORECAST_TODAY = "sensor.vrm_victron_consumption_forecast_today"
CONSUMPTIONFORECAST_TOMORROW = "sensor.vrm_victron_consumption_forecast_tomorrow"

TIMEZONE_BERLIN = zoneinfo.ZoneInfo("Europe/Berlin")


VICTRONENERGY_VRN_BASE_URL = "https://vrmapi.victronenergy.com/v2"

NDIGITS = 2

class AdVictronPvForecast(hass.Hass):
    """
    Manages the retrieval, processing, and storage of photovoltaic (PV) forecast data
    from the Victron VRM API.

    This class interacts with the Victron VRM API to fetch hourly forecast data for
    solar yield over the next two days. It processes this data into a structured format
    and stores it in Home Assistant state variables for further use. This class also supports
    periodic automation to continuously fetch and update forecast data.

    :ivar adapi: Reference to the AppDaemon API instance for task scheduling and automation.
    :type adapi: hass.Hass
    :ivar location_id: The location identifier used for fetching the solar forecast related to a specific site.
    :type location_id: str
    """
    def initialize(self):
        self.log("AdVictronPvForecast init")
        self.adapi = self.get_ad_api()
        self.location_id = self.args["location_id"]

        for sensor_name in [PVFORECAST_TODAY, PVFORECAST_TOMORROW, CONSUMPTIONFORECAST_TODAY, CONSUMPTIONFORECAST_TOMORROW]:
            if self.get_state(sensor_name) is None:
                self.set_state(sensor_name, state=0,
                               attributes={"unique_id": str(uuid.uuid4()), "unit_of_measurement": 'kW',
                                           "device_class": "power", "state_class": "measurement"})
        now =  self.get_now()
        half_hour = now.minute // 30 * 30
        next_run = now.replace(minute=half_hour, second=1, microsecond=0)

        if now >= next_run:
            next_run = next_run + timedelta(minutes=30)

        self.log(f"AdVictronPvForecast: erster Lauf geplant fuer {next_run.isoformat()} danach halbstuendlich")
        self.run_every(self.fetch_ess_forecast, next_run, 1800)

        if next_run > (now + timedelta(minutes=2)):
            self.log("AdVictronPvForecast: wird zusaetzlich noch direkt gestartet")
            self.get_ad_api().run_in(self.fetch_ess_forecast, 5)


    def get_victron_ess_forecast(self, auth_token):
        """
        Fetches the Victron ESS forecast for a specified location using the provided
        authentication token. The method retrieves a dynamic energy storage system
        forecast from the VictronEnergy API, including solar energy yield forecasts
        and consumption forecasts, processes the data, and converts it into a
        Home Assistant (HA) compatible result.

        The retrieved forecast spans a time period of 48 hours from the current day,
        aggregated at an hourly interval.

        :param auth_token: Authentication token used to authorize the request to the
            VictronEnergy API.
        :type auth_token: str
        :return: A dictionary containing processed solar and consumption forecasts,
            or None if any error occurs during retrieval or processing.
        :rtype: dict or None
        """

        base_url = VICTRONENERGY_VRN_BASE_URL
        forecast_url = f"{base_url}/installations/{self.location_id}/stats"

        now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = now + timedelta(days=2)
        now_time = datetime.now(tz=TIMEZONE_BERLIN).time()

        params = {
            "type": "dynamic_ess",
            "interval": "hours",
            "start": round(now.timestamp()),
            "end": round(end_dt.timestamp()),
        }

        headers = {
            "X-Authorization": f"Bearer {auth_token}",
        }

        try:
            response = requests.get(forecast_url, headers=headers, params=params)
            response.raise_for_status()
            forecast_data = response.json()
            pv_fc_ha_result = None
            pv_ha_result =[]
            consumption_fc_ha_result = None
            consumption_ha_result = None
            if (forecast_data["records"]["solar_yield_forecast"]):
                solar_forecast = forecast_data["records"]["solar_yield_forecast"]
                pv_fc_ha_result = self.convert_json_result_to_ha_result(solar_forecast, True, now_time)
            if forecast_data["records"]["total_solar_yield"] and not isinstance(forecast_data["records"]["total_solar_yield"], bool):
                solar_yield_data = forecast_data["records"]["total_solar_yield"]
                pv_ha_result = self.convert_json_result_to_ha_result(solar_yield_data, False, now_time, value_scale=1)
            if (forecast_data["records"]["vrm_consumption_fc"]):
                consumption_forecast = forecast_data["records"]["vrm_consumption_fc"]
                consumption_fc_ha_result = self.convert_json_result_to_ha_result(consumption_forecast, True, now_time)
            if (forecast_data["records"]["total_consumption"]):
                consumption_data = forecast_data["records"]["total_consumption"]
                consumption_ha_result = self.convert_json_result_to_ha_result(consumption_data, False, now_time,
                                                                              value_scale=1)
            return {
                "solar_forecast": pv_fc_ha_result,
                "solar_yield": pv_ha_result,
                "consumption_forecast": consumption_fc_ha_result,
                "consumption": consumption_ha_result
            }
        except requests.exceptions.RequestException as e:
            self.error(f"Fehler beim Abrufen des Forecasts: {e}")
            return None

    # Beispielhafte Verwendung
    def convert_json_result_to_ha_result(self, data_hourly, is_forecast_data, now_time, timeval_scale=1000,
                                         value_scale=1000):
        """
        Converts a JSON result of PV forecast data into a format suitable
        for Home Assistant. This method processes the provided PV
        forecast data and transforms it into a structured result containing
        forecast entries with timestamp and rounded power values.

        :param is_forecast_data: True if the forecast data is to be processed, False for
        :param data_hourly: A list of PV forecast data entries, where
            each entry contains a timestamp (in milliseconds) and a power
            value (in watts).
        :param now_time: current time to calculate forecast sums or total sums for non forecast data
        :type data_hourly: List[List[Union[int, float]]]
        :return: A PVForecastResult object containing transformed forecast
            entries with timestamp and rounded power values.
        :rtype: AdVictronPvForecast.ForecastResult
        """
        if (data_hourly):
            result = []
            for forecast_entry in data_hourly:
                list_entry = AdVictronPvForecast.ForecastEntry(
                    datetime.fromtimestamp(forecast_entry[0] / timeval_scale, tz=TIMEZONE_BERLIN),
                    round(forecast_entry[1] / value_scale, NDIGITS)
                )
                result.append(list_entry)
            return AdVictronPvForecast.ForecastResult(result, now_time, is_forecast_data)

    def fetch_ess_forecast(self, kwargs):
        token = self.args["token"]
        result = self.get_victron_ess_forecast(token)
        if not result:
            self.error("Keine Forecast-Daten vom Victron API erhalten.")
            return
        if result["solar_forecast"].daily_entries:
            attributes = self.get_state(PVFORECAST_TODAY, attribute="all").get("attributes", {})
            attributes.update({"data": result["solar_forecast"].daily_entries[0]["entries"]})
            sum_fc_today = result["solar_forecast"].daily_sums[0]["fc_sum"]
            if result["solar_yield"].daily_entries:
                self.log("fetch pv_yield inst_id({}), sum result: {}".format(self.location_id,
                                                                                result["solar_yield"].daily_sums))
                attributes.update({"yield_data": result["solar_yield"].daily_entries[0]["entries"]})
                sum_fc_today += result["solar_yield"].daily_sums[0]["fc_sum"]
            self.log("fetch pv_forecast inst_id({}), sum result: {}".format(self.location_id, sum_fc_today))
            self.set_state(PVFORECAST_TODAY, state=round(sum_fc_today), attributes=attributes)
            attributes = self.get_state(PVFORECAST_TOMORROW, attribute="all").get("attributes", {})
            attributes.update({"data": result["solar_forecast"].daily_entries[1]["entries"]})
            self.set_state(PVFORECAST_TOMORROW, state=round(result["solar_forecast"].daily_sums[1]["fc_sum"]),
                attributes=attributes)
        else:
            self.error("no solar_forecast result from victron vrm server")

        if result["consumption_forecast"].daily_entries:
            attributes = self.get_state(CONSUMPTIONFORECAST_TODAY, attribute="all").get("attributes", {})
            attributes.update({"data": result["consumption_forecast"].daily_entries[0]["entries"]})
            sum_fc_today = result["consumption_forecast"].daily_sums[0]["fc_sum"]
            if result["consumption"].daily_entries:
                self.log("fetch consumption inst_id({}), sum result: {}".format(self.location_id,
                                                                                result["consumption"].daily_sums))
                attributes.update({"consumption_data": result["consumption"].daily_entries[0]["entries"]})
                sum_fc_today += result["consumption"].daily_sums[0]["fc_sum"]
            self.log("fetch consumption_forecast inst_id({}), sum result: {}".format(self.location_id, sum_fc_today))
            self.set_state(CONSUMPTIONFORECAST_TODAY, state=round(sum_fc_today),
                           attributes=attributes)
            attributes = self.get_state(CONSUMPTIONFORECAST_TOMORROW, attribute="all").get("attributes", {})
            attributes.update({"data": result["consumption_forecast"].daily_entries[1]["entries"]})
            self.set_state(CONSUMPTIONFORECAST_TOMORROW, state=round(result["consumption_forecast"].daily_sums[1]["fc_sum"]),
                           attributes=attributes)
        else:
            self.error("no consumption_forecast result from victron vrm server")

    class ForecastEntry:
        def __init__(self, timestamp_from, forecast_value):
            self.ts_from = timestamp_from
            self.fc_val = forecast_value

        """
        Initialisiert die PvForecastEntry-Klasse.

        Args:
            timestamp_from (timestamp): Zeitpunkt, ab dem der stündliche Vorhersagewert gilt
            forecast_value (float): Vorhersagewert in 
        """

    class ForecastResult:
        def __init__(self, daily_entries_list, now_time:time, is_forecast_data=True):
            """
            Initializes the class instance by processing the provided daily entries list, generating
            daily sums and organizing entries for each day. It computes and stores the sums of values
            and lists of entries grouped by day, formatted as dictionaries. These results are sorted
            chronologically by day. Optionally stores whether the data is forecast data.

            :param daily_entries_list: A list of daily entry objects to process. Each entry object
                should contain the attributes `ts_from` (a datetime object indicating the timestamp)
                and `fc_val` (a numerical value to sum).
            :param now_time: A datetime object representing the current timestamp, although it is
                not directly used in this function.
            :param is_forecast_data: A boolean value indicating whether the provided data is forecast
                data. Defaults to True.
            """
            self.is_forecast_data = is_forecast_data

            day_entries = {} #Dictionary, um die Listen pro Tag zu speichern
            for entry in daily_entries_list:
                day = entry.ts_from.date()
                if day in day_entries:
                    day_entries[day].append(object_to_dict(entry))
                else:
                    day_entries[day] = [object_to_dict(entry)]

            # Konvertiere das Dictionary in eine Liste von Dictionaries
            day_list_ergebnis = [{'day': day, 'entries': daylist} for day, daylist in day_entries.items()]

            # Sortiere die Ergebnisse nach Datum
            day_list_ergebnis.sort(key=lambda x: x['day'])
            self.daily_entries = day_list_ergebnis

            day_sums = {}  # Dictionary, um Summen pro Tag zu speichern

            for entry in daily_entries_list:
                day = entry.ts_from.date()
                if day not in day_sums:
                    day_sums[day] = 0.0

                # Betrachte Uhrzeit nur für den ersten Tag der Prognose, da nur da Istdaten vorliegen können
                if day == day_list_ergebnis[0]['day']:
                    if is_forecast_data:
                        if now_time < entry.ts_from.time():
                            day_sums[day] += entry.fc_val
                        elif now_time.hour == entry.ts_from.time().hour:
                            # Forecast nur anteilig in Summe übernehmen
                            day_sums[day] += entry.fc_val * ((60 - now_time.minute) / 60)
                    else:
                        if now_time > entry.ts_from.time():
                            day_sums[day] += entry.fc_val
                else:
                    day_sums[day] += entry.fc_val

            sum_ergebnis = [{'day': day, 'fc_sum': round(fc_sum, NDIGITS)} for day, fc_sum in day_sums.items()]
            self.daily_sums = sum_ergebnis
            sum_ergebnis.sort(key=lambda x: x['day'])

def object_to_dict(obj):
    """
    Converts an object to a dictionary.
    Args:
        obj: The object to convert.

    Returns:
        A dictionary representation of the object.
    """
    if hasattr(obj, "__dict__"):
        return obj.__dict__
    else:
        # if the object does not have a __dict__ attribute, try vars()
        try:
            return vars(obj)
        except TypeError:
            # if vars() also fails, the object is likely not a standard object.
            return str(obj)  # return string representation of the object.
