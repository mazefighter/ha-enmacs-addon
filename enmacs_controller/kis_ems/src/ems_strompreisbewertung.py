import ha_wrapper as hass
from datetime import datetime
import aiohttp

SENSOR_EMS_DYNAMISCHER_STROMPREIS = "sensor.ems_dynamischer_strompreis"

class EMSStrompreisbewertung(hass.Hass):

    def initialize(self):
        """
        Diese Methode wird beim Start der App ausgefuehrt.
        Sie initialisiert die Timer und die Lock-Variable.
        """
        # Liest den zu aktualisierenden Sensor aus der Konfigurationsdatei (apps.yaml)
        self.sensor = self.args["sensor"]

        # Diese Lock-Variable verhindert, dass mehrere Anfragen gleichzeitig laufen.
        self.processing_lock = False

        self.initialize_expex_spot_prices()
        # Richtet einen taeglichen Timer ein, der um 00:05 Uhr in der lokalen Zeitzone von Home Assistant ausgefuehrt wird.
        self.log("EPEX Price Updater Started (Runs at 4am and 2pm)")
        self.run_daily(self.request_prices, "00:05:00")  # Schedule for 0:00 AM
        self.run_daily(self.request_prices, "04:00:10")  # Schedule for 4:00 AM
        self.run_daily(self.request_prices, "14:00:10")  # Schedule for 2:00 PM
        if self.get_state(self.sensor) is None:
            self.set_state(self.sensor, state="",
                           attributes={"friendly_name": "Strompreise EPEX"})

        # Ruft die Preise einmalig 5 Sekunden nach dem Start der App ab, um initiale Daten zu erhalten.
        self.run_in(self.request_prices, 5)

        self.log(f"EPEX Price Average App initialisiert. Sensor: {self.sensor} (Runs at 0am, 4am and 2pm)")

    def initialize_expex_spot_prices(self):
        """Initialisierung: Abruf des Tokens und Start des Timers."""

        self.epex_token = self.args.get('epex_token')
        self.api_url = f"https://www.energyforecast.de/api/v1/predictions/next_48_hours?fixed_cost_cent=11.93&vat=19&market_zone=DE-LU&resolution=QUARTER_HOURLY&token={self.epex_token}"

        if not self.epex_token:
            self.error("Der API-Token ist nicht konfiguriert! Die App wird nicht ausgefuehrt.")
            return

        self.log("EPEX Spot Fetcher konfiguriert.")

    async def fetch_epex_spot_prices(self) -> list:
        """
        Fetches spot prices from the EPEX SPOT API.

        This method retrieves electricity prices from the EPEX SPOT API, processes the
        data to extract and format the prices, and determines the current valid price
        based on the current time. It also updates the sensor in Home Assistant with
        all relevant information such as the current price and related attributes.

        :raises requests.exceptions.HTTPError: If an HTTP error occurs during the 
            GET request or the API responds with an error status.
        :raises requests.exceptions.RequestException: If a network-related error occurs, 
            such as connection timeout or failure.
        :raises Exception: If any other error occurs during data processing or 
            handling.

        :param self: The class instance calling this method.
        :return: A list of dictionaries where each dictionary contains the starting
            time (`start_time`) and price per kWh in eur
            (`price`) of 15-minute intervals, or an empty list if no valid
            data is available.
        :rtype: list
        """

        self.log("Starte Abruf der EPEX-Preise...")

        try:
            # Führe den HTTP GET Request aus
            api_url = self.api_url
            async with aiohttp.ClientSession() as session:
                async with session.get(api_url, timeout=aiohttp.ClientTimeout(total=15)) as response:
                    response.raise_for_status()
                    data = await response.json()

            # --- Datenverarbeitung ---
            prices_list = []

            for entry in data:

                # Der Preis ist in €/kWh (z.B. 0.27923).
                price = entry.get('price', 0.0)

                prices_list.append({
                    'start_time': entry['start'],
                    'price': round(price, 4),
                })

            if not prices_list:
                self.warning("API lieferte keine verwertbaren Preisdaten.")
                return []

            return prices_list

        except aiohttp.ClientError as e:
            self.error(f"Netzwerkfehler beim Abruf: {e}")
            return []
        except Exception as e:
            self.error(f"Unbekannter Fehler bei der Verarbeitung der EPEX-Preise: {e}")
            return []

    async def request_prices(self, kwargs=None):
        """
        Asynchronously requests and processes Epex energy spot prices. This method ensures that no
        overlapping requests are processed by utilizing a processing lock. It handles price data
        computation for today's and tomorrow's averages and updates relevant sensor attributes
        based on retrieved data. The method ensures to log every significant processing step,
        including errors, and safely releases the lock after the completion of the operation.

        :param kwargs: Optional keyword arguments for future extensibility.
        :type kwargs: dict, optional
        :return: None

        :raises ValueError: If a timestamp or price value in the retrieved price data is invalid.
        :raises TypeError: If an unexpected type is encountered in the processing of the price data.
        """
        # PRUEFUNG: Bricht ab, wenn bereits eine Anfrage in Bearbeitung ist.
        if self.processing_lock:
            self.log("Verarbeitung laeuft bereits, ueberspringe diese Anfrage.", level="INFO")
            return

        self.log("Fordere Epex-Preise an...")
        try:
            # SETZT DEN LOCK: Blockiert nachfolgende Anfragen.
            self.processing_lock = True

            # Ruft den Service auf und wartet auf die Antwort.
            price_data = await self.fetch_epex_spot_prices()

            # Ab hier wird der Code erst ausgefuehrt, wenn die Preisdaten da sind.
            if not price_data:
                self.log("Keine Rueckgabedaten vom EPEX-Price Service erhalten.", level="WARNING")
                return

            self.log("Epex-Preisdaten aus Service-Rueckgabe empfangen, verarbeite sie jetzt.")


            today = self.get_now().date()

            prices_today = []
            prices_tomorrow = []

            for price_info in price_data:
                try:
                    timestamp_str = price_info.get("start_time")
                    price_value = price_info.get("price")

                    if timestamp_str is None or price_value is None:
                        continue

                    price_date = datetime.fromisoformat(timestamp_str).date()

                    if price_date == today:
                        prices_today.append(price_value)
                    else:
                        prices_tomorrow.append(price_value)
                except (ValueError, TypeError) as e:
                    self.log(f"Konnte einen Preiseintrag nicht verarbeiten: {price_info}. Fehler: {e}", level="WARNING")
                    continue

            avg_today = self._calculate_average(prices_today)
            avg_tomorrow = self._calculate_average(prices_tomorrow)

            self.log(f"Berechneter Durchschnittspreis fuer heute: {avg_today}")
            self.log(f"Berechneter Durchschnittspreis fuer morgen: {avg_tomorrow}")

            # Ruft die asynchrone Update-Funktion auf und wartet auf deren Abschluss.
            await self._update_sensor_attributes(price_data, avg_today, avg_tomorrow)

        except Exception as e:
            self.log(f"Ein unerwarteter Fehler ist in request_prices aufgetreten: {e}", level="ERROR")
        finally:
            # GIBT DEN LOCK FREI: Dieser Block wird immer ausgefuehrt.
            self.log("Verarbeitung abgeschlossen, Lock wird freigegeben.")
            self.processing_lock = False

    async def _update_sensor_attributes(self, price_data, avg_today, avg_tomorrow):
        """
        Aktualisiert die Attribute des Ziel-Sensors.
        Bestehende Attribute (ausser 'data') bleiben erhalten.
        """
        try:
            current_state_obj = self.get_state(self.sensor, attribute="all")
            attribs = {
                'unit_of_measurement': 'eur/kWh',
                'icon': 'mdi:flash',
                'last_fetch_time': self.get_now().isoformat(),
                'friendly_name': 'Strompreise EPEX'
            }
            if current_state_obj:
                attributes = current_state_obj["attributes"].copy()
                attributes.update(attribs)
            else:
                self.log(f"Sensor {self.sensor} nicht gefunden. Er wird neu erstellt.", level="INFO")
                attributes = attribs

        except Exception as e:
            self.log(f"Fehler beim Auslesen des Sensors {self.sensor}: {e}", level="ERROR")
            return

        today_prices = []
        tomorrow_prices = []

        today = self.get_now().date()
        for price_info in price_data:
            try:
                timestamp_str = price_info.get("start_time")
                price_value = price_info.get("price")

                if timestamp_str is None or price_value is None:
                    continue

                price_date = datetime.fromisoformat(timestamp_str)

                if price_date.date() == today:
                    today_prices.append({"timestamp": price_date, "price": price_value,
                                         "abweichung": round(price_value / avg_today, 4)})
                else:
                    tomorrow_prices.append({"timestamp": price_date, "price": price_value,
                                            "abweichung": round(price_value / avg_tomorrow, 4)})
            except (ValueError, TypeError) as e:
                self.log(f"Konnte einen Preiseintrag nicht verarbeiten: {price_info}. Fehler: {e}", level="WARNING")
                continue

        new_data_attribute = {
            "data": {
                "today": {"avg_price": avg_today, "hourly_prices": today_prices},
                "tomorrow": {"avg_price": avg_tomorrow, "hourly_prices": tomorrow_prices}
            }
        }

        attributes.update(new_data_attribute)
        await self.set_state_async(self.sensor, state=avg_today, attributes=attributes)
        self.log(f"Attribute fuer {self.sensor} erfolgreich aktualisiert.")

    def _calculate_average(self, price_list):
        """
        Eine Hilfsfunktion zur Berechnung des Durchschnitts einer Zahlenliste.
        """
        if not price_list:
            return None

        average = sum(price_list) / len(price_list)
        return round(average, 5)

