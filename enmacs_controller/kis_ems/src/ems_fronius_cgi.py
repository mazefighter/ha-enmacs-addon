import ha_wrapper as hass
import requests
import xml.etree.ElementTree as ET

MDI_SOLAR_POWER_VARIANT = "mdi:solar-power-variant"


# AppDaemon-Klasse zum Auslesen von Fronius CGI-Daten
class EmsFroniusCgiReader(hass.Hass):

    def initialize(self):
        """Initialisiert die App und startet den 30-Sekunden-Timer."""
        self.log(f"Fronius CGI Reader gestartet fuer URl {self.args['fronius_legacy_url']}.")

        if not self.args.get("fronius_legacy_url"):
            self.log("Keine Fronius-URL angegeben. Daten werden nicht abgerufen.", level="ERROR")
        else:
            # Führt die Methode 'read_fronius_data' alle 30 Sekunden aus.
            # Der erste Aufruf erfolgt sofort ("now").
            self.run_every(self.read_fronius_data, "now", 30)

    def read_fronius_data(self, kwargs):
        """
        Holt die Daten vom Fronius-Server, parst das XML und aktualisiert
        die Sensoren in Home Assistant.
        """
        url = self.args.get("fronius_legacy_url") + "/cgi-bin/coutFile.cgi?filename=%2Ftmp%2Fsystemdata.xml"

        try:
            # Sendet eine Anfrage an den Server mit einem Timeout von 10 Sekunden
            response = requests.get(url, timeout=10)
            # Wirft eine Ausnahme bei einem Fehler-Statuscode (z.B. 404 oder 500)
            response.raise_for_status()

            try:
                # Parst den XML-Inhalt der Antwort
                root = ET.fromstring(response.content)

                # 1. Aktuelle Leistung (PAC) auslesen und Sensor aktualisieren
                pac_element = root.find('./current/PAC')
                if (pac_element is not None
                        and pac_element.text is not None):
                    pac_value = int(pac_element.text)
                    self.log(f"Aktuelle Leistung (PAC) gefunden: {pac_value} W")

                    # Setzt den Zustand des HASS-Sensors für die Leistung
                    attributes = self.get_state("sensor.pv_direkteinspeisung_gesamt_leistung_in_w", attribute="all").get("attributes", {})
                    attributes.update({"friendly_name": "Gesamtleistung Direkteinspeisung"})
                    self.set_state("sensor.pv_direkteinspeisung_gesamt_leistung_in_w",
                                   state=pac_value, attributes=attributes)
                else:
                    self.log("Element 'PAC' nicht in der XML-Antwort gefunden.", level="WARNING")

                # 2. Gesamtenergie (TOTAL_ENERGY) auslesen und Sensor aktualisieren
                total_energy_element = root.find('./current/TOTAL_ENERGY')
                if (total_energy_element is not None
                        and total_energy_element.text is not None):
                    # Der Wert wird durch 1000 geteilt, um ihn in Wh umzurechnen
                    total_energy_wh = int(total_energy_element.text)
                    self.log(f"Gesamtenergie (TOTAL_ENERGY) gefunden: {total_energy_wh} Wh")

                    # Setzt den Zustand des HASS-Sensors für die Energie
                    attributes = self.get_state("sensor.pv_anlage_direkteinspeisung_energie_gesamt_in_wh", attribute="all").get("attributes", {})
                    self.set_state("sensor.pv_anlage_direkteinspeisung_energie_gesamt_in_wh",
                                   state=round(total_energy_wh), attributes=attributes)
                else:
                    self.log("Element 'TOTAL_ENERGY' nicht in der XML-Antwort gefunden.", level="WARNING")

                # 3. Gesamtenergie (DAY_ENERGY) auslesen und Sensor aktualisieren
                day_energy_element = root.find('./current/DAY_ENERGY')
                if (day_energy_element is not None
                        and day_energy_element.text is not None):
                    # Der Wert wird durch 1000 geteilt, um ihn in Wh umzurechnen
                    day_energy_wh = int(day_energy_element.text)
                    self.log(f"Tagesenergie (DAY_ENERGY) gefunden: {day_energy_wh} Wh")

                    # Setzt den Zustand des HASS-Sensors für die Energie
                    attributes = self.get_state("sensor.pv_direkteinspeisung_tagesenergie_in_wh", attribute="all").get("attributes", {})
                    self.set_state("sensor.pv_direkteinspeisung_tagesenergie_in_wh",
                                   state=round(day_energy_wh), attributes=attributes)
                else:
                    self.log("Element 'DAY_ENERGY' nicht in der XML-Antwort gefunden.", level="WARNING")

            except ET.ParseError as e:
                self.log(f"Fehler beim Parsen der XML-Daten: {e}", level="ERROR")
            except Exception as e:
                self.log(f"Unbekannter Fehler beim Parsen der XML-Daten: {e}", level="ERROR")

        except requests.exceptions.RequestException as e:
            self.log(
                f"Fehler bei der Abfrage des Fronius-Servers ({self.args.get('fronius_legacy_url')}): {e}",
                level="ERROR",
            )
