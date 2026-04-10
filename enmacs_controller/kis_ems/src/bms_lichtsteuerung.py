import ha_wrapper as hass

SKRIPT_HALLE1_BELEUCHTUNG_SCHALTEN = "script/halle1_beleuchtung_schalten_flash_duplizieren"


class HalleBeleuchtungCheck(hass.Hass):

    async def initialize(self):
        """Initialisiert die App und plant die täglichen Ueberpruefungen."""

        # Geplante Ausfuehrung fuer normale Wochentage (Mo - Do) um 16:45 Uhr
        self.run_daily(self.check_all_lights, "16:45:00",
                       constrain_days = "mon,tue,wed,thu")

        # Geplante Ausfuehrung fuer Freitag um 14:15 Uhr
        self.run_daily(self.check_all_lights, "14:15:00",
                       constrain_days = "fri")

        self.list_of_lights = [
            {
                "name": "Regal 1",
                "nummer": 1,
                "sensor_leiste_1": "binary_sensor.swli_halle1_di_02",
                "sensor_leiste_2": "binary_sensor.swli_halle1_di_01",
            },
            {
                "name": "Regal 3/5/7 hinten",
                "nummer": 2,
                "sensor_leiste_1": "binary_sensor.swli_halle1_di_08",
                "sensor_leiste_2": "binary_sensor.swli_halle1_di_07",
            },
            {
                "name": "Regal 3/5/7 vorne",
                "nummer": 3,
                "sensor_leiste_1": "binary_sensor.swli_halle1_di_03",
                "sensor_leiste_2": "binary_sensor.swli_halle1_di_04",
            },
            {
                "name": "Regal 9",
                "nummer": 4,
                "sensor_leiste_1": "binary_sensor.swli_halle1_di_06",
                "sensor_leiste_2": "binary_sensor.swli_halle1_di_05",
            },
        ]
        self.log(f"Halle1 Beleuchtung Check initialisiert. {len(self.list_of_lights)} Lichter konfiguriert.")


    async def check_all_lights(self, kwargs):
        """
        Wird taeglich zur geplanten Zeit ausgefuehrt.
        Iteriert ueber alle Lichter und ruft die Pruefroutine fuer jedes Licht auf.
        """
        self.log("--- START TAeGLICHER BELEUCHTUNGSCHECK ---")

        for light_config in self.list_of_lights:
            await self.ensure_light_off(light_config)

        self.log("--- ENDE TAeGLICHER BELEUCHTUNGSCHECK ---")

    async def ensure_light_off(self, light_config):
        """
        Prueft den Status der beiden Lichtleisten und schaltet das Licht
        so oft, bis beide Leisten aus sind.
        """
        name = light_config['name']
        nummer = light_config['nummer']
        sensor_1 = light_config['sensor_leiste_1']
        sensor_2 = light_config['sensor_leiste_2']

        self.log(f"Pruefe Licht: {name} (Nummer: {nummer})")

        # Zaehler fuer die maximal moegliche Schaltzyklen (4 Zustaende)
        max_cycles = 4
        current_cycle = 0

        while current_cycle < max_cycles:
            # 1. Aktuelle Zustaende abfragen (asynchron)
            status_1 = await self.get_state(sensor_1)
            status_2 = await self.get_state(sensor_2)

            if status_1 == 'off' and status_2 == 'off':
                self.log(f"Licht {name} ist (bereits) AUS. Abbruch.")
                return # Beide Leisten sind aus, die Routine ist beendet

            # 2. Wenn nicht aus: Skript aufrufen (eine Schaltstufe weiter)
            self.log(f"Licht {name} Status: Leiste 1='{status_1}', Leiste 2='{status_2}'. Schalte weiter...")

            await self.call_service(
                "%s" % SKRIPT_HALLE1_BELEUCHTUNG_SCHALTEN,
                schalter_nummer = nummer,
                zeit_in_hs = 1
            )

            # Warten Sie eine kurze Zeit, damit das Home Assistant Skript
            # ausgefuehrt wird und die Sensoren aktualisiert werden koennen (z.B. 1 Sekunde).
            await self.sleep(1)

            current_cycle += 1

        # Falls die Schleife 4 Mal durchlaufen wurde und das Licht immer noch an ist
        if current_cycle == max_cycles:
            self.log(f"ACHTUNG: Licht {name} konnte nach {max_cycles} Versuchen nicht ausgeschaltet werden. Bitte manuell pruefen.")
