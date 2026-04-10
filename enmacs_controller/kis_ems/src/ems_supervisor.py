import ha_wrapper as hass
from datetime import datetime, timezone

# Zemperatur unter dieser Grenze wird zu einer "Vereisung" der Wärmepumpen führen
WP_VEREISUNGS_GRENZ_TEMPERATUR = 5.0


class EmsSupervisor(hass.Hass):
    """
    AppDaemon App: EMS Supervisor
    Läuft täglich um 06:00 und 18:00 Uhr und ruft control_ems_parameters() auf.
    """

    def initialize(self):
        """Wird beim Start von AppDaemon aufgerufen – registriert die täglichen Trigger."""
        self.log("EMS Supervisor gestartet. Scheduler wird eingerichtet.")

        # Täglich um 06:00 Uhr
        self.run_daily(self.run_morning, "06:00:00")
        self.log("Morgendlicher Trigger registriert: 06:00 Uhr taeglich.")

        # Täglich um 18:00 Uhr
        self.run_daily(self.run_evening, "18:00:00")
        self.log("Abendlicher Trigger registriert: 18:00 Uhr taeglich.")

    # ------------------------------------------------------------------
    # Callback-Methoden für die Scheduler-Trigger
    # ------------------------------------------------------------------

    def run_morning(self, kwargs):
        """Callback für den Morgen-Trigger (06:00 Uhr)."""
        self.log("Morgen-Trigger ausgeloest (06:00 Uhr). Starte control_ems_parameters.")
        self.control_ems_parameters(trigger="morning")

    def run_evening(self, kwargs):
        """Callback für den Abend-Trigger (18:00 Uhr)."""
        self.log("Abend-Trigger ausgeloest (18:00 Uhr). Starte control_ems_parameters.")
        self.control_ems_parameters(trigger="evening")

    # ------------------------------------------------------------------
    # Hilfs-Funktionen
    # ------------------------------------------------------------------

    def pruefe_temp_fuer_wp(self, temperatur_in_c: float) -> bool:
        """
        Prüft anhand des DWD-Temperatursensors, ob die angegebene Temperatur
        in den nächsten 12 Stunden unterschritten wird.

        Der Sensor liefert stündliche Prognosewerte im Attribut 'data':
            data:
              - datetime: "2026-02-16T05:00:00.000Z"
                value: 1.8
              - datetime: "2026-02-16T06:00:00.000Z"
                value: 2.0
              ...

        Die Entity-ID des Sensors muss in apps.yaml unter dem Schlüssel
        'dwd_temp_entity' konfiguriert sein, z. B.:
            dwd_temp_entity: sensor.STATIONSNAME_temperatur

        Args:
            temperatur_in_c: Schwellwert in °C. Wird dieser Wert in den
                             nächsten 12 Stunden unterschritten, gibt die
                             Funktion True zurück.

        Returns:
            True  – mindestens ein Prognosewert liegt < temperatur_in_c.
            False – alle Prognosewerte >= temperatur_in_c oder keine
                    verwertbaren Daten vorhanden.
        """
        dwd_entity = self.args.get("dwd_temp_entity", "sensor.STATIONSNAME_temperatur")

        # Attribut 'data' direkt abrufen
        data = self.get_state(dwd_entity, attribute="data")

        if data is None:
            self.log(
                f"pruefe_temp_fuer_wp: Attribut 'data' fuer '{dwd_entity}' nicht gefunden. "
                "Pruefe die Entity-ID in apps.yaml.",
                level="WARNING",
            )
            return False

        if not isinstance(data, list) or len(data) == 0:
            self.log(
                f"pruefe_temp_fuer_wp: Keine Prognosedaten in '{dwd_entity}' vorhanden.",
                level="WARNING",
            )
            return False

        jetzt = datetime.now(timezone.utc)
        horizont_stunden = 12
        unterschritten = False

        for eintrag in data:
            # --- Zeitstempel parsen ---
            try:
                # ISO-8601 mit 'Z'-Suffix wird ab Python 3.11 nativ unterstützt
                prognose_zeit = datetime.fromisoformat(eintrag["datetime"])
            except (KeyError, ValueError) as err:
                self.log(
                    f"pruefe_temp_fuer_wp: Ungueltiger Zeitstempel '{eintrag.get('datetime')}': {err}",
                    level="WARNING",
                )
                continue

            # Nur Einträge innerhalb des 12-Stunden-Horizonts berücksichtigen
            delta_stunden = (prognose_zeit - jetzt).total_seconds() / 3600
            if not (0 <= delta_stunden <= horizont_stunden):
                continue

            # --- Temperaturwert lesen ---
            try:
                prog_temp = float(eintrag["value"])
            except (KeyError, TypeError, ValueError) as err:
                self.log(
                    f"pruefe_temp_fuer_wp: Ungueltiger Temperaturwert in Eintrag {eintrag}: {err}",
                    level="WARNING",
                )
                continue

            self.log(
                f"pruefe_temp_fuer_wp: {prognose_zeit.strftime('%d.%m. %H:%M')} UTC "
                f"{prog_temp:.1f} C (Schwelle: {temperatur_in_c:.1f} C)",
                level="DEBUG",
            )

            if prog_temp < temperatur_in_c:
                self.log(
                    f"pruefe_temp_fuer_wp: Schwelle unterschritten. "
                    f"{prog_temp:.1f} C < {temperatur_in_c:.1f} C "
                    f"um {prognose_zeit.strftime('%d.%m. %H:%M')} UTC."
                )
                unterschritten = True
                break  # Erster Treffer genügt

        if not unterschritten:
            self.log(
                f"pruefe_temp_fuer_wp: Temperatur bleibt in den naechsten "
                f"{horizont_stunden} h bei oder ueber {temperatur_in_c:.1f} C."
            )

        return unterschritten

    # ------------------------------------------------------------------
    # Haupt-Steuerlogik
    # ------------------------------------------------------------------

    def control_ems_parameters(self, trigger: str = "manual"):
        """
        Steuert die EMS-Parameter.

        Args:
            trigger: Herkunft des Aufrufs ('morning', 'evening' oder 'manual').
        """
        self.log(f"control_ems_parameters aufgerufen (trigger={trigger}).")

        vereisungsgefahr = self.pruefe_temp_fuer_wp(WP_VEREISUNGS_GRENZ_TEMPERATUR)

        automation_id = "automation.ls_niedrigpreis_limit_temporar_anpassen"
        if vereisungsgefahr:
            self.turn_on(automation_id)
            self.log(f"Vereisungsgefahr erkannt. '{automation_id}' aktiviert.")
        else:
            self.turn_off(automation_id)
            self.log(f"Keine Vereisungsgefahr. '{automation_id}' deaktiviert.")
