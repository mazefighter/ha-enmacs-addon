import ha_wrapper as hass
from datetime import datetime, timezone, timedelta
import requests

# ---------------------------------------------------------------------------
# Sensor-Konfiguration der beiden WP-Cluster
# ---------------------------------------------------------------------------
CLUSTERS = {
    "cube20": [
        "sensor.wolfcube20_cha_0x3_270009_ruecklauftemperatur",
        "sensor.wolfcube20_cha_0x13_270009_ruecklauftemperatur",
    ],
    "cube30": [
        "sensor.wolfcube30_cha_0x3_270009_ruecklauftemperatur",
        "sensor.wolfcube30_cha_0x13_270009_ruecklauftemperatur",
        "sensor.wolfcube30_cha_0x33_270009_ruecklauftemperatur",
    ],
}

# Defaults (ueberschreibbar per apps.yaml)
DEFAULT_STALE_MINUTES   = 10
DEFAULT_COOLDOWN_MINUTES = 5
DEFAULT_CHECK_INTERVAL   = 120   # Sekunden

WOLFLINK_REBOOT_PATH = "/protect/reboot.htm"


class ControlIsm7(hass.Hass):
    """
    Ueberwacht die WP-Cluster-Sensoren cube20 und cube30 auf fehlende Updates.

    Logik:
    - Alle CHECK_INTERVAL Sekunden wird das last_updated jedes Sensors geprueft.
    - Sensoren, die laenger als STALE_MINUTES kein Update erhalten haben, gelten
      als 'stale' und werden pro Cluster erfasst.
    - Sind ALLE Sensoren eines Clusters stale, wird das zugehoerige WolfLink-Pro-Modul
      per REST-Call neu gestartet (POST /protect/reboot.htm mit Basic-Auth).
    - Sind nur einzelne Sensoren eines Clusters stale, wird das ISM7MQTT-Add-on
      neu gestartet.
    - Sind ALLE Sensoren aller Cluster gleichzeitig stale, werden alle WolfLink-Pro-
      Module neu gestartet (kein Add-on-Neustart - deutet auf Hardware-Problem hin).
    - Pro Cluster gilt ein eigener Cooldown fuer WolfLink-Neustarts.
    - Fuer den Add-on-Neustart gilt ein globaler Cooldown.

    apps.yaml-Konfiguration:
        control_ism7:
          module: control_ism7
          class: ControlIsm7
          addon_slug: "bf7963f0_ism7mqtt-experimental"
          stale_threshold_minutes: 10
          restart_cooldown_minutes: 5
          check_interval_seconds: 120
          wolflink_modules:
            cube20:
              ip: "172.16.20.139"
              user: "admin"
              password: "geheim"
            cube30:
              ip: "172.16.20.140"
              user: "admin"
              password: "geheim"
    """

    def initialize(self):
        self.addon_slug      = self.args.get("addon_slug", "ism7mqtt")
        self.stale_threshold = timedelta(minutes=self.args.get("stale_threshold_minutes", DEFAULT_STALE_MINUTES))
        self.cooldown        = timedelta(minutes=self.args.get("restart_cooldown_minutes", DEFAULT_COOLDOWN_MINUTES))
        check_interval       = self.args.get("check_interval_seconds", DEFAULT_CHECK_INTERVAL)

        # WolfLink-Modul-Konfiguration je Cluster
        # HA-Addon-Config liefert eine Liste von Objekten; wir konvertieren in ein Dict {name: cfg}
        wolflink_raw = self.args.get("wolflink_modules", {})
        if isinstance(wolflink_raw, list):
            self.wolflink_modules: dict[str, dict] = {m["name"]: m for m in wolflink_raw if "name" in m}
        else:
            self.wolflink_modules: dict[str, dict] = wolflink_raw  # Fallback für altes Dict-Format

        # Letzter Neustart-Zeitstempel: global (Addon) und pro Cluster (WolfLink)
        self.last_addon_restart_dt: datetime | None = None
        self.last_wolflink_restart_dt: dict[str, datetime | None] = {c: None for c in CLUSTERS}

        self.run_every(self.check_sensors, "now", check_interval)
        self.log(
            f"ControlIsm7 gestartet - Addon: '{self.addon_slug}', "
            f"Stale-Schwelle: {self.stale_threshold}, "
            f"Cooldown: {self.cooldown}, "
            f"Pruefintervall: {check_interval}s, "
            f"WolfLink-Module konfiguriert: {list(self.wolflink_modules.keys())}"
        )

    # ------------------------------------------------------------------
    # Haupt-Prufroutine
    # ------------------------------------------------------------------

    def check_sensors(self, kwargs):
        """Prueft alle Cluster-Sensoren und leitet bei Bedarf Neustarts ein."""
        now_utc = datetime.now(timezone.utc)

        # Stale-Sensoren je Cluster ermitteln
        stale_by_cluster: dict[str, list[str]] = {}
        for cluster, sensors in CLUSTERS.items():
            stale = [s for s in sensors if self._is_stale(s, now_utc)]
            if stale:
                stale_by_cluster[cluster] = stale

        if not stale_by_cluster:
            self.log("Alle WP-Sensoren aktuell - kein Handlungsbedarf.")
            return

        # Stale-Sensoren ausgeben
        for cluster, stale_sensors in stale_by_cluster.items():
            total = len(CLUSTERS[cluster])
            self.log(
                f"[{cluster}] {len(stale_sensors)}/{total} Sensoren ohne Update "
                f"seit > {int(self.stale_threshold.total_seconds() // 60)} min: "
                f"{stale_sensors}",
                level="WARNING"
            )

        # Cluster klassifizieren: komplett stale vs. teilweise stale
        fully_stale_clusters   = [c for c, sensors in CLUSTERS.items()
                                   if set(stale_by_cluster.get(c, [])) == set(sensors)]
        partial_stale_clusters = [c for c in stale_by_cluster if c not in fully_stale_clusters]

        all_clusters_fully_stale = len(fully_stale_clusters) == len(CLUSTERS)

        # WolfLink-Neustarts fuer komplett stale Cluster
        for cluster in fully_stale_clusters:
            self._maybe_restart_wolflink(cluster, stale_by_cluster[cluster], now_utc)

        # ISM7MQTT Add-on nur bei partiell stalen Clustern neu starten
        # (bei Totalausfall uebernehmen die WolfLink-Neustarts)
        if partial_stale_clusters and not all_clusters_fully_stale:
            partial_stale_sensors = {c: stale_by_cluster[c] for c in partial_stale_clusters}
            self._maybe_restart_addon(partial_stale_sensors, now_utc)
        elif all_clusters_fully_stale:
            self.log(
                "TOTALAUSFALL: Alle Sensoren aller Cluster sind stale. "
                "WolfLink-Neustarts wurden eingeleitet - kein Add-on-Neustart.",
                level="ERROR"
            )

    # ------------------------------------------------------------------
    # Neustart-Methoden
    # ------------------------------------------------------------------

    def _maybe_restart_wolflink(self, cluster: str, stale_sensors: list[str], now_utc: datetime):
        """Startet das WolfLink-Pro-Modul des Clusters neu, wenn Cooldown abgelaufen."""
        module_cfg = self.wolflink_modules.get(cluster)
        if not module_cfg:
            self.log(
                f"[{cluster}] Alle Sensoren stale, aber kein WolfLink-Modul konfiguriert. "
                f"Ueberspringe WolfLink-Neustart.",
                level="WARNING"
            )
            return

        last_dt = self.last_wolflink_restart_dt.get(cluster)
        if last_dt is not None:
            elapsed = now_utc - last_dt
            if elapsed < self.cooldown:
                remaining = int((self.cooldown - elapsed).total_seconds())
                self.log(
                    f"[{cluster}] WolfLink-Cooldown aktiv (letzter Neustart vor "
                    f"{int(elapsed.total_seconds())}s) - naechster Neustart in {remaining}s.",
                    level="INFO"
                )
                return

        self._restart_wolflink(cluster, module_cfg, stale_sensors, now_utc)

    def _restart_wolflink(self, cluster: str, module_cfg: dict,
                          stale_sensors: list[str], now_utc: datetime):
        """Fuehrt den REST-Reboot-Call gegen das WolfLink-Pro-Modul durch."""
        ip       = module_cfg.get("ip", "")
        user     = module_cfg.get("user", "admin")
        password = module_cfg.get("password", "")
        url      = f"http://{ip}{WOLFLINK_REBOOT_PATH}"

        self.log(
            f"[{cluster}] Starte WolfLink-Pro-Modul neu ({ip}). "
            f"Betroffene Sensoren ({len(stale_sensors)}): {stale_sensors}",
            level="WARNING"
        )
        try:
            response = requests.post(
                url,
                auth=(user, password),
                data={"name": "reboKickoff", "rebo": "init"},
                timeout=10,
            )
            if response.status_code < 400:
                self.last_wolflink_restart_dt[cluster] = now_utc
                self.log(
                    f"[{cluster}] WolfLink-Pro-Modul ({ip}) erfolgreich neu gestartet "
                    f"(HTTP {response.status_code})."
                )
            else:
                self.log(
                    f"[{cluster}] WolfLink-Neustart fehlgeschlagen: "
                    f"HTTP {response.status_code} von {url}",
                    level="ERROR"
                )
        except Exception as e:
            self.log(
                f"[{cluster}] FEHLER beim WolfLink-Neustart ({url}): {e}",
                level="ERROR"
            )

    def _maybe_restart_addon(self, stale_by_cluster: dict[str, list[str]], now_utc: datetime):
        """Startet das ISM7MQTT-Add-on neu, wenn globaler Cooldown abgelaufen."""
        if self.last_addon_restart_dt is not None:
            elapsed = now_utc - self.last_addon_restart_dt
            if elapsed < self.cooldown:
                remaining = int((self.cooldown - elapsed).total_seconds())
                self.log(
                    f"Addon-Cooldown aktiv (letzter Neustart vor "
                    f"{int(elapsed.total_seconds())}s) - naechster Neustart in {remaining}s.",
                    level="INFO"
                )
                return

        self._restart_addon(stale_by_cluster, now_utc)

    def _restart_addon(self, stale_by_cluster: dict[str, list[str]], now_utc: datetime):
        """Startet das ISM7MQTT-Add-on neu und setzt den globalen Cooldown-Zeitstempel."""
        all_stale = [s for sensors in stale_by_cluster.values() for s in sensors]
        self.log(
            f"Starte Add-on '{self.addon_slug}' neu. "
            f"Betroffene Sensoren ({len(all_stale)}): {all_stale}",
            level="WARNING"
        )
        try:
            self.call_service("hassio/addon_restart", slug=self.addon_slug)
            self.last_addon_restart_dt = now_utc
            self.log(f"Add-on '{self.addon_slug}' erfolgreich neu gestartet.")
        except Exception as e:
            self.log(f"FEHLER beim Neustart von '{self.addon_slug}': {e}", level="ERROR")

    # ------------------------------------------------------------------
    # Hilfsmethoden
    # ------------------------------------------------------------------

    def _is_stale(self, entity_id: str, now_utc: datetime) -> bool:
        """Gibt True zurueck, wenn der Sensor laenger als stale_threshold kein Update hatte."""
        state_obj = self.get_state(entity_id, attribute="all")
        if not state_obj:
            self.log(f"Sensor '{entity_id}' nicht gefunden.", level="WARNING")
            return True

        last_updated_raw = state_obj.get("last_updated")
        if not last_updated_raw:
            return True

        try:
            last_updated_utc = self.convert_utc(last_updated_raw)
            age = now_utc - last_updated_utc
            return age > self.stale_threshold
        except Exception as e:
            self.log(f"Fehler beim Parsen von last_updated fuer '{entity_id}': {e}", level="ERROR")
            return True
