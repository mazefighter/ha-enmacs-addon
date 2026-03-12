"""
Enmacs HAApi – Home Assistant API Wrapper.

Importiere diese Klasse in deinen Skripten für volle IDE-Autovervollständigung:

    from haapi import HAApi

    def initialize(api: HAApi) -> None:
        pass

    def run(api: HAApi) -> None:
        data = api.get_state("sensor.temperature")
        print(data["state"])
"""

from __future__ import annotations
import requests


class HAApi:
    """Wrapper für die Home Assistant REST-API.

    Wird von Enmacs automatisch instanziiert und an initialize() / run() übergeben.
    Importiere die Klasse lokal für IDE-Unterstützung:  from haapi import HAApi
    """

    def __init__(self, token: str) -> None:
        self._base = "http://supervisor/core/api"
        self._headers = {
            "Authorization": f"Bearer {token}",
            "content-type": "application/json",
        }

    def get_state(self, entity_id: str) -> dict:
        """Gibt den aktuellen Zustand einer Entity zurück.

        Args:
            entity_id: Die Entity-ID, z.B. "sensor.temperature_living_room"

        Returns:
            dict mit den Feldern:
              - "state" (str): Aktueller Zustand
              - "attributes" (dict): Alle Attribute der Entity
              - "entity_id" (str)
              - "last_changed" (str)
              - "last_updated" (str)
        """
        try:
            resp = requests.get(
                f"{self._base}/states/{entity_id}",
                headers=self._headers,
                timeout=10,
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else "unknown"
            reason = e.response.reason if (e.response is not None and hasattr(e.response, "reason")) else ""
            body = e.response.text if (e.response is not None and hasattr(e.response, "text")) else ""
            raise requests.exceptions.HTTPError(
                f"Error fetching state for entity '{entity_id}': {status_code} {reason} - {body}",
                response=e.response,
                request=e.request,
            ) from e
        return resp.json()

    def set_state(self, entity_id: str, state: str, attributes: dict | None = None) -> dict:
        """Setzt den Zustand einer Entity (nur virtuelle Entities).

        Args:
            entity_id: Die Entity-ID
            state: Der neue Zustand als String
            attributes: Optionale Attribute (z.B. {"unit_of_measurement": "°C"})

        Returns:
            dict mit dem neuen Zustand wie von HA zurückgegeben
        """
        payload = {"state": state, "attributes": attributes or {}}
        try:
            resp = requests.post(
                f"{self._base}/states/{entity_id}",
                headers=self._headers,
                json=payload,
                timeout=10,
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else "unknown"
            reason = e.response.reason if (e.response is not None and hasattr(e.response, "reason")) else ""
            body = e.response.text if (e.response is not None and hasattr(e.response, "text")) else ""
            raise requests.exceptions.HTTPError(
                f"Error setting state for entity '{entity_id}' to '{state}': {status_code} {reason} - {body}",
                response=e.response,
                request=e.request,
            ) from e
        return resp.json()

    def get_all_states(self) -> list[dict]:
        """Gibt den aktuellen Zustand aller Entities zurück.

        Returns:
            Liste von dicts, jedes mit den Feldern "entity_id", "state", "attributes",
            "last_changed" und "last_updated".
        """
        try:
            resp = requests.get(
                f"{self._base}/states",
                headers=self._headers,
                timeout=30,
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else "unknown"
            reason = e.response.reason if (e.response is not None and hasattr(e.response, "reason")) else ""
            body = e.response.text if (e.response is not None and hasattr(e.response, "text")) else ""
            raise requests.exceptions.HTTPError(
                f"Error fetching all states: {status_code} {reason} - {body}",
                response=e.response,
                request=e.request,
            ) from e
        return resp.json()

    def call_service(self, domain: str, service: str, **kwargs) -> dict:
        """Ruft einen Home Assistant Service auf.

        Args:
            domain: Domain des Services, z.B. "light", "switch", "notify"
            service: Name des Services, z.B. "turn_on", "turn_off"
            **kwargs: Service-Parameter, z.B. entity_id="light.wohnzimmer", brightness=200

        Returns:
            Liste der betroffenen States als dict

        Beispiele:
            api.call_service("light", "turn_on", entity_id="light.wohnzimmer")
            api.call_service("notify", "persistent_notification", message="Hallo!", title="Enmacs")
            api.call_service("switch", "toggle", entity_id="switch.steckdose")
        """
        try:
            resp = requests.post(
                f"{self._base}/services/{domain}/{service}",
                headers=self._headers,
                json=kwargs,
                timeout=10,
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else "unknown"
            reason = e.response.reason if (e.response is not None and hasattr(e.response, "reason")) else ""
            body = e.response.text if (e.response is not None and hasattr(e.response, "text")) else ""
            raise requests.exceptions.HTTPError(
                f"Error calling service '{domain}.{service}': {status_code} {reason} - {body}",
                response=e.response,
                request=e.request,
            ) from e
        return resp.json()
