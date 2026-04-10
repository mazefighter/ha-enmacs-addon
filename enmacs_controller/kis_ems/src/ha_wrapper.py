import asyncio
import aiohttp
import websockets
import json
import logging
import os
import uuid
import requests as _requests_sync
from datetime import datetime, timedelta, time, date
from typing import Dict, Any, Callable, Optional, List
import concurrent.futures

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class EntityProxy:
    """
    Stellt eine einzelne HA-Entity dar – Ersatz für AppDaemon's Entity-Objekt.
    Ermöglicht .get_state(), .listen_state() und .set_state() direkt auf der Entity.
    """

    def __init__(self, entity_id: str, hass: "Hass"):
        self._entity_id = entity_id
        self._hass = hass

    def get_state(self, attribute: str = None, default: Any = None) -> Any:
        return self._hass.get_state(self._entity_id, attribute=attribute, default=default)

    def listen_state(self, cb: Callable, attribute: str = None, new: str = None, **kwargs) -> str:
        if attribute is not None:
            kwargs["attribute"] = attribute
        if new is not None:
            kwargs["new"] = new
        return self._hass.listen_state(cb, self._entity_id, **kwargs)

    def set_state(self, **kwargs):
        return self._hass.set_state(self._entity_id, **kwargs)

    def call_service(self, service: str, **kwargs):
        return self._hass.call_service(service, entity_id=self._entity_id, **kwargs)

    def __bool__(self):
        return self._entity_id is not None

    def __repr__(self):
        return f"EntityProxy({self._entity_id})"


class Hass:
    """
    Asynchroner Ersatz für die AppDaemon hass.Hass Basisklasse.
    Erlaubt es, bestehende AppDaemon-Skripte fast unverändert als native
    Python-Add-ons in Home Assistant laufen zu lassen.
    """

    # Singleton-ähnliche Connection – wird von allen App-Instanzen geteilt
    _ws_client = None
    _rest_session = None
    _supervisor_token = os.environ.get("SUPERVISOR_TOKEN") or os.environ.get("HASSIO_TOKEN", "")
    _base_url = "http://supervisor/core/api"
    _ws_url = "ws://supervisor/core/websocket"
    _loop: Optional[asyncio.AbstractEventLoop] = None
    _message_id = 1
    _pending_requests: Dict[int, asyncio.Future] = {}
    _state_cache: Dict[str, Any] = {}
    _state_listeners: List = []   # [(handle, cb, entity_id, kwargs_dict), ...]
    _event_listeners: List = []
    _timers: Dict[str, asyncio.Task] = {}

    def __init__(self, args: dict = None):
        self.args = args or {}
        self.name = self.__class__.__name__
        self._init_logger()
        self._my_timers: List[str] = []

    def _init_logger(self):
        self._logger = logging.getLogger(self.name)

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    def log(self, msg: str, level: str = "INFO"):
        lvl = getattr(logging, level.upper(), logging.INFO)
        self._logger.log(lvl, msg)

    def error(self, msg: str):
        self._logger.error(msg)

    def warning(self, msg: str):
        self._logger.warning(msg)

    def set_log_level(self, level: str):
        lvl = getattr(logging, level.upper(), logging.INFO)
        self._logger.setLevel(lvl)

    # ------------------------------------------------------------------
    # Verbindungsaufbau
    # ------------------------------------------------------------------

    @classmethod
    async def connect(cls):
        """Verbindet den Singleton WebSocket und REST Session."""
        cls._loop = asyncio.get_running_loop()
        if not cls._supervisor_token:
            logger.warning("Kein SUPERVISOR_TOKEN – API-Calls könnten fehlschlagen.")

        cls._rest_session = aiohttp.ClientSession(
            headers={
                "Authorization": f"Bearer {cls._supervisor_token}",
                "Content-Type": "application/json",
            },
            timeout=aiohttp.ClientTimeout(total=30),
        )

        await cls._fetch_all_states()
        asyncio.create_task(cls._ws_loop())
        await asyncio.sleep(1)  # Kurz warten bis Auth durch ist

    @classmethod
    async def _fetch_all_states(cls):
        try:
            async with cls._rest_session.get(f"{cls._base_url}/states") as resp:
                if resp.status == 200:
                    states = await resp.json()
                    for state in states:
                        cls._state_cache[state["entity_id"]] = state
        except Exception as e:
            logger.error(f"Fehler beim Laden der initialen States: {e}")

    @classmethod
    async def _ws_loop(cls):
        while True:
            try:
                async with websockets.connect(cls._ws_url) as ws:
                    cls._ws_client = ws
                    while True:
                        msg = await ws.recv()
                        data = json.loads(msg)

                        if data.get("type") == "auth_required":
                            await ws.send(json.dumps({"type": "auth", "access_token": cls._supervisor_token}))

                        elif data.get("type") == "auth_ok":
                            logger.info("WebSocket authentifiziert.")
                            await cls._fetch_all_states()
                            await cls._send_ws({"type": "subscribe_events", "event_type": "state_changed"})
                            await cls._send_ws({"type": "subscribe_events"})

                        elif data.get("type") == "event":
                            event = data.get("event", {})
                            event_type = event.get("event_type")
                            event_data = event.get("data", {})
                            if event.get("event_type") == "state_changed":
                                ed = event.get("data", {})
                                entity_id = ed.get("entity_id")
                                old_state = ed.get("old_state") or {}
                                new_state = ed.get("new_state") or {}

                                # Cache aktualisieren
                                if new_state:
                                    cls._state_cache[entity_id] = new_state

                                # Listener benachrichtigen
                                for _handle, cb, eid, listen_kwargs in cls._state_listeners:
                                    if eid is not None and eid != entity_id:
                                        continue
                                    asyncio.create_task(
                                        cls._dispatch_listener(cb, entity_id, old_state, new_state, listen_kwargs)
                                    )

                            for _handle, cb, listen_event, listen_kwargs in cls._event_listeners:
                                if listen_event and listen_event != event_type:
                                    continue

                                # Zusätzliche Filter wie service/domain über event.data
                                match = True
                                for key, expected in listen_kwargs.items():
                                    if key == "event":
                                        continue
                                    if event_data.get(key) != expected:
                                        match = False
                                        break
                                if not match:
                                    continue

                                asyncio.create_task(cls._run_callback(cb, event_type, event_data, listen_kwargs))

                        elif data.get("id") in cls._pending_requests:
                            future = cls._pending_requests.pop(data["id"])
                            if not future.done():
                                future.set_result(data)

            except Exception as e:
                logger.error(f"WebSocket getrennt: {e}. Reconnect in 5s...")
                cls._ws_client = None
                await asyncio.sleep(5)

    @classmethod
    async def _dispatch_listener(cls, cb, entity_id: str, old_state: dict, new_state: dict, listen_kwargs: dict):
        """Wertet Listener-Filter aus und ruft den Callback auf."""
        attribute = listen_kwargs.get("attribute")
        new_filter = listen_kwargs.get("new")

        if attribute:
            # Attribut-Level Listener
            old_val = old_state.get("attributes", {}).get(attribute)
            new_val = new_state.get("attributes", {}).get(attribute)
            if old_val == new_val:
                return  # Attribut hat sich nicht geändert
            if new_filter is not None and str(new_val) != str(new_filter):
                return
            await cls._run_callback(cb, entity_id, attribute, old_val, new_val)
        else:
            # State-Level Listener
            old_val = old_state.get("state")
            new_val = new_state.get("state")
            if new_filter is not None and str(new_val) != str(new_filter):
                return
            await cls._run_callback(cb, entity_id, "state", old_val, new_val)

    @classmethod
    async def _send_ws(cls, payload: dict) -> dict:
        if not cls._ws_client:
            return {}
        msg_id = cls._message_id
        cls._message_id += 1
        payload["id"] = msg_id

        future = asyncio.get_running_loop().create_future()
        cls._pending_requests[msg_id] = future

        await cls._ws_client.send(json.dumps(payload))
        try:
            return await asyncio.wait_for(future, timeout=10.0)
        except asyncio.TimeoutError:
            cls._pending_requests.pop(msg_id, None)
            return {}

    @classmethod
    def _schedule_coroutine(cls, coro):
        """Plant Coroutine sowohl aus dem Event-Loop als auch aus Worker-Threads sicher ein."""
        try:
            running_loop = asyncio.get_running_loop()
            if cls._loop and running_loop is cls._loop:
                return cls._loop.create_task(coro)
        except RuntimeError:
            pass

        if cls._loop is None:
            logger.error("Kein Event-Loop verfuegbar, Coroutine kann nicht geplant werden.")
            try:
                coro.close()
            except Exception:
                pass
            return None

        return asyncio.run_coroutine_threadsafe(coro, cls._loop)

    @classmethod
    async def _run_callback(cls, cb, *args):
        """Führt einen Callback aus – async per await, sync per Thread."""
        try:
            if asyncio.iscoroutinefunction(cb):
                await cb(*args)
            else:
                await asyncio.to_thread(cb, *args)
        except Exception as e:
            cb_name = getattr(cb, "__name__", repr(cb))
            logger.error(f"Fehler im Callback {cb_name}: {e}", exc_info=True)

    # ------------------------------------------------------------------
    # AppDaemon API – State
    # ------------------------------------------------------------------

    def get_state(self, entity_id: str = None, attribute: str = None, default: Any = None) -> Any:
        """Holt den State aus dem lokalen Cache (synchron)."""
        if not entity_id:
            return self._state_cache

        ent = self._state_cache.get(entity_id)

        if attribute:
            if attribute == "all":
                # Gibt immer ein dict zurück – auch wenn Entity noch nicht existiert
                return ent if ent else {"entity_id": entity_id, "state": default, "attributes": {}}
            if not ent:
                return default
            return ent.get("attributes", {}).get(attribute, default)

        if not ent:
            return default
        return ent.get("state", default)

    async def set_state_async(self, entity_id: str, state=None, attributes: dict = None):
        """Setzt den State über die HA REST API und aktualisiert den Cache."""
        payload = {}
        if state is not None:
            payload["state"] = str(state)
        if attributes is not None:
            payload["attributes"] = attributes

        try:
            async with self._rest_session.post(
                f"{self._base_url}/states/{entity_id}", json=payload
            ) as resp:
                if resp.status in (200, 201):
                    result = await resp.json()
                    self._state_cache[entity_id] = result
                else:
                    self.log(f"set_state {entity_id}: HTTP {resp.status}", "ERROR")
        except Exception as e:
            self.log(f"Fehler bei set_state für {entity_id}: {e}", "ERROR")

    def set_state(self, entity_id: str, **kwargs):
        """Synchrone Variante: optimistischer Cache-Update + async REST-Call."""
        state = kwargs.get("state")
        attributes = kwargs.get("attributes")

        # Optimistisches Cache-Update damit nachfolgende get_state()-Aufrufe direkt funktionieren
        if entity_id not in self._state_cache:
            self._state_cache[entity_id] = {"entity_id": entity_id, "state": None, "attributes": {}}
        if state is not None:
            self._state_cache[entity_id]["state"] = str(state)
        if attributes is not None:
            self._state_cache[entity_id]["attributes"] = attributes

        self._schedule_coroutine(self.set_state_async(entity_id, state, attributes))

    def get_entity(self, entity_id: str) -> EntityProxy:
        """Gibt ein EntityProxy-Objekt zurück (Ersatz für AppDaemon's get_entity)."""
        return EntityProxy(entity_id, self)

    def get_history(self, entity_id: str, start_time: datetime, end_time: datetime) -> list:
        """
        Holt historische Zustände über die HA REST API.
        Synchron – darf nur aus Thread-Callbacks (run_every/run_in) aufgerufen werden.
        """
        headers = {
            "Authorization": f"Bearer {self._supervisor_token}",
            "Content-Type": "application/json",
        }
        start_str = start_time.isoformat()
        end_str = end_time.isoformat()
        url = (
            f"{self._base_url}/history/period/{start_str}"
            f"?filter_entity_id={entity_id}&end_time={end_str}&minimal_response=true"
        )
        try:
            response = _requests_sync.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                return response.json()
            else:
                self.log(f"get_history {entity_id}: HTTP {response.status_code}", "WARNING")
                return []
        except Exception as e:
            self.log(f"get_history {entity_id}: {e}", "ERROR")
            return []

    # ------------------------------------------------------------------
    # AppDaemon API – Services
    # ------------------------------------------------------------------

    async def call_service_async(self, service_str: str, **kwargs):
        if "." in service_str:
            domain, service = service_str.split(".", 1)
        elif "/" in service_str:
            domain, service = service_str.split("/", 1)
        else:
            domain, service = "homeassistant", service_str
        try:
            async with self._rest_session.post(
                f"{self._base_url}/services/{domain}/{service}", json=kwargs
            ) as resp:
                if resp.status != 200:
                    self.log(
                        f"Fehler beim Aufruf von {service_str}: HTTP {resp.status} – {await resp.text()}",
                        "ERROR",
                    )
        except Exception as e:
            self.log(f"Fehler bei call_service {service_str}: {e}", "ERROR")

    def call_service(self, service: str, **kwargs):
        self._schedule_coroutine(self.call_service_async(service, **kwargs))

    def turn_on(self, entity_id: str, **kwargs):
        self.call_service("homeassistant.turn_on", entity_id=entity_id, **kwargs)

    def turn_off(self, entity_id: str, **kwargs):
        self.call_service("homeassistant.turn_off", entity_id=entity_id, **kwargs)

    # ------------------------------------------------------------------
    # AppDaemon API – Listener
    # ------------------------------------------------------------------

    def listen_state(self, cb: Callable, entity: str = None, **kwargs) -> str:
        """Registriert einen State-Listener. Gibt einen Handle zurück."""
        handle = uuid.uuid4().hex
        self._state_listeners.append((handle, cb, entity, kwargs))
        return handle

    def cancel_listen_state(self, handle: str):
        """Entfernt einen zuvor registrierten State-Listener anhand seines Handles."""
        self._state_listeners[:] = [e for e in self._state_listeners if e[0] != handle]

    def listen_event(self, cb: Callable, event: str = None, **kwargs) -> str:
        """Registriert einen Event-Listener (kompatibel zu AppDaemon listen_event)."""
        handle = uuid.uuid4().hex
        self._event_listeners.append((handle, cb, event, kwargs))
        return handle

    def cancel_listen_event(self, handle: str):
        """Entfernt einen zuvor registrierten Event-Listener anhand seines Handles."""
        self._event_listeners[:] = [e for e in self._event_listeners if e[0] != handle]

    # ------------------------------------------------------------------
    # AppDaemon API – Timer
    # ------------------------------------------------------------------

    def run_every(self, cb: Callable, start, interval: int, **kwargs) -> str:
        """
        Führt cb alle `interval` Sekunden aus.
        start kann ein datetime-Objekt oder der String "now" sein.
        """
        handle = uuid.uuid4().hex

        async def _timer_loop():
            if isinstance(start, str) and start.lower() == "now":
                delay = 0.0
            elif isinstance(start, datetime):
                delay = (start - datetime.now()).total_seconds()
            else:
                delay = 0.0

            if delay > 0:
                await asyncio.sleep(delay)

            while handle in self._timers:
                self._schedule_coroutine(self._run_callback(cb, kwargs))
                await asyncio.sleep(interval)

        scheduled = self._schedule_coroutine(_timer_loop())
        if scheduled is not None:
            self._timers[handle] = scheduled
        self._my_timers.append(handle)
        return handle

    def run_daily(self, cb: Callable, time_str: str, **kwargs) -> str:
        """Führt cb täglich zur angegebenen Uhrzeit aus."""
        handle = uuid.uuid4().hex
        try:
            t = datetime.strptime(time_str, "%H:%M:%S").time()
        except ValueError:
            t = datetime.strptime(time_str, "%H:%M").time()

        async def _daily_loop():
            while handle in self._timers:
                now = datetime.now()
                target = datetime.combine(now.date(), t)
                if now >= target:
                    target += timedelta(days=1)
                
                # Polling mechanism to avoid DST drift
                while datetime.now() < target:
                    await asyncio.sleep(10)
                    
                if handle in self._timers:
                    self._schedule_coroutine(self._run_callback(cb, kwargs))
                    # Prevent duplicate execution right at the switch second
                    await asyncio.sleep(60)

        scheduled = self._schedule_coroutine(_daily_loop())
        if scheduled is not None:
            self._timers[handle] = scheduled
        self._my_timers.append(handle)
        return handle

    def run_in(self, cb: Callable, seconds: int, **kwargs) -> str:
        """Führt cb einmalig nach `seconds` Sekunden aus."""
        handle = uuid.uuid4().hex

        async def _in_loop():
            await asyncio.sleep(seconds)
            if handle in self._timers:
                self._schedule_coroutine(self._run_callback(cb, kwargs))
                self.cancel_timer(handle)

        scheduled = self._schedule_coroutine(_in_loop())
        if scheduled is not None:
            self._timers[handle] = scheduled
        self._my_timers.append(handle)
        return handle

    def cancel_timer(self, handle: str):
        if handle in self._timers:
            task = self._timers.pop(handle)
            task.cancel()
        if handle in self._my_timers:
            self._my_timers.remove(handle)

    # ------------------------------------------------------------------
    # AppDaemon API – Zeit / Datum
    # ------------------------------------------------------------------

    def get_now(self) -> datetime:
        """Gibt die aktuelle lokale Zeit als timezone-aware datetime zurück."""
        return datetime.now().astimezone()

    def datetime(self, aware: bool = False) -> datetime:
        return datetime.now().astimezone() if aware else datetime.now()

    def date(self) -> date:
        return datetime.now().date()

    def time(self) -> time:
        return datetime.now().time()

    def convert_utc(self, datetime_str: str) -> datetime:
        """
        Parst einen UTC-ISO-Datetime-String (wie ihn HA liefert) und
        gibt ein timezone-aware datetime in der lokalen Zeitzone zurück.
        """
        if not datetime_str:
            return datetime.now().astimezone()
        try:
            # Python 3.11+ versteht 'Z' nativ; für ältere Versionen ersetzen wir es
            dt = datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
            return dt.astimezone()
        except Exception as e:
            self.log(f"convert_utc: Fehler beim Parsen von '{datetime_str}': {e}", "ERROR")
            return datetime.now().astimezone()

    # ------------------------------------------------------------------
    # AppDaemon API – Sonstiges
    # ------------------------------------------------------------------

    def get_ad_api(self) -> "Hass":
        """Gibt sich selbst zurück – Ersatz für AppDaemon's self.get_ad_api()."""
        return self

    def get_app(self, app_name: str):
        """
        Gibt die App-Instanz zurück, falls sie im selben Addon läuft.
        Wird von main.py nach dem Laden aller Apps überschrieben.
        Cross-Addon-Kommunikation nicht unterstützt – bitte HA-Entities nutzen.
        """
        self.log(
            f"WARNUNG: get_app('{app_name}') – Cross-Addon-Kommunikation nicht unterstützt. "
            f"Bitte HA-Sensoren/MQTT verwenden.",
            "WARNING",
        )
        return None

    def get_plugin_config(self) -> dict:
        return {}

    def initialize(self):
        """Zu überschreiben in der Subklasse."""
        pass


def app_lock(func):
    """Dummy-Decorator als Ersatz für @ad.app_lock."""
    import functools

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper
