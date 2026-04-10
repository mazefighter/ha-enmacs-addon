"""
KIS EMS Simulator

Laeuft als eigenes HA-Addon und:
  1. Registriert alle benoetigten Test-Entities beim Start in HA.
  2. Stellt ein Web-UI (Ingress) bereit zum Ausloesen von Szenarien.
  3. Simuliert automatisch die Wallbox-Job-Bestaetigung, damit der
     vollstaendige End-to-End-Flow von ems_bezugsueberwachung testbar ist.
  4. Aktualisiert periodisch sensor.sun_next_dusk damit
     ems_einspeisebegrenzung korrekt rechnen kann.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta

from aiohttp import web
import ha_wrapper as hass
from scenarios import INITIAL_ENTITIES, SCENARIOS, ENTITY_LABELS

logger = logging.getLogger("ems_simulator")


# ---------------------------------------------------------------------------
# HTML-Template (eingebettet – keine externen Assets nötig)
# ---------------------------------------------------------------------------
_HTML = """\
<!DOCTYPE html>
<html lang="de">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>KIS EMS Simulator</title>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
          background:#f0f2f5;color:#333;font-size:14px}}
    .hdr{{background:#1a237e;color:#fff;padding:18px 24px;
          display:flex;align-items:center;justify-content:space-between}}
    .hdr h1{{font-size:1.3rem;font-weight:600}}
    .badge{{background:rgba(255,255,255,.18);padding:5px 14px;
            border-radius:20px;font-size:.85rem}}
    .wrap{{max-width:1200px;margin:0 auto;padding:24px 16px}}
    h2{{margin:28px 0 12px;color:#1a237e;font-size:.85rem;
        text-transform:uppercase;letter-spacing:.8px}}
    /* Scenario grid */
    .grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:12px}}
    .card{{background:#fff;border-radius:10px;padding:16px 18px;
           box-shadow:0 1px 6px rgba(0,0,0,.08);border-left:4px solid #ccc;
           cursor:pointer;transition:all .15s;text-decoration:none;
           display:block;color:inherit}}
    .card:hover{{transform:translateY(-2px);box-shadow:0 4px 14px rgba(0,0,0,.12)}}
    .card.active{{background:#f1f8f4}}
    .card-head{{display:flex;align-items:center;gap:8px;margin-bottom:6px}}
    .card-title{{font-size:.95rem;font-weight:600}}
    .card-desc{{font-size:.8rem;color:#666;line-height:1.45}}
    .pill{{font-size:.65rem;background:#4caf50;color:#fff;
           padding:2px 8px;border-radius:10px;white-space:nowrap}}
    /* Sensor table */
    table{{width:100%;border-collapse:collapse;background:#fff;
           border-radius:10px;overflow:hidden;box-shadow:0 1px 6px rgba(0,0,0,.08)}}
    th{{background:#1a237e;color:#fff;padding:10px 14px;text-align:left;
        font-weight:500;font-size:.8rem}}
    td{{padding:9px 14px;border-bottom:1px solid #f0f2f5;font-size:.85rem}}
    tr:last-child td{{border-bottom:none}}
    tr:hover td{{background:#fafbff}}
    .val{{font-family:monospace;font-weight:700;color:#1a237e}}
    /* Manual form */
    .mform{{background:#fff;border-radius:10px;padding:18px;
            box-shadow:0 1px 6px rgba(0,0,0,.08)}}
    .row{{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:8px;align-items:center}}
    select,input{{padding:7px 10px;border:1px solid #dde;border-radius:6px;
                  font-size:.85rem;flex:1;min-width:140px}}
    btn,button{{background:#1a237e;color:#fff;border:none;padding:7px 18px;
                border-radius:6px;cursor:pointer;font-size:.85rem;white-space:nowrap}}
    button:hover{{background:#283593}}
    .msg{{font-size:.75rem;color:#777;margin-top:4px}}
    footer{{text-align:center;margin-top:32px;font-size:.75rem;color:#aaa}}
    a{{color:#1a237e}}
  </style>
</head>
<body>
<div class="hdr">
  <h1>🧪 KIS EMS Simulator</h1>
  <div class="badge" id="active-name">Aktiv: {active_name}</div>
</div>
<div class="wrap">

  <h2>Szenarien</h2>
  <div class="grid">{scenario_cards}</div>

  <h2>Sensor-Werte</h2>
  <table>
    <thead><tr><th>Entity</th><th>Wert</th><th>Beschreibung</th></tr></thead>
    <tbody id="stbody">{sensor_rows}</tbody>
  </table>

  <h2>Manuelle Kontrolle</h2>
  <div class="mform">
    <form id="mf">
      <div class="row">
        <select id="eid">{entity_opts}</select>
        <input id="eval" type="number" placeholder="Neuer Wert" step="any">
        <button type="submit">Setzen</button>
      </div>
      <div class="msg" id="mfmsg"></div>
    </form>
  </div>

  <footer>
    Aktualisiert alle 8 s &nbsp;·&nbsp;
    <a href="api/status" target="_blank">API Status</a> &nbsp;·&nbsp;
    <a href="api/sensors" target="_blank">Alle Sensoren (JSON)</a>
  </footer>
</div>
<script>
async function refresh() {{
  try {{
    const r = await fetch('api/sensors');
    const rows = await r.json();
    document.getElementById('stbody').innerHTML = rows.map(s =>
      `<tr><td><code>${{s.entity_id}}</code></td>
           <td class="val">${{s.state}}</td>
           <td>${{s.label}}</td></tr>`
    ).join('');
    const st = await fetch('api/status');
    const d = await st.json();
    document.getElementById('active-name').textContent = 'Aktiv: ' + d.active_scenario_name;
  }} catch(e) {{}}
}}
document.getElementById('mf').addEventListener('submit', async e => {{
  e.preventDefault();
  const entity_id = document.getElementById('eid').value;
  const state = document.getElementById('eval').value;
  const r = await fetch('api/sensor', {{
    method:'POST',
    headers:{{'Content-Type':'application/json'}},
    body: JSON.stringify({{entity_id, state}})
  }});
  const msg = document.getElementById('mfmsg');
  if (r.ok) {{ msg.style.color='#4caf50'; msg.textContent = '✓ ' + entity_id + ' = ' + state; }}
  else       {{ msg.style.color='#f44336'; msg.textContent = 'Fehler beim Setzen'; }}
  await refresh();
}});
setInterval(refresh, 8000);
</script>
</body>
</html>
"""


def _build_html(active_key: str, entity_states: dict) -> str:
    """Rendert das HTML-Template mit aktuellen Werten."""
    active = SCENARIOS.get(active_key, {})
    active_name = active.get("name", active_key or "–")

    # Szenario-Karten
    cards = []
    for key, sc in SCENARIOS.items():
        is_active = key == active_key
        border = sc["color"]
        pill = '<span class="pill">Aktiv</span>' if is_active else ""
        cards.append(
            f'<a class="card {"active" if is_active else ""}" '
            f'style="border-left-color:{border}" '
            f'href="scenario/{key}">'
            f'<div class="card-head">'
            f'<span style="font-size:1.3rem">{sc["icon"]}</span>'
            f'<span class="card-title">{sc["name"]}</span>{pill}'
            f'</div>'
            f'<div class="card-desc">{sc["description"]}</div>'
            f'</a>'
        )

    # Sensor-Zeilen
    rows = []
    for eid, label in ENTITY_LABELS.items():
        state = entity_states.get(eid, "–")
        rows.append(
            f'<tr><td><code>{eid}</code></td>'
            f'<td class="val">{state}</td>'
            f'<td>{label}</td></tr>'
        )

    # Entity-Optionen für manuelles Formular
    opts = "\n".join(
        f'<option value="{eid}">{label}</option>'
        for eid, label in ENTITY_LABELS.items()
    )

    return _HTML.format(
        active_name=active_name,
        scenario_cards="\n".join(cards),
        sensor_rows="\n".join(rows),
        entity_opts=opts,
    )


# ---------------------------------------------------------------------------
# Simulator-App
# ---------------------------------------------------------------------------

class EmsSimulator(hass.Hass):
    """
    Steuert alle Simulations-Entities und stellt das Web-UI bereit.
    """

    def initialize(self):
        self._active_scenario: str = ""
        self._confirm_delay: int = int(self.args.get("confirm_delay_seconds", 3))
        self._auto_sun: bool = bool(self.args.get("auto_update_sun_sensor", True))
        self._port: int = 8099  # muss mit ingress_port in config.yaml übereinstimmen

        asyncio.create_task(self._boot())

    # ------------------------------------------------------------------
    # Boot-Sequenz
    # ------------------------------------------------------------------

    async def _boot(self):
        await asyncio.sleep(2)  # Kurz warten bis HA-Verbindung stabil ist
        await self._register_all_entities()
        await self._start_http_server()

        # Wallbox-Job-Auto-Confirm: lauscht auf ems_netzbezug_begrenzung
        self.listen_state(
            self._on_begrenzung_changed,
            "sensor.ems_netzbezug_begrenzung",
            attribute="timestamp",
        )

        # Sonne periodisch aktualisieren
        if self._auto_sun:
            self.run_every(self._update_sun, "now", 3600)

        # Simulation loop fuer automatische Aenderung von Werten
        asyncio.create_task(self._simulation_loop())

        self.log("EMS Simulator gestartet – Web-UI läuft auf Port 8099")

    # ------------------------------------------------------------------
    # Entity-Initialisierung
    # ------------------------------------------------------------------

    async def _register_all_entities(self):
        """Legt alle Simulator-Entities in HA an (oder setzt sie zurück)."""
        count = 0
        for entity_id, cfg in INITIAL_ENTITIES.items():
            await self.set_state_async(
                entity_id,
                state=cfg["state"],
                attributes=cfg.get("attributes", {}),
            )
            count += 1
        self.log(f"{count} Test-Entities in HA registriert.")

    # ------------------------------------------------------------------
    # Szenarien
    # ------------------------------------------------------------------

    async def _apply_scenario(self, key: str) -> bool:
        if key not in SCENARIOS:
            return False
        sc = SCENARIOS[key]
        for entity_id, value in sc["sensors"].items():
            if isinstance(value, dict):
                await self.set_state_async(
                    entity_id,
                    state=value["state"],
                    attributes=value.get("attributes", {}),
                )
            else:
                await self.set_state_async(entity_id, state=value)
        self._active_scenario = key
        self.log(f"Szenario aktiviert: '{sc['name']}'")
        return True

    # ------------------------------------------------------------------
    # Auto-Bestätigung Wallbox-Jobs
    # ------------------------------------------------------------------

    async def _on_begrenzung_changed(self, entity, attribute, old, new, **kwargs):
        """
        Wenn ems_bezugsueberwachung einen Job anlegt (timestamp-Attribut ändert
        sich und aktiviert=True), bestätigen wir ihn nach confirm_delay_seconds.
        """
        if not new or new == old:
            return
        aktiviert = self.get_state("sensor.ems_netzbezug_begrenzung", attribute="aktiviert")
        if not aktiviert:
            return
        self.log(
            f"Wallbox-Job erkannt (timestamp={new}), "
            f"bestätige in {self._confirm_delay}s …"
        )
        asyncio.create_task(self._confirm_job(new))

    async def _confirm_job(self, timestamp):
        """Simuliert die Wallbox-Bestätigung durch Setzen von source_timestamp."""
        await asyncio.sleep(self._confirm_delay)
        await self.set_state_async(
            "sensor.ems_netzbezug_begrenzung_ausfuehrung",
            state=1,
            attributes={"source_timestamp": timestamp, "friendly_name": "EMS Netzbezug Ausführung"},
        )
        self.log(f"Wallbox-Job bestätigt: source_timestamp={timestamp}")

    # ------------------------------------------------------------------
    # Sonne aktualisieren
    # ------------------------------------------------------------------

    async def _update_sun(self, kwargs):
        now = datetime.now()
        dusk = now.replace(hour=20, minute=30, second=0, microsecond=0)
        if dusk <= now:
            dusk += timedelta(days=1)
        await self.set_state_async(
            "sensor.sun_next_dusk",
            state=dusk.isoformat(),
            attributes={"friendly_name": "[SIM] Nächster Sonnenuntergang"},
        )

    # ------------------------------------------------------------------
    # Simulation Loop
    # ------------------------------------------------------------------

    async def _simulation_loop(self):
        import random
        await asyncio.sleep(5)
        
        while True:
            await asyncio.sleep(10)
            
            # PV Generation
            pv_str = self.get_state("sensor.pv_anlage_1_2_gesamtleistung_in_w", default="0")
            try:
                pv = float(pv_str)
                pv += random.uniform(-1500, 1500)
                pv = max(0.0, min(pv, 150000.0))
                await self.set_state_async("sensor.pv_anlage_1_2_gesamtleistung_in_w", state=int(pv))
            except ValueError:
                pass

            # Grid Power
            grid_str = self.get_state("sensor.grid_leistung_gesamt_in_w", default="0")
            try:
                grid = float(grid_str)
                grid += random.uniform(-3000, 3000)
                grid = max(-100000.0, min(grid, 300000.0))
                await self.set_state_async("sensor.grid_leistung_gesamt_in_w", state=int(grid))
                
                # Einspeisung derived from Grid
                einspeisung = -grid if grid < 0 else 0
                await self.set_state_async("sensor.grid_leistung_einspeisung_in_w", state=int(einspeisung))
            except ValueError:
                pass

            # Battery
            soc_str = self.get_state("sensor.soc_gesamt", default="50")
            batt_p_str = self.get_state("sensor.batterie_leistung", default="0")
            try:
                soc = float(soc_str)
                batt_p = float(batt_p_str)
                
                batt_p += random.uniform(-1000, 1000)
                batt_p = max(-50000.0, min(batt_p, 50000.0))
                await self.set_state_async("sensor.batterie_leistung", state=int(batt_p))
                
                # Update SOC based on power over 10 seconds, assuming 100kWh capacity
                soc_change = (batt_p * 10 / 3600) / 100000 * 100
                soc -= soc_change # negative power means charge / positive means discharge
                soc = max(0.0, min(soc, 100.0))
                await self.set_state_async("sensor.soc_gesamt", state=round(soc, 1))
            except ValueError:
                pass

            # Wallboxen
            wb_str = self.get_state("sensor.wallbox_leistung", default="0")
            try:
                wb = float(wb_str)
                wb += random.uniform(-2000, 2000)
                wb = max(0.0, min(wb, 200000.0))
                await self.set_state_async("sensor.wallbox_leistung", state=int(wb))
            except ValueError:
                pass

    # ------------------------------------------------------------------
    # HTTP-Server (aiohttp)
    # ------------------------------------------------------------------

    async def _start_http_server(self):
        app = web.Application()
        app.router.add_get("/",                    self._ui)
        app.router.add_get("/scenario/{key}",      self._trigger_scenario)
        app.router.add_get("/api/status",          self._api_status)
        app.router.add_get("/api/sensors",         self._api_sensors)
        app.router.add_post("/api/sensor",         self._api_set_sensor)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", self._port)
        await site.start()
        self.log(f"HTTP-Server lauscht auf 0.0.0.0:{self._port}")

    def _entity_states_snapshot(self) -> dict:
        """Liest den aktuellen State aller relevanten Entities aus dem Cache."""
        return {
            eid: self.get_state(eid, default="–")
            for eid in ENTITY_LABELS
        }

    async def _ui(self, request: web.Request) -> web.Response:
        html = _build_html(self._active_scenario, self._entity_states_snapshot())
        return web.Response(text=html, content_type="text/html")

    async def _trigger_scenario(self, request: web.Request) -> web.Response:
        key = request.match_info["key"]
        ok = await self._apply_scenario(key)
        if not ok:
            return web.Response(status=404, text=f"Szenario '{key}' nicht gefunden.")
        # Redirect zurück zur UI
        raise web.HTTPSeeOther("/")

    async def _api_status(self, request: web.Request) -> web.Response:
        active = SCENARIOS.get(self._active_scenario, {})
        body = {
            "active_scenario": self._active_scenario,
            "active_scenario_name": active.get("name", "–"),
            "scenarios": {k: v["name"] for k, v in SCENARIOS.items()},
        }
        return web.Response(
            text=json.dumps(body, ensure_ascii=False),
            content_type="application/json",
        )

    async def _api_sensors(self, request: web.Request) -> web.Response:
        rows = [
            {
                "entity_id": eid,
                "state": self.get_state(eid, default="–"),
                "label": ENTITY_LABELS.get(eid, ""),
            }
            for eid in ENTITY_LABELS
        ]
        return web.Response(
            text=json.dumps(rows, ensure_ascii=False),
            content_type="application/json",
        )

    async def _api_set_sensor(self, request: web.Request) -> web.Response:
        try:
            data = await request.json()
            entity_id = data.get("entity_id", "").strip()
            state = data.get("state")
            if not entity_id or state is None:
                return web.Response(status=400, text='{"error":"entity_id und state erforderlich"}',
                                    content_type="application/json")
            attributes = data.get("attributes", {})
            await self.set_state_async(entity_id, state=state, attributes=attributes)
            self.log(f"Manuell gesetzt: {entity_id} = {state}")
            return web.Response(
                text=json.dumps({"ok": True, "entity_id": entity_id, "state": str(state)},
                                ensure_ascii=False),
                content_type="application/json",
            )
        except Exception as exc:
            return web.Response(status=500,
                                text=json.dumps({"error": str(exc)}, ensure_ascii=False),
                                content_type="application/json")
