import asyncio
import logging
import yaml
import importlib
import sys
import os
import json
from datetime import datetime

from aiohttp import web
from ha_wrapper import Hass

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ems.main")

UI_PORT = 8100

_RUNTIME = {
        "loaded_apps": [],
        "options": {},
        "enabled_flags": {},
    "event_log": [],
    "last_states": {},
}

_MAX_EVENT_LOG = 120

_IMPORTANT_ENTITY_LABELS = {
    "sensor.ems_netzbezug_begrenzung": "Netzbezugbegrenzung",
    "sensor.ems_netzbezug_begrenzung_ausfuehrung": "Netzbezug-Ausfuehrung",
    "sensor.ems_einspeisesteuerung_zustand": "Einspeisebegrenzung",
    "sensor.pv_anlage_3_wirkleistungsbegrenzung": "WR3 Wirkleistungsbegrenzung",
    "sensor.ems_ladesteuerung_wb_main_status": "Wallbox Hauptstatus",
    "sensor.ems_netzbezug_begrenzung_wallbox_aktiv": "Wallbox Ueberlastregelung",
    "sensor.ems_ladesteuerung_wb_temperatur_status": "Wallbox Temperatur-Status",
    "sensor.ems_dynamischer_strompreis": "Dynamischer Strompreis",
}

_HTML = """\
<!DOCTYPE html>
<html lang="de">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>KIS EMS Monitor</title>
    <style>
        :root {
            --bg: #f3f7f2;
            --ink: #152226;
            --card: #ffffff;
            --line: #d8e0d5;
            --good: #2d8f58;
            --warn: #b26a11;
            --bad: #a6322f;
            --accent: #184f5c;
            --accent-soft: #e7f2f4;
        }
        * { box-sizing: border-box; }
        body {
            margin: 0;
            font-family: "Segoe UI", "Noto Sans", sans-serif;
            background: radial-gradient(1200px 400px at 20% -10%, #d9ece9 0%, transparent 60%), var(--bg);
            color: var(--ink);
        }
        .top {
            padding: 20px 24px;
            background: linear-gradient(135deg, #133b44, #205867);
            color: #fff;
            display: flex;
            gap: 12px;
            flex-wrap: wrap;
            align-items: center;
            justify-content: space-between;
        }
        .title { font-size: 1.3rem; font-weight: 700; letter-spacing: .3px; }
        .stamp { font-size: .85rem; opacity: .9; }
        .wrap { max-width: 1300px; margin: 0 auto; padding: 18px; }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 10px;
            margin-bottom: 14px;
        }
        .kpi {
            background: var(--card);
            border: 1px solid var(--line);
            border-radius: 12px;
            padding: 12px;
            box-shadow: 0 2px 8px rgba(0,0,0,.04);
        }
        .kpi .label { font-size: .75rem; text-transform: uppercase; color: #5f6a6d; }
        .kpi .value { font-size: 1.2rem; font-weight: 700; margin-top: 4px; }
        .chips { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 16px; }
        .chip {
            background: var(--accent-soft);
            border: 1px solid #b6d0d6;
            color: #123b45;
            font-size: .75rem;
            border-radius: 16px;
            padding: 4px 10px;
        }
        .sections {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
            gap: 12px;
            margin-bottom: 12px;
        }
        .events {
            background: var(--card);
            border: 1px solid var(--line);
            border-radius: 12px;
            box-shadow: 0 2px 8px rgba(0,0,0,.04);
            overflow: hidden;
        }
        .events h2 {
            margin: 0;
            font-size: .9rem;
            letter-spacing: .5px;
            text-transform: uppercase;
            background: #eef5ee;
            color: #234d2d;
            padding: 10px 12px;
            border-bottom: 1px solid var(--line);
        }
        .events-wrap { max-height: 280px; overflow: auto; }
        .section {
            background: var(--card);
            border: 1px solid var(--line);
            border-radius: 12px;
            overflow: hidden;
            box-shadow: 0 2px 8px rgba(0,0,0,.04);
        }
        .section h2 {
            margin: 0;
            font-size: .9rem;
            letter-spacing: .5px;
            text-transform: uppercase;
            background: #eef5ee;
            color: #234d2d;
            padding: 10px 12px;
            border-bottom: 1px solid var(--line);
        }
        table { width: 100%; border-collapse: collapse; }
        th, td {
            font-size: .8rem;
            text-align: left;
            padding: 8px 10px;
            border-bottom: 1px solid #edf2ed;
            vertical-align: top;
            overflow-wrap: anywhere;
        }
        th { font-size: .72rem; text-transform: uppercase; color: #5f6a6d; }
        tr:last-child td { border-bottom: none; }
        .state-ok { color: var(--good); font-weight: 700; }
        .state-warn { color: var(--warn); font-weight: 700; }
        .state-bad { color: var(--bad); font-weight: 700; }
        .muted { color: #6c7579; }
        @media (max-width: 640px) {
            .sections { grid-template-columns: 1fr; }
            .title { font-size: 1.05rem; }
        }
    </style>
</head>
<body>
    <div class="top">
        <div>
            <div class="title">KIS EMS Laufzeitmonitor</div>
            <div class="stamp" id="stamp">Lade Daten...</div>
        </div>
        <div class="chip" id="uptime-chip">Uptime: -</div>
    </div>
    <div class="wrap">
        <div class="grid" id="kpis"></div>
        <div class="chips" id="apps"></div>
        <div class="sections" id="sections"></div>
        <section class="events">
            <h2>Wichtige Ereignisse</h2>
            <div class="events-wrap">
                <table>
                    <thead><tr><th>Zeit</th><th>System</th><th>Wert</th></tr></thead>
                    <tbody id="events-body"></tbody>
                </table>
            </div>
        </section>
    </div>
<script>
function esc(v) {
    if (v === null || v === undefined) return '-';
    return String(v)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
}

function stateClass(v) {
    const t = String(v || '').toLowerCase();
    if (t === 'on' || t === 'ok' || t === 'running') return 'state-ok';
    if (t.includes('error') || t.includes('fail') || t === 'unavailable') return 'state-bad';
    if (t === 'off' || t.includes('warn')) return 'state-warn';
    return 'muted';
}

function buildRows(items) {
    if (!items.length) {
        return '<tr><td colspan="3" class="muted">Keine passenden Entities im Cache.</td></tr>';
    }
    return items.map(e =>
        `<tr>
            <td>${esc(e.entity_id)}</td>
            <td class="${stateClass(e.state)}">${esc(e.state)}</td>
            <td class="muted">${esc(e.unit)}</td>
        </tr>`
    ).join('');
}

function buildEventRows(items) {
    if (!items.length) {
        return '<tr><td colspan="3" class="muted">Noch keine wichtigen Ereignisse erkannt.</td></tr>';
    }
    return items.map(e =>
        `<tr>
            <td>${esc(e.ts)}</td>
            <td>${esc(e.label)}</td>
            <td class="${stateClass(e.state)}">${esc(e.state)}</td>
        </tr>`
    ).join('');
}

async function refresh() {
    const [statusResp, entitiesResp] = await Promise.all([
        fetch('api/status'),
        fetch('api/entities')
    ]);

    const status = await statusResp.json();
    const entities = await entitiesResp.json();

    document.getElementById('stamp').textContent =
        `Letzte Aktualisierung: ${status.now} | States im Cache: ${status.cached_states}`;
    document.getElementById('uptime-chip').textContent = `Uptime: ${status.uptime_s}s`;

    document.getElementById('kpis').innerHTML = [
        { label: 'Geladene Apps', value: status.loaded_apps.length },
        { label: 'Core aktiv', value: status.enabled_flags.enable_core ? 'Ja' : 'Nein' },
        { label: 'Facility aktiv', value: status.enabled_flags.enable_facility ? 'Ja' : 'Nein' },
        { label: 'Storage aktiv', value: status.enabled_flags.enable_storage ? 'Ja' : 'Nein' },
        { label: 'Wallbox aktiv', value: status.enabled_flags.enable_wallbox ? 'Ja' : 'Nein' }
    ].map(k => `<div class="kpi"><div class="label">${k.label}</div><div class="value">${k.value}</div></div>`).join('');

    document.getElementById('apps').innerHTML = status.loaded_apps.length
        ? status.loaded_apps.map(a => `<span class="chip">${esc(a)}</span>`).join('')
        : '<span class="chip">Keine Apps geladen</span>';

    document.getElementById('sections').innerHTML = Object.entries(entities.categories).map(([name, rows]) =>
        `<section class="section">
             <h2>${esc(name)} (${rows.length})</h2>
             <table>
                 <thead><tr><th>Entity</th><th>State</th><th>Einheit</th></tr></thead>
                 <tbody>${buildRows(rows)}</tbody>
             </table>
         </section>`
    ).join('');

    document.getElementById('events-body').innerHTML = buildEventRows(status.event_log || []);
}

refresh().catch(() => {
    document.getElementById('stamp').textContent = 'Fehler beim Laden der Daten';
});
setInterval(() => refresh().catch(() => {}), 5000);
</script>
</body>
</html>
"""


def _extract_unit(state_obj):
        attrs = state_obj.get("attributes") or {}
        return attrs.get("unit_of_measurement", "")


def _category_for_entity(entity_id):
        eid = entity_id.lower()
        if eid.startswith("sensor.ems_") or eid.startswith("input_boolean.ems_") or eid.startswith("input_number.ems_"):
                return "Core"
        if "wallbox" in eid or "ladebox" in eid or eid.startswith("sensor.wb_"):
                return "Wallbox"
        if "redox" in eid or "batterie" in eid or "victron" in eid or "pv_forecast" in eid:
                return "Storage"
        if "wolfcube" in eid or "swli" in eid or "halle" in eid:
                return "Facility"
        if eid.startswith("sensor.grid_") or eid.startswith("sensor.pv_") or eid.startswith("sensor.sun_") or eid.startswith("sensor.dwd_"):
                return "Core"
        return "System"


def _categorized_entities():
        categories = {
                "Core": [],
                "Facility": [],
                "Storage": [],
                "Wallbox": [],
                "System": [],
        }
        for entity_id, state_obj in Hass._state_cache.items():
                category = _category_for_entity(entity_id)
                categories[category].append(
                        {
                                "entity_id": entity_id,
                                "state": state_obj.get("state", "-"),
                                "unit": _extract_unit(state_obj),
                        }
                )

        for key in categories:
                categories[key].sort(key=lambda row: row["entity_id"])

        return categories


    def _normalize_state_value(value):
        if value is None:
            return "-"
        return str(value)


    def _update_important_event_log():
        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        event_log = _RUNTIME["event_log"]
        last_states = _RUNTIME["last_states"]

        for entity_id, label in _IMPORTANT_ENTITY_LABELS.items():
            current_obj = Hass._state_cache.get(entity_id) or {}
            current_state = _normalize_state_value(current_obj.get("state"))
            previous_state = last_states.get(entity_id)

            if previous_state is None:
                last_states[entity_id] = current_state
                continue

            if previous_state != current_state:
                event_log.insert(
                    0,
                    {
                        "ts": now_ts,
                        "entity_id": entity_id,
                        "label": label,
                        "state": current_state,
                        "prev": previous_state,
                    },
                )
                last_states[entity_id] = current_state

        if len(event_log) > _MAX_EVENT_LOG:
            del event_log[_MAX_EVENT_LOG:]


def _make_ui_app(started_at):
        app = web.Application()

        async def _ui(_request):
                return web.Response(text=_HTML, content_type="text/html")

        async def _status(_request):
                now = asyncio.get_running_loop().time()
            _update_important_event_log()
                body = {
                "now": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "uptime_s": int(now - started_at),
                        "loaded_apps": _RUNTIME["loaded_apps"],
                        "enabled_flags": _RUNTIME["enabled_flags"],
                        "cached_states": len(Hass._state_cache),
                "event_log": _RUNTIME["event_log"][:40],
                }
                return web.Response(text=json.dumps(body), content_type="application/json")

        async def _entities(_request):
                body = {"categories": _categorized_entities()}
                return web.Response(text=json.dumps(body), content_type="application/json")

        app.router.add_get("/", _ui)
        app.router.add_get("/api/status", _status)
        app.router.add_get("/api/entities", _entities)
        return app


async def _start_ui_server():
        started_at = asyncio.get_running_loop().time()
        app = _make_ui_app(started_at)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", UI_PORT)
        await site.start()
        logger.info(f"KIS EMS Monitor UI gestartet auf 0.0.0.0:{UI_PORT}")

async def start_apps():
    config_file = "apps.yaml"
    options_file = "/data/options.json"
    
    if not os.path.exists(config_file):
        logger.error(f"Konnte {config_file} nicht finden!")
        return

    # 1. Lade statische App-Klassen aus apps.yaml
    with open(config_file, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # 2. Lade dynamische Add-on Optionen aus Home Assistant (/data/options.json)
    options = {}
    if os.path.exists(options_file):
        try:
            with open(options_file, "r", encoding="utf-8") as f:
                options = json.load(f)
            logger.info("Benutzerdefinierte /data/options.json erfolgreich geladen.")
        except Exception as e:
            logger.error(f"Fehler beim Laden von options.json: {e}")
    else:
        logger.warning(f"{options_file} nicht gefunden. Verwende Standard-Werte aus apps.yaml.")

    # 3. Lade externe Config aus /config/kis-ems.yaml (überschreibt Add-on Optionen)
    ext_config_file = "/config/kis-ems.yaml"
    if os.path.exists(ext_config_file):
        try:
            with open(ext_config_file, "r", encoding="utf-8") as f:
                ext_cfg = yaml.safe_load(f)
                if ext_cfg:
                    options.update(ext_cfg)
            logger.info("Externe /config/kis-ems.yaml erfolgreich verarbeitet.")
        except Exception as e:
            logger.error(f"Fehler beim Laden von /config/kis-ems.yaml: {e}")

    # Initialisiere Home Assistant Verbindung
    await Hass.connect()
    logger.info("Home Assistant API verbunden.")

    loaded_apps = {}
    
    for app_name, app_config in config.items():
        module_name = app_config.get("module")
        class_name = app_config.get("class")
        
        if not module_name or not class_name:
            continue

        source_addon = app_config.get("_source_addon", "ems_core")
        toggle_key = f"enable_{source_addon.replace('ems_', '')}"
        is_enabled = options.get(toggle_key, True)
        if str(is_enabled).lower() == "false":
            logger.info(f"Ueberspringe App {app_name} aus {source_addon}, da {toggle_key} deaktiviert ist.")
            continue
            
        try:
            mod = importlib.import_module(module_name)
            AppClass = getattr(mod, class_name)
            
            # --- DER MAGIC TRICK FÜR MULTI-FIRMEN DEPLOYMENTS ---
            # Überschreibe alle args aus apps.yaml mit den aktuellen Werten aus den HA Add-on Optionen!
            for k, v in options.items():
                if v != "":  # Überschreibe nur, wenn Option gesetzt wurde
                    app_config[k] = v
            # ----------------------------------------------------

            app_instance = AppClass(args=app_config)
            loaded_apps[app_name] = app_instance
            
            app_instance.get_app = lambda name: loaded_apps.get(name)
            
            init_result = app_instance.initialize()
            if asyncio.iscoroutine(init_result):
                await init_result
            logger.info(f"App {app_name} wurde gestartet.")
            
        except Exception as e:
            logger.error(f"Fehler beim Laden von Modul {module_name} (App {app_name}): {e}", exc_info=True)

    _RUNTIME["loaded_apps"] = sorted(loaded_apps.keys())
    _RUNTIME["options"] = options
    _RUNTIME["enabled_flags"] = {
        "enable_core": str(options.get("enable_core", True)).lower() != "false",
        "enable_facility": str(options.get("enable_facility", True)).lower() != "false",
        "enable_storage": str(options.get("enable_storage", True)).lower() != "false",
        "enable_wallbox": str(options.get("enable_wallbox", True)).lower() != "false",
    }

    try:
        await _start_ui_server()
    except Exception as e:
        logger.error(f"KIS EMS Monitor UI konnte nicht gestartet werden: {e}", exc_info=True)

    logger.info("Alle Apps geladen. Warte auf Events...")
        
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    try:
        asyncio.run(start_apps())
    except KeyboardInterrupt:
        pass
