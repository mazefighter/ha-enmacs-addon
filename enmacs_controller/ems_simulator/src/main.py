import asyncio
import logging
import yaml
import importlib
import sys
import os
import json

from ha_wrapper import Hass

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ems.main")

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

    # Initialisiere Home Assistant Verbindung
    await Hass.connect()
    logger.info("Home Assistant API verbunden.")

    loaded_apps = {}
    
    for app_name, app_config in config.items():
        module_name = app_config.get("module")
        class_name = app_config.get("class")
        
        if not module_name or not class_name:
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
            
            app_instance.initialize()
            logger.info(f"App {app_name} wurde gestartet.")
            
        except Exception as e:
            logger.error(f"Fehler beim Laden von Modul {module_name} (App {app_name}): {e}", exc_info=True)

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
