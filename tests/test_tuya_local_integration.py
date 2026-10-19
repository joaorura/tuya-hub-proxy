"""(d) Run tuya-local's own ``TuyaLocalDevice`` against the proxy.

Home Assistant itself is not installed: the handful of HA symbols that
``device.py`` imports are stubbed.  Point ``TUYA_LOCAL_PATH`` at a checkout of
https://github.com/make-all/tuya-local to run it; otherwise it is skipped.
"""

from __future__ import annotations

import asyncio
import importlib
import os
import re
import sys
import types
from pathlib import Path

import pytest
import yaml
from conftest import DOOR_CID, DOOR_ID, DOOR_KEY, SWITCH_CID, SWITCH_ID, SWITCH_KEY, run, wait_until

from tuya_hub_proxy.proxy import Proxy

TL = os.environ.get("TUYA_LOCAL_PATH")
pytestmark = [
    pytest.mark.skipif(
        not TL or not (Path(TL) / "custom_components/tuya_local/device.py").exists(),
        reason="set TUYA_LOCAL_PATH to a make-all/tuya-local checkout",
    ),
    pytest.mark.skipif(sys.version_info < (3, 14), reason="tuya-local requires Python 3.14+"),
]


def _install_ha_stubs() -> None:
    if "homeassistant" in sys.modules and getattr(sys.modules["homeassistant"], "_stub", False):
        return
    ha = types.ModuleType("homeassistant")
    ha._stub = True
    const = types.ModuleType("homeassistant.const")
    const.CONF_HOST, const.CONF_NAME = "host", "name"
    const.EVENT_HOMEASSISTANT_STARTED = "homeassistant_started"
    const.EVENT_HOMEASSISTANT_STOP = "homeassistant_stop"
    core = types.ModuleType("homeassistant.core")
    core.HomeAssistant = object
    core.callback = lambda f: f
    util = types.ModuleType("homeassistant.util")
    util.slugify = lambda s, separator="_": re.sub(r"[^a-z0-9]+", separator, str(s).lower()).strip(separator)
    uyaml = types.ModuleType("homeassistant.util.yaml")

    def load_yaml(path):
        with open(path, encoding="utf-8") as fh:
            return yaml.safe_load(fh)

    uyaml.load_yaml = load_yaml
    for name, mod in {"homeassistant": ha, "homeassistant.const": const,
                      "homeassistant.core": core, "homeassistant.util": util,
                      "homeassistant.util.yaml": uyaml}.items():
        sys.modules[name] = mod


def _import_device_module():
    _install_ha_stubs()
    root = Path(TL) / "custom_components"
    cc = types.ModuleType("custom_components")
    cc.__path__ = [str(root)]
    tl = types.ModuleType("custom_components.tuya_local")
    tl.__path__ = [str(root / "tuya_local")]
    tl.DOMAIN = "tuya_local"  # skip the real __init__ (needs full HA)
    sys.modules.setdefault("custom_components", cc)
    sys.modules.setdefault("custom_components.tuya_local", tl)
    return importlib.import_module("custom_components.tuya_local.device")


class FakeHass:
    def __init__(self) -> None:
        self.data = {"tuya_local": {}}
        self.is_stopping = False
        self.is_running = True
        self.bus = types.SimpleNamespace(async_listen_once=lambda *a, **k: (lambda: None))

    async def async_add_executor_job(self, func, *args):
        return await asyncio.get_running_loop().run_in_executor(None, func, *args)

    def async_create_task(self, coro):
        return asyncio.ensure_future(coro)


class FakeEntity:
    entity_id = "binary_sensor.test"

    def __init__(self) -> None:
        self._config = types.SimpleNamespace(dps=lambda: [])
        self.received: list[dict] = []

    def on_receive(self, poll, full_poll):
        self.received.append(dict(poll))

    def schedule_update_ha_state(self, *a):
        pass

    def async_schedule_update_ha_state(self, *a):
        pass


def test_tuya_local_device_against_proxy(gateway, tmp_path):
    from conftest import make_config

    devmod = _import_device_module()

    async def scenario():
        proxy = Proxy(make_config(gateway.port, tmp_path))
        await proxy.start()
        hass = FakeHass()
        try:
            assert await wait_until(lambda: proxy.upstream.connected, 5)
            assert await wait_until(lambda: proxy.cache.dps(DOOR_ID).get("103") == 87)

            # --- door sensor, configured exactly as a user would: host, id, key, 3.3, no cid
            door = devmod.TuyaLocalDevice("Proxy door", DOOR_ID, "127.0.0.1", DOOR_KEY,
                                          3.3, None, hass)
            door._api.port = proxy.fakes[DOOR_ID].port  # real setup: 6668 on bind_ip
            await door.async_refresh()
            assert door.get_property("101") is False
            assert door.get_property("103") == 87

            # config-flow style type detection picks the Zigbee door sensor
            names = {c.config_type for c in await door.async_possible_types()}
            assert "tuya_smart_zigbee_door_sensor" in names

            # persistent receive loop gets hub pushes
            ent = FakeEntity()
            door.register_entity(ent)
            await asyncio.sleep(1.0)
            gateway.push(DOOR_CID, {"101": True})
            assert await wait_until(lambda: door.get_property("101") is True, 15)
            assert any(p.get("101") is True for p in ent.received)

            # --- switch: control from HA reaches the hub with the right cid
            sw = devmod.TuyaLocalDevice("Proxy switch", SWITCH_ID, "127.0.0.1", SWITCH_KEY,
                                        3.3, None, hass)
            sw._api.port = proxy.fakes[SWITCH_ID].port
            await sw.async_refresh()
            assert sw.get_property("1") is False
            await sw.async_set_property("1", True)
            assert await wait_until(lambda: gateway.children[SWITCH_CID]["1"] is True, 5)

            assert gateway.max_active == 1
            await door.async_unregister_entity(ent)
        finally:
            await proxy.stop()

    run(scenario(), timeout=60)
