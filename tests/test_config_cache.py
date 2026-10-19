"""Config parsing, cache semantics and config generation."""

from __future__ import annotations

import json

import pytest
import yaml
from conftest import DOOR_ID, HUB_ID, HUB_KEY, SWITCH_ID, make_config

from tuya_hub_proxy.cache import DpsCache
from tuya_hub_proxy.config import ConfigError, load_config, parse_config
from tuya_hub_proxy.genconfig import build


def test_kind_defaults_and_secret_not_in_repr(tmp_path):
    cfg = make_config(6668, tmp_path)
    door = cfg.children[0]
    assert door.defaults == {"101": False}
    assert cfg.children[1].defaults == {"1": False}
    assert door.fake_local_key not in repr(door)
    assert HUB_KEY not in repr(cfg)


def test_rejects_bad_keys_and_duplicates(tmp_path):
    base = {"hub": {"device_id": HUB_ID, "ip": "192.0.2.1", "local_key": HUB_KEY},
            "children": [{"name": "a", "device_id": "x1", "node_id": "n1",
                          "bind_ip": "192.0.2.20", "fake_local_key": "short"}]}
    with pytest.raises(ConfigError):
        parse_config(base)
    base["children"][0]["fake_local_key"] = "A" * 16
    base["children"].append(dict(base["children"][0], name="b", device_id="x2", node_id="n2"))
    with pytest.raises(ConfigError, match="duplicate"):
        parse_config(base)


def test_hub_key_from_devices_json(tmp_path):
    dj = tmp_path / "devices.json"
    dj.write_text(json.dumps([{"id": HUB_ID, "local_key": HUB_KEY, "ip": "192.0.2.9"}]))
    cfgfile = tmp_path / "c.yaml"
    cfgfile.write_text(yaml.safe_dump({
        "hub": {"device_id": HUB_ID, "ip": "192.0.2.1", "devices_json": str(dj)},
        "children": [{"name": "a", "device_id": "x1", "node_id": "n1",
                      "bind_ip": "192.0.2.20", "fake_local_key": "B" * 16,
                      "kind": "switch"}]}))
    cfg = load_config(cfgfile)
    assert cfg.hub.local_key == HUB_KEY
    assert cfg.hub.ip == "192.0.2.1"  # explicit ip wins over devices.json


def test_cache_merge_defaults_and_persist(tmp_path):
    c = DpsCache(tmp_path / "c.json")
    c.register(DOOR_ID, {"101": False})
    c.register(SWITCH_ID, {"1": False})
    assert c.dps(DOOR_ID) == {"101": False}
    assert c.update(DOOR_ID, {"103": 80}) == {"103": 80}
    assert c.update(DOOR_ID, {"103": 80}) == {}  # unchanged
    c.update(DOOR_ID, {101: True}, push=True)
    assert c.dps(DOOR_ID) == {"101": True, "103": 80}
    c.maybe_save(force=True)
    on_disk = json.loads((tmp_path / "c.json").read_text())
    assert on_disk["children"][SWITCH_ID]["dps"] == {}  # defaults are not persisted

    c2 = DpsCache(tmp_path / "c.json")
    c2.register(DOOR_ID, {"101": False})
    c2.register(SWITCH_ID)
    c2.load()
    assert c2.dps(DOOR_ID) == {"101": True, "103": 80}
    assert c2.get(DOOR_ID).last_push > 0


def test_genconfig_builds_children_with_unique_keys():
    devices = [
        {"id": HUB_ID, "local_key": HUB_KEY, "category": "wg2"},
        {"id": "child-a", "node_id": "n-a", "gateway_id": HUB_ID, "category": "mcs", "name": "A"},
        {"id": "child-b", "node_id": "n-b", "gateway_id": HUB_ID, "category": "kg", "name": "B"},
        {"id": "other", "node_id": "n-c", "gateway_id": "another-hub", "category": "kg"},
    ]
    cfg = build(devices, HUB_ID, "192.0.2.1", "192.0.2.20")
    kids = cfg["children"]
    assert [k["device_id"] for k in kids] == ["child-a", "child-b"]
    assert [k["bind_ip"] for k in kids] == ["192.0.2.20", "192.0.2.21"]
    assert [k["kind"] for k in kids] == ["door_sensor", "switch"]
    keys = [k["fake_local_key"] for k in kids]
    assert all(len(k) == 16 for k in keys) and len(set(keys)) == 2
    assert cfg["hub"]["local_key"] == HUB_KEY
    parse_config(cfg)
    ref = build(devices, HUB_ID, "192.0.2.1", "192.0.2.20", "/x/devices.json")
    assert "local_key" not in ref["hub"] and ref["hub"]["devices_json"] == "/x/devices.json"
