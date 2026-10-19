"""Configuration loading (YAML or JSON)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

#: DPs announced before the hub ever reports them, per child kind.  Only the
#: "identity" DP is defaulted: tuya-local needs it to recognise the device type.
#: Battery/tamper are not invented.
KIND_DEFAULTS: dict[str, dict[str, Any]] = {
    "door_sensor": {"101": False},
    "switch": {"1": False},
    "generic": {},
}


class ConfigError(ValueError):
    pass


@dataclass
class HubConfig:
    device_id: str
    ip: str
    local_key: str = field(repr=False)
    version: float = 3.4
    port: int = 6668

    def __post_init__(self) -> None:
        if len(self.local_key) != 16:
            raise ConfigError("hub local_key must have 16 characters")


@dataclass
class ChildConfig:
    name: str
    device_id: str
    node_id: str
    bind_ip: str
    fake_local_key: str = field(repr=False)
    kind: str = "generic"
    defaults: dict[str, Any] = field(default_factory=dict)
    port: int | None = None  # defaults to proxy.listen_port (6668)

    def __post_init__(self) -> None:
        if len(self.fake_local_key) != 16:
            raise ConfigError(f"{self.name}: fake_local_key must have 16 characters")
        if self.kind not in KIND_DEFAULTS:
            raise ConfigError(f"{self.name}: unknown kind {self.kind!r}")
        self.defaults = {**KIND_DEFAULTS[self.kind],
                         **{str(k): v for k, v in self.defaults.items()}}


@dataclass
class ProxyConfig:
    hub: HubConfig
    children: list[ChildConfig]
    listen_port: int = 6668
    cache_file: str | None = "/var/lib/tuya-hub-proxy/cache.json"
    health_host: str = "127.0.0.1"
    health_port: int = 8765
    heartbeat_interval: float = 10.0
    refresh_interval: float = 300.0  # minimum seconds between upstream polls of one child
    receive_timeout: float = 1.0
    command_timeout: float = 10.0
    backoff_min: float = 2.0
    backoff_max: float = 300.0
    backoff_914: float = 10.0  # minimum wait after a failed session negotiation
    # Some gateways ignore heartbeats and close a session that has been quiet
    # for ~30 s.  After this many seconds without an answer to anything sent,
    # a keepalive status query goes to the next target of a rotation: the
    # gateway itself every gateway_probe_every cycles, children that answer
    # queries in between.  A probe counts as failed after probe_timeout.
    quiet_after: float = 15.0
    probe_timeout: float = 10.0
    gateway_probe_every: int = 2
    # The session is declared dead when consecutive probes fail on two
    # distinct targets one of which is the gateway, or after this many
    # consecutive failures whatever the targets.
    probe_fail_limit: int = 3
    # A child leaves the rotation after this many consecutive unanswered probes.
    probe_miss_limit: int = 2
    # With the hub session down for longer than this, the fake devices close
    # their clients and refuse connections until it is back, so Home Assistant
    # shows the entities as unavailable instead of stale.  0 disables.
    unavailable_after: float = 120.0
    log_level: str = "INFO"
    log_format: str = "json"


def _read(path: Path) -> dict[str, Any]:
    text = path.read_text()
    if path.suffix in (".yaml", ".yml"):
        import yaml  # imported lazily so JSON-only setups do not need PyYAML

        return yaml.safe_load(text) or {}
    return json.loads(text)


def _hub_from_devices_json(path: str, device_id: str) -> dict[str, Any]:
    """Fill hub ip/key from a tinytuya ``devices.json`` (wizard output)."""
    for dev in json.loads(Path(path).read_text()):
        if dev.get("id") == device_id:
            return {"local_key": dev.get("local_key") or dev.get("key") or "", "ip": dev.get("ip") or None,
                    "version": dev.get("version") or None}
    raise ConfigError(f"hub {device_id} not found in {path}")


def parse_config(raw: dict[str, Any]) -> ProxyConfig:
    hub_raw = dict(raw.get("hub") or {})
    if not hub_raw.get("device_id"):
        raise ConfigError("hub.device_id is required")
    if "devices_json" in hub_raw:
        found = _hub_from_devices_json(hub_raw.pop("devices_json"), hub_raw["device_id"])
        for k, v in found.items():
            if v and not hub_raw.get(k):
                hub_raw[k] = v
    if not hub_raw.get("ip"):
        raise ConfigError("hub.ip is required")
    hub_raw["version"] = float(hub_raw.get("version", 3.4))
    hub = HubConfig(**hub_raw)

    children = [ChildConfig(**c) for c in raw.get("children") or []]
    if not children:
        raise ConfigError("at least one child is required")
    opts = {k: v for k, v in (raw.get("proxy") or {}).items()}
    cfg = ProxyConfig(hub=hub, children=children, **opts)
    seen: set[str] = set()
    for c in children:
        port = c.port if c.port is not None else cfg.listen_port
        keys = [f"id:{c.device_id}", f"cid:{c.node_id}"]
        if port:  # port 0 = ephemeral (tests)
            keys.append(f"{c.bind_ip}:{port}")
        for key in keys:
            if key in seen:
                raise ConfigError(f"{c.name}: duplicate {key}")
            seen.add(key)
    return cfg


def load_config(path: str | Path) -> ProxyConfig:
    return parse_config(_read(Path(path)))
