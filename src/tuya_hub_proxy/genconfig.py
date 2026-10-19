"""Generate a proxy config from a tinytuya ``devices.json``.

    tuya-hub-proxy-genconfig --devices-json ~/.config/tuya/devices.json \\
        --hub-id <gateway id> --hub-ip <gateway ip> --first-ip 192.0.2.20 \\
        -o /etc/tuya-hub-proxy/config.yaml

Every child of the hub gets the next IP of the range and a fresh random
16-character fake local key.  The output file is created with mode 0600 and
keys are never printed.  The hub local key is copied from devices.json into
the config (so the service does not need access to the user's home).  An existing file is never overwritten (keys already
registered in tuya-local would be lost) unless ``--force`` is given.
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
import secrets
import string
import sys
from pathlib import Path

import yaml

CATEGORY_KIND = {"mcs": "door_sensor", "kg": "switch"}
_ALPHABET = string.ascii_letters + string.digits + "!#$%&()*+,-./:;<=>?@[]^_{|}~"


def fake_key() -> str:
    """16 printable chars, like the keys Tuya issues (AES-128 key as text)."""
    return "".join(secrets.choice(_ALPHABET) for _ in range(16))


def build(devices: list[dict], hub_id: str, hub_ip: str, first_ip: str,
          devices_json_path: str | None = None) -> dict:
    """Build the config dict.

    The hub key is copied into the config (which is written 0600) unless
    ``devices_json_path`` is given, in which case it is referenced instead and
    read at runtime -- handy when the service user can read devices.json.
    """
    def parent(d: dict) -> str | None:
        return d.get("gateway_id") or d.get("parent")

    kids = [d for d in devices
            if d.get("node_id") and d.get("id") != hub_id and parent(d) in (hub_id, None)]
    if not kids:
        raise SystemExit(f"no children of hub {hub_id} with node_id in devices.json")
    ip = ipaddress.ip_address(first_ip)
    children = []
    for i, d in enumerate(kids):
        children.append({
            "name": d.get("name") or d["id"],
            "device_id": d["id"],
            "node_id": d["node_id"],
            "bind_ip": str(ip + i),
            "fake_local_key": fake_key(),
            "kind": CATEGORY_KIND.get(d.get("category", ""), "generic"),
        })
    hub: dict = {"device_id": hub_id, "ip": hub_ip, "version": 3.4}
    if devices_json_path:
        hub["devices_json"] = devices_json_path
    else:
        gw = next((d for d in devices if d.get("id") == hub_id), None)
        key = (gw or {}).get("local_key") or (gw or {}).get("key")
        if not key:
            raise SystemExit(f"hub {hub_id} has no local_key in devices.json")
        hub["local_key"] = key
    return {"hub": hub, "proxy": {"cache_file": "/var/lib/tuya-hub-proxy/cache.json"},
            "children": children}


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(prog="tuya-hub-proxy-genconfig", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--devices-json", required=True)
    ap.add_argument("--hub-id", required=True)
    ap.add_argument("--hub-ip", required=True)
    ap.add_argument("--first-ip", required=True, help="first fake-device IP (consecutive)")
    ap.add_argument("-o", "--output", required=True)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--reference-devices-json", action="store_true",
                    help="read the hub key from devices.json at runtime instead of copying it")
    a = ap.parse_args(argv)

    devices = json.loads(Path(a.devices_json).expanduser().read_text())
    ref = str(Path(a.devices_json).expanduser().resolve()) if a.reference_devices_json else None
    cfg = build(devices, a.hub_id, a.hub_ip, a.first_ip, ref)
    out = Path(a.output)
    if out.exists() and not a.force:
        raise SystemExit(f"{out} exists; refusing to overwrite (use --force)")
    out.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as fh:
        yaml.safe_dump(cfg, fh, sort_keys=False, allow_unicode=True)
    print(f"wrote {out} with {len(cfg['children'])} children:", file=sys.stderr)
    for c in cfg["children"]:
        print(f"  {c['bind_ip']:15} {c['kind']:12} {c['name']}", file=sys.stderr)


if __name__ == "__main__":
    main()
