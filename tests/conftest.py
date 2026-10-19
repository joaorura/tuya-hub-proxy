"""Shared fixtures.  All ids and keys here are fictitious test values."""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

from mock_gateway import MockGateway  # noqa: E402

from tuya_hub_proxy.config import parse_config  # noqa: E402

HUB_ID = "bftesthub0000000000gw1"
HUB_KEY = "TestHubKey012345"
DOOR_ID = "bftestdoor000000000001"
DOOR_CID = "00124b0000000001"
DOOR_KEY = "FakeDoorKey01234"
SWITCH_ID = "bftestswitch00000000s2"
SWITCH_CID = "00124b0000000002"
SWITCH_KEY = "FakeSwitchKey567"


def run(coro, timeout: float = 30):
    return asyncio.run(asyncio.wait_for(coro, timeout))


async def wait_until(pred, timeout: float = 5.0, step: float = 0.05) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return True
        await asyncio.sleep(step)
    return pred()


def make_config(hub_port: int, tmp_path: Path, **proxy_opts):
    raw = {
        "hub": {"device_id": HUB_ID, "ip": "127.0.0.1", "local_key": HUB_KEY,
                "version": 3.4, "port": hub_port},
        "proxy": {
            "cache_file": str(tmp_path / "cache.json"),
            "heartbeat_interval": 2.0,
            "receive_timeout": 0.3,
            "backoff_min": 0.1,
            "backoff_max": 1.0,
            "backoff_914": 0.3,
            "refresh_interval": 0.0,
            **proxy_opts,
        },
        "children": [
            {"name": "Test door", "device_id": DOOR_ID, "node_id": DOOR_CID,
             "bind_ip": "127.0.0.1", "port": 0, "fake_local_key": DOOR_KEY,
             "kind": "door_sensor"},
            {"name": "Test switch", "device_id": SWITCH_ID, "node_id": SWITCH_CID,
             "bind_ip": "127.0.0.1", "port": 0, "fake_local_key": SWITCH_KEY,
             "kind": "switch"},
        ],
    }
    return parse_config(raw)


@pytest.fixture
def gateway():
    gw = MockGateway(HUB_KEY, {DOOR_CID: {"101": False, "103": 87},
                               SWITCH_CID: {"1": False}})
    yield gw
    gw.close()
