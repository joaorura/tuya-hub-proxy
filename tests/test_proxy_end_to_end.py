"""(b)+(c) Full proxy: mock 3.4 gateway <-> Upstream <-> fake 3.3 devices <-> tinytuya clients."""

from __future__ import annotations

import asyncio
import json

import tinytuya
from conftest import (DOOR_CID, DOOR_ID, DOOR_KEY, SWITCH_CID, SWITCH_ID, SWITCH_KEY,
                      make_config, run, wait_until)
from tinytuya.core import command_types as CT

from tuya_hub_proxy.proxy import Proxy


def client(dev_id, key, port):
    d = tinytuya.Device(dev_id, "127.0.0.1", key, version=3.3, port=port, connection_timeout=2)
    d.set_socketRetryLimit(1)
    d.disabledetect = True
    d.set_socketPersistent(True)
    return d


async def receive_dps(d, tries=4):
    """Read until a frame with DPs arrives (skipping empty acks/timeouts)."""
    for _ in range(tries):
        got = await asyncio.to_thread(d.receive)
        if got and "dps" in got:
            return got
    return None


async def start_proxy(gateway, tmp_path, **opts):
    proxy = Proxy(make_config(gateway.port, tmp_path, **opts))
    await proxy.start()
    assert await wait_until(lambda: proxy.upstream.connected, 5)
    return proxy


def test_initial_sync_and_push_routing_by_cid(gateway, tmp_path):
    async def scenario():
        proxy = await start_proxy(gateway, tmp_path)
        try:
            # initial status() of every child through the single hub session
            assert await wait_until(lambda: proxy.cache.dps(DOOR_ID).get("103") == 87)
            assert await wait_until(lambda: proxy.cache.get(SWITCH_ID).updated_at > 0)

            door = client(DOOR_ID, DOOR_KEY, proxy.fakes[DOOR_ID].port)
            sw = client(SWITCH_ID, SWITCH_KEY, proxy.fakes[SWITCH_ID].port)
            st = await asyncio.to_thread(door.status)
            assert st["dps"] == {"101": False, "103": 87}
            st = await asyncio.to_thread(sw.status)
            assert st["dps"] == {"1": False}

            # push for the door cid reaches only the door's fake device
            gateway.push(DOOR_CID, {"101": True})
            got = await receive_dps(door)
            assert got["dps"] == {"101": True} and got["devId"] == DOOR_ID
            assert await asyncio.to_thread(sw.receive) is None

            # push for the switch cid (flat format) reaches only the switch
            gateway.push(SWITCH_CID, {"1": True}, nested=False)
            got = await receive_dps(sw)
            assert got["dps"] == {"1": True} and got["devId"] == SWITCH_ID
            assert await asyncio.to_thread(door.receive) is None

            assert proxy.cache.dps(DOOR_ID)["101"] is True
            assert proxy.cache.get(DOOR_ID).last_push > 0
            assert gateway.max_active == 1 and gateway.connections_total == 1
            door.close()
            sw.close()
        finally:
            await proxy.stop()

    run(scenario())


def test_control_goes_to_hub_with_cid_and_state_comes_back(gateway, tmp_path):
    async def scenario():
        proxy = await start_proxy(gateway, tmp_path)
        try:
            sw = client(SWITCH_ID, SWITCH_KEY, proxy.fakes[SWITCH_ID].port)
            res = await asyncio.to_thread(sw.set_value, 1, True)
            sent = [d for c, d in gateway.received if c == CT.CONTROL_NEW]
            assert sent and sent[-1]["data"]["cid"] == SWITCH_CID
            assert sent[-1]["data"]["dps"] == {"1": True}
            if not (res and res.get("dps")):  # ack came first; state follows
                res = await receive_dps(sw)
            assert res["dps"] == {"1": True}
            assert gateway.children[SWITCH_CID]["1"] is True
            assert await wait_until(lambda: proxy.cache.dps(SWITCH_ID)["1"] is True)
            sw.close()
        finally:
            await proxy.stop()

    run(scenario())


def test_sleepy_child_keeps_default_and_cached_values(gateway, tmp_path):
    gateway.silent.add(DOOR_CID)
    gateway.children[DOOR_CID] = {"103": 50}

    async def scenario():
        proxy = await start_proxy(gateway, tmp_path)
        try:
            door = client(DOOR_ID, DOOR_KEY, proxy.fakes[DOOR_ID].port)
            st = await asyncio.to_thread(door.status)
            # DP 101 announced from the kind default so tuya-local detects a door sensor
            assert st["dps"] == {"101": False}
            gateway.push(DOOR_CID, {"103": 50})
            got = await receive_dps(door)
            assert got["dps"] == {"103": 50}
            st = await asyncio.to_thread(door.status)
            assert st["dps"] == {"101": False, "103": 50}
            door.close()
        finally:
            await proxy.stop()

    run(scenario())


def test_cache_survives_restart(gateway, tmp_path):
    async def first():
        proxy = await start_proxy(gateway, tmp_path)
        gateway.push(DOOR_CID, {"101": True, "103": 42})
        assert await wait_until(lambda: proxy.cache.dps(DOOR_ID).get("103") == 42)
        await proxy.stop()

    run(first())
    saved = json.loads((tmp_path / "cache.json").read_text())
    assert saved["children"][DOOR_ID]["dps"]["103"] == 42
    assert "101" in saved["children"][DOOR_ID]["dps"]  # real value persisted
    # nothing secret is ever written to the cache
    assert DOOR_KEY not in (tmp_path / "cache.json").read_text()

    gateway.close()  # hub gone: values must come from disk
    proxy2 = Proxy(make_config(gateway.port, tmp_path))
    assert proxy2.cache.dps(DOOR_ID)["103"] == 42
    assert proxy2.cache.dps(DOOR_ID)["101"] is True


# ---------------------------------------------------------------- (c)

def test_reconnect_after_hub_drop_never_overlaps_sessions(gateway, tmp_path):
    async def scenario():
        proxy = await start_proxy(gateway, tmp_path)
        try:
            door = client(DOOR_ID, DOOR_KEY, proxy.fakes[DOOR_ID].port)
            await asyncio.to_thread(door.status)
            for _ in range(3):
                before = gateway.connections_total
                gateway.drop_all()
                assert await wait_until(
                    lambda: gateway.connections_total > before and gateway.active == 1
                    and gateway.sessions == 1 and proxy.upstream.connected, 8)
            assert gateway.max_active == 1, "two hub sessions were open at once"
            assert gateway.max_negotiating == 1, "two negotiations ran concurrently"
            assert proxy.upstream.stats.connects == gateway.negotiations

            # the downstream client never noticed; pushes still flow
            gateway.push(DOOR_CID, {"101": True})
            got = await receive_dps(door)
            assert got["dps"] == {"101": True}
            door.close()
        finally:
            await proxy.stop()

    run(scenario())


def test_914_negotiation_failure_backs_off_and_recovers(gateway, tmp_path):
    gateway.reject_negotiations = 3

    async def scenario():
        proxy = Proxy(make_config(gateway.port, tmp_path, backoff_914=0.4))
        await proxy.start()
        try:
            assert await wait_until(lambda: proxy.upstream.connected, 15)
            st = proxy.upstream.stats
            assert st.errors["914"] == 3
            assert gateway.negotiations == 4
            assert gateway.max_active == 1 and gateway.max_negotiating == 1
            assert st.consecutive_failures == 0
        finally:
            await proxy.stop()

    run(scenario())


def test_commands_wait_for_reconnect_instead_of_racing(gateway, tmp_path):
    async def scenario():
        proxy = await start_proxy(gateway, tmp_path, backoff_min=1.0, backoff_max=1.0)
        try:
            gateway.drop_all()
            assert await wait_until(lambda: not proxy.upstream.connected, 5)
            fut = proxy.upstream.set_dps(SWITCH_ID, {"1": True}, timeout=10)
            ok = await asyncio.wait_for(asyncio.wrap_future(fut), 10)
            assert ok is True
            assert await wait_until(lambda: gateway.children[SWITCH_CID]["1"] is True)
            assert gateway.max_active == 1
        finally:
            await proxy.stop()

    run(scenario())


def test_health_snapshot(gateway, tmp_path):
    async def scenario():
        from tuya_hub_proxy.health import start_health_server

        proxy = await start_proxy(gateway, tmp_path)
        srv = await start_health_server("127.0.0.1", 0, proxy.health)
        port = srv.sockets[0].getsockname()[1]
        try:
            r, w = await asyncio.open_connection("127.0.0.1", port)
            w.write(b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n")
            raw = await r.read()
            head, body = raw.split(b"\r\n\r\n", 1)
            assert head.startswith(b"HTTP/1.1 200")
            doc = json.loads(body)
            assert doc["upstream"]["state"] == "connected"
            assert set(doc["children"]) == {"Test door", "Test switch"}
            assert DOOR_KEY not in body.decode() and "TestHubKey" not in body.decode()
        finally:
            srv.close()
            await proxy.stop()

    run(scenario())
