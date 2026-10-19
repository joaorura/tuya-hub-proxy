"""Keepalive rotation (gateway + answering children) and fake-device availability."""

from __future__ import annotations

import asyncio

from conftest import DOOR_CID, DOOR_ID, SWITCH_CID, SWITCH_ID, make_config, run, wait_until

from tuya_hub_proxy.proxy import Proxy
from tuya_hub_proxy.upstream import GATEWAY


def _proxy(gateway, tmp_path, **opts):
    base = dict(heartbeat_interval=0.3, quiet_after=1.0, probe_timeout=1.0,
                gateway_probe_every=2)
    base.update(opts)
    return Proxy(make_config(gateway.port, tmp_path, **base))


def _ka(proxy, child_id):
    return proxy.upstream.keepalive_snapshot()["targets"][child_id]


def test_gateway_answer_without_cid_keeps_the_session(gateway, tmp_path):
    """No child answers queries: the gateway's own (cid-less) reply must be
    recognised as the probe answer."""
    gateway.answer_heartbeats = False
    gateway.silent.update({DOOR_CID, SWITCH_CID})

    async def scenario():
        proxy = _proxy(gateway, tmp_path)
        await proxy.start()
        try:
            assert await wait_until(lambda: proxy.upstream.connected, 5)
            await asyncio.sleep(6)
            assert proxy.upstream.stats.disconnects == 0
            assert gateway.connections_total == 1
            assert gateway.gateway_queries >= 2
            gw = _ka(proxy, GATEWAY)
            assert gw["last_probe_ok"] > 0 and gw["missed"] == 0
            health = proxy.health()
            assert health["keepalive"]["gateway"]["last_probe_ok"] == gw["last_probe_ok"]
            assert health["keepalive"]["rotation"] == [GATEWAY]
        finally:
            await proxy.stop()

    run(scenario())


def test_unanswered_gateway_probes_alone_drop_after_limit(gateway, tmp_path):
    gateway.answer_heartbeats = False
    gateway.silent.update({DOOR_CID, SWITCH_CID})
    gateway.gateway_silent = True

    async def scenario():
        proxy = _proxy(gateway, tmp_path, probe_fail_limit=3)
        await proxy.start()
        try:
            assert await wait_until(lambda: proxy.upstream.stats.disconnects >= 1, 10)
            assert proxy.upstream.stats.drop_reasons["keepalive probes unanswered"] >= 1
            assert gateway.gateway_queries >= 3
        finally:
            await proxy.stop()

    run(scenario())


def test_offline_child_in_rotation_does_not_drop(gateway, tmp_path):
    """A switch that answered before and then loses power must not take the
    session down while the gateway still answers; it leaves the rotation."""
    gateway.answer_heartbeats = False
    gateway.silent.add(DOOR_CID)

    async def scenario():
        proxy = _proxy(gateway, tmp_path)
        await proxy.start()
        try:
            assert await wait_until(
                lambda: _ka(proxy, SWITCH_ID)["keepalive"] == "rotation", 5)
            gateway.silent.add(SWITCH_CID)  # unpowered
            assert await wait_until(lambda: _ka(proxy, SWITCH_ID)["missed"] >= 2, 15)
            assert _ka(proxy, SWITCH_ID)["keepalive"] == "out"
            await asyncio.sleep(3)
            assert proxy.upstream.stats.disconnects == 0
            assert gateway.connections_total == 1
            assert _ka(proxy, GATEWAY)["last_probe_ok"] > 0
        finally:
            await proxy.stop()

    run(scenario())


def test_gateway_and_second_target_failing_drops(gateway, tmp_path):
    gateway.answer_heartbeats = False
    gateway.silent.add(DOOR_CID)

    async def scenario():
        # a high generic limit: only the "gateway + another target" rule can fire
        proxy = _proxy(gateway, tmp_path, probe_fail_limit=50)
        await proxy.start()
        try:
            assert await wait_until(
                lambda: _ka(proxy, SWITCH_ID)["keepalive"] == "rotation", 5)
            gateway.gateway_silent = True
            gateway.silent.add(SWITCH_CID)
            assert await wait_until(lambda: proxy.upstream.stats.disconnects >= 1, 10)
            st = proxy.upstream.stats
            assert st.drop_reasons["keepalive probes unanswered"] == st.disconnects
            assert _ka(proxy, GATEWAY)["missed"] >= 1
            assert _ka(proxy, SWITCH_ID)["missed"] >= 1
        finally:
            gateway.gateway_silent = False
            await proxy.stop()

    run(scenario())


def test_sleepy_sensor_answering_900_leaves_rotation(gateway, tmp_path):
    gateway.answer_heartbeats = False
    gateway.silent.add(SWITCH_CID)

    async def scenario():
        proxy = _proxy(gateway, tmp_path)
        await proxy.start()
        try:
            assert await wait_until(
                lambda: _ka(proxy, DOOR_ID)["keepalive"] == "rotation", 5)
            gateway.err900.add(DOOR_CID)  # fell asleep: hub has no data for it
            assert await wait_until(
                lambda: _ka(proxy, DOOR_ID)["keepalive"] == "not_probeable", 10)
            assert DOOR_ID not in proxy.upstream.keepalive_snapshot()["rotation"]
            await asyncio.sleep(3)
            assert proxy.upstream.stats.disconnects == 0
            # still receives its pushes
            gateway.push(DOOR_CID, {"101": True})
            assert await wait_until(lambda: proxy.cache.dps(DOOR_ID).get("101") is True)
            assert _ka(proxy, DOOR_ID)["last_signal"] > 0
        finally:
            await proxy.stop()

    run(scenario())


def test_fake_devices_unavailable_while_hub_down_and_back_after(gateway, tmp_path):
    async def scenario():
        proxy = Proxy(make_config(gateway.port, tmp_path, unavailable_after=1.0,
                                  backoff_min=0.2, backoff_max=0.3, backoff_914=0.2))
        await proxy.start()
        try:
            assert await wait_until(lambda: proxy.upstream.connected, 5)
            port = proxy.fakes[DOOR_ID].port
            r, w = await asyncio.open_connection("127.0.0.1", port)

            gateway.reject_negotiations = 10_000  # hub refuses every new session
            gateway.drop_all()
            assert await wait_until(lambda: not proxy.upstream.connected, 5)
            await asyncio.sleep(0.5)
            assert proxy.available  # a short outage is not reported
            assert await wait_until(lambda: not proxy.available, 5)
            assert await asyncio.wait_for(r.read(), 3) == b""  # client closed
            w.close()
            try:
                _, w2 = await asyncio.open_connection("127.0.0.1", port)
            except ConnectionRefusedError:
                pass
            else:
                w2.close()
                raise AssertionError("fake device still accepts connections")
            assert proxy.health()["available"] is False

            gateway.reject_negotiations = 0
            assert await wait_until(lambda: proxy.upstream.connected, 10)
            assert await wait_until(lambda: proxy.available, 5)
            r3, w3 = await asyncio.open_connection("127.0.0.1", port)
            w3.close()
            assert proxy.fakes[SWITCH_ID].server is not None
        finally:
            await proxy.stop()

    run(scenario())
