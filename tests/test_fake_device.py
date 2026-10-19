"""(a) The fake 3.3 device against a real tinytuya client (as tuya-local uses)."""

from __future__ import annotations

import asyncio

import tinytuya
from conftest import DOOR_ID, DOOR_KEY, run, wait_until

from tuya_hub_proxy import protocol33 as p
from tuya_hub_proxy.fake_device import FakeDevice


class StubBackend:
    """Stands in for the hub: echoes every CONTROL back as a STATUS push."""

    def __init__(self) -> None:
        self.dps = {"1": False, "101": False, "103": 90}
        self.sets: list[dict] = []
        self.refreshes: list[tuple[str, bool]] = []
        self.fake: FakeDevice | None = None

    def current_dps(self, child_id):
        return dict(self.dps)

    async def set_dps(self, child_id, dps):
        self.sets.append(dps)
        self.dps.update(dps)

        async def echo():
            await asyncio.sleep(0.05)
            await self.fake.push(dps)

        asyncio.ensure_future(echo())
        return True

    def request_refresh(self, child_id, force=False):
        self.refreshes.append((child_id, force))


def client(port: int) -> tinytuya.Device:
    d = tinytuya.Device(DOOR_ID, "127.0.0.1", DOOR_KEY, version=3.3, port=port,
                        connection_timeout=3)
    d.set_socketRetryLimit(1)
    d.disabledetect = True  # tuya-local does this for an explicit "3.3"
    return d


async def _started():
    be = StubBackend()
    fake = FakeDevice("door", DOOR_ID, DOOR_KEY, "127.0.0.1", 0, be)
    be.fake = fake
    await fake.start()
    return be, fake


def test_status_set_value_heartbeat_and_push():
    async def scenario():
        be, fake = await _started()
        try:
            d = client(fake.port)
            d.set_socketPersistent(True)

            st = await asyncio.to_thread(d.status)
            assert st["devId"] == DOOR_ID
            assert st["dps"] == {"1": False, "101": False, "103": 90}
            assert be.refreshes == [(DOOR_ID, False)]

            res = await asyncio.to_thread(d.set_value, 1, True)
            assert be.sets == [{"1": True}]
            assert res and res["dps"] == {"1": True}

            hb = await asyncio.to_thread(d.heartbeat, False)
            assert hb is None or "Err" not in hb

            # spontaneous push from the "hub"
            be.dps["101"] = True
            await fake.push({"101": True})
            got = await asyncio.to_thread(d.receive)
            assert got["dps"] == {"101": True}
            assert got["devId"] == DOOR_ID

            # a second status reflects the new state
            st = await asyncio.to_thread(d.status)
            assert st["dps"]["1"] is True and st["dps"]["101"] is True
            assert fake.counters["protocol_errors"] == 0
            d.close()
        finally:
            await fake.stop()

    run(scenario())


def test_set_multiple_values_nowait_like_tuya_local():
    async def scenario():
        be, fake = await _started()
        try:
            d = client(fake.port)
            d.set_socketPersistent(True)
            await asyncio.to_thread(d.set_multiple_values, {"1": True, "101": False}, True)
            assert await wait_until(lambda: be.sets == [{"1": True, "101": False}])
            # the echo arrives as a STATUS push on the same socket
            got = await asyncio.to_thread(d.receive)
            if got is None:  # first frame may be the empty CONTROL ack
                got = await asyncio.to_thread(d.receive)
            assert got["dps"] == {"1": True, "101": False}
            d.close()
        finally:
            await fake.stop()

    run(scenario())


def test_updatedps_triggers_refresh_and_push():
    async def scenario():
        be, fake = await _started()
        try:
            d = client(fake.port)
            d.set_socketPersistent(True)
            res = await asyncio.to_thread(d.updatedps, [101])
            assert res["dps"]["101"] is False
            assert (DOOR_ID, True) in be.refreshes
            d.close()
        finally:
            await fake.stop()

    run(scenario())


def test_wrong_key_is_rejected_without_crashing_server():
    async def scenario():
        be, fake = await _started()
        try:
            bad = tinytuya.Device(DOOR_ID, "127.0.0.1", "WrongKey00000000", version=3.3,
                                  port=fake.port, connection_timeout=2)
            bad.set_socketRetryLimit(1)
            r = await asyncio.to_thread(bad.status)
            assert r is None or "Err" in r
            assert await wait_until(lambda: fake.counters["protocol_errors"] == 1)
            # server still serves a correct client afterwards
            st = await asyncio.to_thread(client(fake.port).status)
            assert st["dps"]["101"] is False
        finally:
            await fake.stop()

    run(scenario())


def test_frame_roundtrip_matches_tinytuya_bytes():
    """Our encoder must produce the same bytes tinytuya's client would."""
    key = DOOR_KEY.encode()
    d = tinytuya.Device(DOOR_ID, "127.0.0.1", DOOR_KEY, version=3.3)
    payload = d.generate_payload(tinytuya.CONTROL, {"1": True})
    theirs = d._encode_message(payload)
    seq = d.seqno - 1
    ours = p.encode_request(p.CONTROL, seq, key, None)  # header check below
    dec = p.FrameDecoder(key).feed(theirs)
    assert len(dec) == 1 and dec[0].cmd == p.CONTROL and dec[0].data["dps"] == {"1": True}
    rebuilt = p.encode_request(p.CONTROL, seq, key, dec[0].data)
    assert rebuilt == theirs
    assert ours.startswith(b"\x00\x00\x55\xaa") and ours.endswith(b"\x00\x00\xaa\x55")
