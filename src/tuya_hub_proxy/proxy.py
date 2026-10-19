"""Wires the upstream gateway session, the DP cache and the fake devices."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable

from .cache import DpsCache
from .config import ProxyConfig
from .fake_device import FakeDevice
from .upstream import Upstream

log = logging.getLogger(__name__)

#: How long a CONTROL ack waits for the command to reach the hub.  The command
#: itself stays queued up to ``command_timeout``; tuya-local sends with
#: nowait=True, so this only matters for clients that wait for the ack.
ACK_WAIT = 2.0


class Proxy:
    def __init__(self, cfg: ProxyConfig, upstream_factory: Callable[..., Upstream] = Upstream,
                 **upstream_kwargs: Any) -> None:
        self.cfg = cfg
        self.cache = DpsCache(cfg.cache_file)
        self.fakes: dict[str, FakeDevice] = {}
        self._last_refresh: dict[str, float] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self.started_at = time.time()
        for c in cfg.children:
            self.cache.register(c.device_id, c.defaults)
        self.cache.load()
        self.upstream = upstream_factory(
            cfg.hub,
            {c.device_id: c.node_id for c in cfg.children},
            self._on_dps_threadsafe,
            heartbeat_interval=cfg.heartbeat_interval,
            receive_timeout=cfg.receive_timeout,
            backoff_min=cfg.backoff_min,
            backoff_max=cfg.backoff_max,
            backoff_914=cfg.backoff_914,
            **upstream_kwargs,
        )

    # ------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        for c in self.cfg.children:
            fake = FakeDevice(c.name, c.device_id, c.fake_local_key, c.bind_ip,
                              c.port if c.port is not None else self.cfg.listen_port, self)
            await fake.start()
            self.fakes[c.device_id] = fake
        self.upstream.start()
        self._saver = asyncio.create_task(self._save_loop())

    async def stop(self) -> None:
        self._saver.cancel()
        for fake in self.fakes.values():
            await fake.stop()
        await asyncio.to_thread(self.upstream.stop)
        self.cache.maybe_save(force=True)

    async def _save_loop(self) -> None:
        while True:
            await asyncio.sleep(5)
            self.cache.maybe_save()

    # ------------------------------------------------- upstream -> clients
    def _on_dps_threadsafe(self, child_id: str, dps: dict[str, Any], is_push: bool) -> None:
        loop = self._loop
        if loop is not None and not loop.is_closed():
            loop.call_soon_threadsafe(self._on_dps, child_id, dps, is_push)

    def _on_dps(self, child_id: str, dps: dict[str, Any], is_push: bool) -> None:
        changed = self.cache.update(child_id, dps, push=is_push)
        log.info("dps from hub", extra={"child": child_id, "dps": dps, "push": is_push,
                                        "changed": sorted(changed)})
        fake = self.fakes.get(child_id)
        # Forward pushes as-is (an event may legitimately repeat a value, e.g.
        # a second door-open report); for poll answers only forward changes.
        out = dps if is_push else changed
        if fake and out:
            asyncio.ensure_future(fake.push(out))

    # ---------------------------------------------- Backend (fake devices)
    def current_dps(self, child_id: str) -> dict[str, Any]:
        return self.cache.dps(child_id)

    async def set_dps(self, child_id: str, dps: dict[str, Any]) -> bool:
        fut = self.upstream.set_dps(child_id, dps, timeout=self.cfg.command_timeout)
        try:
            return await asyncio.wait_for(asyncio.shield(asyncio.wrap_future(fut)), ACK_WAIT)
        except asyncio.TimeoutError:
            # still queued (hub reconnecting); ack now like real firmware would
            return True

    def request_refresh(self, child_id: str, force: bool = False) -> None:
        now = time.monotonic()
        last = self._last_refresh.get(child_id, 0.0)
        min_gap = 5.0 if force else self.cfg.refresh_interval
        if now - last < min_gap or not self.upstream.connected:
            return
        self._last_refresh[child_id] = now
        fut = self.upstream.refresh(child_id)
        fut.add_done_callback(lambda f: f.exception())  # silence "never retrieved"

    # ------------------------------------------------------------- health
    def health(self) -> dict[str, Any]:
        children = {}
        for c in self.cfg.children:
            st = self.cache.get(c.device_id)
            fake = self.fakes.get(c.device_id)
            children[c.name] = {
                "device_id": c.device_id,
                "bind": f"{c.bind_ip}:{fake.port if fake else '?'}",
                "clients": len(fake.sessions) if fake else 0,
                "dps": st.merged(),
                "last_update": st.updated_at,
                "last_push": st.last_push,
                "counters": dict(fake.counters) if fake else {},
            }
        up = self.upstream.stats.as_dict()
        return {
            "ok": up["state"] == "connected",
            "uptime": round(time.time() - self.started_at, 1),
            "upstream": up,
            "children": children,
        }
