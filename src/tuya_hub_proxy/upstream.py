"""The single upstream connection to the Tuya Zigbee gateway.

Why a dedicated thread instead of asyncio: tinytuya is a blocking library and
its gateway object owns one socket, one session key and one sequence number.
The whole point of this proxy is that *exactly one* code path ever touches
that socket.  A single worker thread does all of it -- connect/negotiate,
send, heartbeat, receive -- so two negotiations or two concurrent reads are
impossible by construction (that concurrency is what produces error 914 when
several tuya-local entries share one hub).

The asyncio side talks to the worker only through a command queue (with
``concurrent.futures.Future`` results) and receives DP updates through a
callback that must be thread-safe (the proxy wraps it in
``loop.call_soon_threadsafe``).
"""

from __future__ import annotations

import logging
import os
import queue
import random
import select
import threading
import time
from collections import Counter
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Any, Callable

import tinytuya

from .config import HubConfig

log = logging.getLogger(__name__)

# tinytuya error codes that mean "the session is gone"
LINK_ERRORS = {"901", "902", "905", "906", "914"}
ERR_KEY_OR_VER = "914"

DpsCallback = Callable[[str, dict[str, Any], bool], None]  # child_id, dps, is_push


class UpstreamError(Exception):
    pass


@dataclass
class _Command:
    kind: str  # "set" | "refresh"
    child_id: str
    dps: dict[str, Any] | None
    deadline: float
    future: Future = field(default_factory=Future)


@dataclass
class UpstreamStats:
    state: str = "init"  # init | connecting | connected | backoff | stopped
    connects: int = 0  # successful session negotiations (incl. tinytuya-internal ones)
    connect_attempts: int = 0
    disconnects: int = 0
    errors: Counter = field(default_factory=Counter)
    last_error: str | None = None
    last_rx: float = 0.0
    connected_since: float = 0.0
    next_retry_at: float = 0.0
    consecutive_failures: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "connects": self.connects,
            "connect_attempts": self.connect_attempts,
            "disconnects": self.disconnects,
            "errors": dict(self.errors),
            "last_error": self.last_error,
            "last_rx": self.last_rx,
            "connected_since": self.connected_since,
            "next_retry_at": self.next_retry_at,
            "consecutive_failures": self.consecutive_failures,
        }


class Upstream:
    """Owns the gateway socket.  All public methods are thread-safe."""

    def __init__(
        self,
        hub: HubConfig,
        children: dict[str, str],  # child device_id -> node_id (cid)
        on_dps: DpsCallback,
        *,
        heartbeat_interval: float = 10.0,
        receive_timeout: float = 1.0,
        connect_timeout: float = 5.0,
        backoff_min: float = 2.0,
        backoff_max: float = 300.0,
        backoff_914: float = 10.0,
        device_factory: Callable[..., Any] = tinytuya.Device,
    ) -> None:
        self._hub = hub
        self._child_to_cid = dict(children)
        self._cid_to_child = {v: k for k, v in children.items()}
        self._on_dps = on_dps
        self._hb_interval = heartbeat_interval
        self._rx_timeout = receive_timeout
        self._connect_timeout = connect_timeout
        self._backoff_min = backoff_min
        self._backoff_max = backoff_max
        self._backoff_914 = backoff_914
        self._factory = device_factory

        self.stats = UpstreamStats()
        self._cmds: queue.Queue[_Command] = queue.Queue()
        self._wake_r, self._wake_w = os.pipe()
        os.set_blocking(self._wake_r, False)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

        # only touched by the worker thread
        self._gw: Any = None
        self._kids: dict[str, Any] = {}  # child_id -> tinytuya child Device
        self._sock_id: int | None = None
        self._last_hb = 0.0
        self._pending_refresh: set[str] = set()
        self._awaiting_status: set[str] = set()  # children with a query in flight
        self._op = "idle"  # last tinytuya operation, for diagnostics

    # ------------------------------------------------------------------ API
    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="tuya-upstream", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        self._wake()
        if self._thread:
            self._thread.join(timeout)
        for fd in (self._wake_r, self._wake_w):
            try:
                os.close(fd)
            except OSError:
                pass

    @property
    def connected(self) -> bool:
        return self.stats.state == "connected"

    def set_dps(self, child_id: str, dps: dict[str, Any], timeout: float = 10.0) -> Future:
        return self._submit(_Command("set", child_id, dict(dps), time.monotonic() + timeout))

    def refresh(self, child_id: str, timeout: float = 30.0) -> Future:
        return self._submit(_Command("refresh", child_id, None, time.monotonic() + timeout))

    # -------------------------------------------------------------- internals
    def _submit(self, cmd: _Command) -> Future:
        if cmd.child_id not in self._child_to_cid:
            cmd.future.set_exception(UpstreamError(f"unknown child {cmd.child_id}"))
            return cmd.future
        self._cmds.put(cmd)
        self._wake()
        return cmd.future

    def _wake(self) -> None:
        try:
            os.write(self._wake_w, b"x")
        except OSError:
            pass

    def _drain_wake(self) -> None:
        try:
            while os.read(self._wake_r, 512):
                pass
        except (BlockingIOError, OSError):
            pass

    def _run(self) -> None:
        log.info("upstream worker started", extra={"hub_ip": self._hub.ip})
        while not self._stop.is_set():
            try:
                if self._gw is None:
                    if not self._wait_backoff():
                        continue
                    self._connect()
                    continue
                self._loop_once()
            except Exception as exc:  # never let the worker die
                log.exception("upstream worker error: %s", exc)
                self._drop("exception")
        self._close()
        self.stats.state = "stopped"
        self._fail_all(UpstreamError("proxy stopping"))
        log.info("upstream worker stopped")

    # -- connection management --------------------------------------------
    def _backoff_delay(self) -> float:
        n = max(0, self.stats.consecutive_failures - 1)
        base = min(self._backoff_max, self._backoff_min * (2 ** n))
        return base * random.uniform(0.8, 1.2)

    def _wait_backoff(self) -> bool:
        """Sleep until the next retry time, still expiring queued commands."""
        now = time.monotonic()
        if now >= self.stats.next_retry_at:
            return True
        self.stats.state = "backoff"
        self._expire_commands()
        wait = min(1.0, self.stats.next_retry_at - now)
        select.select([self._wake_r], [], [], wait)
        self._drain_wake()
        return False

    def _connect(self) -> None:
        self.stats.state = "connecting"
        self.stats.connect_attempts += 1
        hub = self._hub
        gw = self._factory(
            hub.device_id, hub.ip, hub.local_key, version=hub.version, persist=True,
            connection_timeout=self._connect_timeout, connection_retry_limit=1,
            connection_retry_delay=0, port=hub.port,
        )
        gw.set_socketPersistent(True)
        gw.set_socketRetryLimit(1)
        kids = {
            child_id: self._factory(cid, cid=cid, parent=gw)
            for child_id, cid in self._child_to_cid.items()
        }
        # _get_socket() opens TCP and runs the 3.4 session-key negotiation.
        # It is the only place a negotiation starts, and we are the only thread.
        res = gw._get_socket(False)
        if res is not True:
            code = str(res) if res else "905"
            self._fail_connect(code, gw)
            return
        gw.set_socketTimeout(self._rx_timeout)
        self._gw, self._kids = gw, kids
        self._sock_id = id(gw.socket)
        self.stats.connects += 1
        self.stats.consecutive_failures = 0
        self.stats.state = "connected"
        self.stats.connected_since = time.time()
        self.stats.last_rx = time.time()
        self._last_hb = time.monotonic()
        log.info("upstream connected", extra={"hub_ip": hub.ip})
        # full resync: pushes may have been lost while we were away
        self._pending_refresh = set(self._child_to_cid)

    def _fail_connect(self, code: str, gw: Any) -> None:
        try:
            gw.close()
        except Exception:
            pass
        self._record_error(code, "connect failed")
        self.stats.consecutive_failures += 1
        delay = self._backoff_delay()
        if code == ERR_KEY_OR_VER:
            # 914 on a hub that used to work means it is refusing/garbling the
            # negotiation (often because another client is hammering it).
            # Back off harder; never retry immediately.
            delay = max(delay, self._backoff_914)
        self.stats.next_retry_at = time.monotonic() + delay
        self.stats.state = "backoff"
        log.warning("upstream connect failed, retrying later",
                    extra={"err": code, "retry_in": round(delay, 1)})

    def _drop(self, reason: str) -> None:
        if self._gw is None:
            return
        now = time.time()
        log.warning("upstream session dropped", extra={
            "reason": reason,
            "session_age": round(now - self.stats.connected_since, 1),
            "since_rx": round(now - self.stats.last_rx, 1),
            "since_hb": round(time.monotonic() - self._last_hb, 1),
            "during": self._op,
        })
        self._close()
        self.stats.disconnects += 1
        self.stats.consecutive_failures += 1
        delay = self._backoff_delay()
        if reason == ERR_KEY_OR_VER:
            delay = max(delay, self._backoff_914)
        self.stats.next_retry_at = time.monotonic() + delay
        self.stats.state = "backoff"

    def _close(self) -> None:
        if self._gw is not None:
            try:
                self._gw.set_socketPersistent(False)
                self._gw.close()
            except Exception:
                pass
        self._gw = None
        self._kids = {}
        self._sock_id = None

    def _record_error(self, code: str, msg: str) -> None:
        self.stats.errors[code] += 1
        self.stats.last_error = f"{code}: {msg}"

    def _check_socket(self) -> bool:
        """Detect a reconnect done silently inside tinytuya, or a lost socket."""
        gw = self._gw
        if gw is None:
            return False
        if gw.socket is None:
            self._drop("socket closed")
            return False
        if id(gw.socket) != self._sock_id:
            # tinytuya re-opened (and re-negotiated) inside the same thread.
            # Still a single session, but count it and resync state.
            self._sock_id = id(gw.socket)
            self.stats.connects += 1
            self._pending_refresh = set(self._child_to_cid)
            log.info("upstream session re-established by tinytuya")
        return True

    # -- main loop ----------------------------------------------------------
    def _loop_once(self) -> None:
        gw = self._gw
        self._process_commands()
        if self._gw is None:
            return
        if self._pending_refresh:
            child_id = self._pending_refresh.pop()
            self._do_refresh(child_id)
            return
        now = time.monotonic()
        if now - self._last_hb >= self._hb_interval:
            self._last_hb = now
            self._op = "heartbeat"
            self._handle_result(gw.heartbeat(nowait=True), None)
            if not self._check_socket():
                return
            if time.time() - self.stats.last_rx > max(3 * self._hb_interval, 30):
                self._drop("no traffic from hub")
                return
        # Wait for hub data OR a command, whichever comes first.
        if not gw.received_wrong_cid_queue:
            try:
                r, _, _ = select.select([gw.socket, self._wake_r], [], [], self._rx_timeout)
            except (OSError, ValueError, TypeError):
                self._drop("socket error")
                return
            if self._wake_r in r:
                self._drain_wake()
            if gw.socket not in r:
                return
        self._op = "receive"
        result = gw.receive()
        for m in gw.raw_recv:
            if not m.payload:
                log.debug("hub empty frame", extra={"cmd": m.cmd, "retcode": m.retcode})
        if gw.raw_recv:
            self.stats.last_rx = time.time()
        self._handle_result(result, None)
        self._check_socket()

    def _expire_commands(self) -> None:
        keep: list[_Command] = []
        now = time.monotonic()
        while True:
            try:
                cmd = self._cmds.get_nowait()
            except queue.Empty:
                break
            if now > cmd.deadline:
                if not cmd.future.done():
                    cmd.future.set_exception(UpstreamError("hub unavailable (timeout)"))
            else:
                keep.append(cmd)
        for cmd in keep:
            self._cmds.put(cmd)

    def _fail_all(self, exc: Exception) -> None:
        while True:
            try:
                cmd = self._cmds.get_nowait()
            except queue.Empty:
                return
            if not cmd.future.done():
                cmd.future.set_exception(exc)

    def _process_commands(self) -> None:
        while self._gw is not None:
            try:
                cmd = self._cmds.get_nowait()
            except queue.Empty:
                return
            if time.monotonic() > cmd.deadline:
                cmd.future.set_exception(UpstreamError("command expired before hub was ready"))
                continue
            if cmd.kind == "set":
                self._do_set(cmd)
            elif cmd.kind == "refresh":
                self._pending_refresh.discard(cmd.child_id)
                ok = self._do_refresh(cmd.child_id)
                cmd.future.set_result(ok)

    def _do_set(self, cmd: _Command) -> None:
        kid = self._kids[cmd.child_id]
        log.info("upstream set", extra={"child": cmd.child_id, "dps": cmd.dps})
        # nowait: the hub answers with an ack + a STATUS push, both of which
        # arrive through the receive loop like any other push.
        self._op = "set"
        res = kid.set_multiple_values(cmd.dps, nowait=True)
        if isinstance(res, dict) and "Err" in res:
            self._record_error(str(res["Err"]), res.get("Error", "set failed"))
            cmd.future.set_exception(UpstreamError(f"hub error {res['Err']}"))
            if str(res["Err"]) in LINK_ERRORS:
                self._drop(str(res["Err"]))
            return
        if not self._check_socket():
            cmd.future.set_exception(UpstreamError("hub connection lost"))
            return
        cmd.future.set_result(True)

    def _do_refresh(self, child_id: str) -> bool:
        """Send a status query for one child without waiting for the answer.

        Sleepy Zigbee children (and a hub whose cache was reset) often never
        answer.  A blocking ``status()`` would then time out, and tinytuya
        reacts to a send timeout by closing the socket and *renegotiating* --
        an extra session per silent child.  So the query is fire-and-forget;
        any answer arrives through the normal receive path, routed by cid.
        """
        kid = self._kids.get(child_id)
        if kid is None:
            return False
        self._op = "status_query"
        res = kid.status(nowait=True)
        if isinstance(res, dict) and "Err" in res:
            self._handle_result(res, child_id)
            return False
        self._awaiting_status.add(child_id)
        return self._check_socket()

    # -- result dispatch ----------------------------------------------------
    def _handle_result(self, result: Any, expected_child: str | None) -> bool:
        """Route one decoded tinytuya result.  Returns True if it carried DPs."""
        if not result or not isinstance(result, dict):
            return False
        if "Err" in result:
            code = str(result["Err"])
            if code == "904":  # empty/undecodable read, harmless on 3.4
                return False
            self._record_error(code, str(result.get("Error", "")))
            if code in LINK_ERRORS:
                self._drop(code)
            else:
                payload = result.get("Payload")
                log.info("hub returned error", extra={
                    "err": code, "child": expected_child,
                    "payload": str(payload)[:160] if payload else None})
            return False

        data = result.get("data") if isinstance(result.get("data"), dict) else {}
        cid = result.get("cid") or data.get("cid")
        dev = result.get("device")
        if not cid and dev is not None:
            cid = getattr(dev, "cid", None)
        dps = result.get("dps") or data.get("dps")
        if not cid:
            log.debug("hub message without cid ignored", extra={"keys": sorted(result)})
            return False
        child_id = self._cid_to_child.get(cid)
        if child_id is None:
            log.debug("message for unknown cid", extra={"cid": cid})
            return False
        if not isinstance(dps, dict) or not dps:
            return False
        # A reply to our own status query is not a push; everything else is.
        is_push = child_id != expected_child and child_id not in self._awaiting_status
        self._awaiting_status.discard(child_id)
        self._on_dps(child_id, {str(k): v for k, v in dps.items()}, is_push)
        return True
