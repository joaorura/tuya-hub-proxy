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

#: How long after a status query an incoming report counts as its answer
#: (and so is not forwarded as a push).
STATUS_REPLY_WINDOW = 5.0

#: How long after a status query a report from that child still proves the
#: hub is processing our requests.  The gateway answers in order but, when
#: its Zigbee side is busy, 10-40 s late; such a late answer is a liveness
#: signal even though it no longer counts as the reply to the query.
LATE_REPLY_WINDOW = 60.0

#: Name of the gateway itself in the keepalive rotation and the health output.
GATEWAY = "gateway"

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
    down_since: float = field(default_factory=time.time)  # 0 while connected
    drop_reasons: Counter = field(default_factory=Counter)

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
            "session_age": (round(time.time() - self.connected_since, 1)
                            if self.state == "connected" else 0),
            "down_since": self.down_since,
            "next_retry_at": self.next_retry_at,
            "consecutive_failures": self.consecutive_failures,
            "drop_reasons": dict(self.drop_reasons),
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
        quiet_after: float = 15.0,
        probe_timeout: float = 10.0,
        gateway_probe_every: int = 2,
        probe_fail_limit: int = 3,
        probe_miss_limit: int = 2,
        confirm_timeout: float = 8.0,
        set_retries: int = 2,
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
        self._quiet_after = quiet_after
        self._probe_timeout = probe_timeout
        self._gw_every = max(1, int(gateway_probe_every))
        self._fail_limit = max(1, int(probe_fail_limit))
        self._miss_limit = max(1, int(probe_miss_limit))
        self._confirm_timeout = confirm_timeout
        self._set_retries = set_retries
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
        # children with a status query in flight -> when it was sent.  The
        # gateway may never answer (or answer with an error that carries no
        # cid), so entries expire after STATUS_REPLY_WINDOW seconds.
        self._awaiting_status: dict[str, float] = {}
        self._op = "idle"  # last tinytuya operation, for diagnostics
        # -- keepalive rotation (see _keepalive) --
        self._probe_at = 0.0  # when the outstanding probe was sent (0 = none)
        self._probe_target: str | None = None  # GATEWAY or a child id
        self._rotation: list[str] = []  # children that answer status queries
        self._rot_idx = 0
        self._cycle = 0
        self._unprobeable: set[str] = set()  # answer queries with 900 (sleepy sensors)
        self._misses: Counter = Counter()  # consecutive unanswered probes per child
        self._fail_streak: list[str] = []  # targets of consecutive unanswered probes
        self._last_exchange = 0.0  # last answer to something we sent (time.time())
        # target -> {"last_signal", "last_probe_ok", "probes", "missed"}
        self._signals: dict[str, dict[str, Any]] = {
            t: {"last_signal": 0.0, "last_probe_ok": 0.0, "probes": 0, "missed": 0}
            for t in [GATEWAY, *self._child_to_cid]}
        # child_id -> (dps, sent_at, tries) for sets awaiting the hub's report
        self._unconfirmed: dict[str, tuple[dict[str, Any], float, int]] = {}

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

    def down_for(self) -> float:
        """Seconds the upstream session has been down (0 while connected)."""
        if self.connected or not self.stats.down_since:
            return 0.0
        return max(0.0, time.time() - self.stats.down_since)

    def keepalive_snapshot(self) -> dict[str, Any]:
        """Keepalive state for /health (read from another thread: copies only)."""
        signals = {k: dict(v) for k, v in dict(self._signals).items()}
        rotation = list(self._rotation)
        unprobeable = set(self._unprobeable)
        for child_id, sig in signals.items():
            if child_id == GATEWAY:
                sig["keepalive"] = "always"
            elif child_id in rotation:
                sig["keepalive"] = "rotation"
            elif child_id in unprobeable:
                sig["keepalive"] = "not_probeable"
            else:
                sig["keepalive"] = "out"
        return {
            "rotation": [GATEWAY, *rotation],
            "probe_target": self._probe_target if self._probe_at else None,
            "fail_streak": list(self._fail_streak),
            "last_exchange": self._last_exchange,
            "targets": signals,
        }

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
        self._probe_at = 0.0
        self._probe_target = None
        self._fail_streak = []
        self._cycle = 0
        self._last_exchange = time.time()
        self.stats.connects += 1
        self.stats.consecutive_failures = 0
        self.stats.state = "connected"
        self.stats.connected_since = time.time()
        self.stats.down_since = 0.0
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
        self.stats.drop_reasons[reason] += 1
        self.stats.down_since = now
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
        if not self._keepalive():
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
            # header fields only: the payload is never logged
            log.debug("hub frame", extra={
                "cmd": m.cmd, "seq": m.seqno, "retcode": m.retcode,
                "payload_len": len(m.payload or b""), **self._probe_context()})
        if gw.raw_recv:
            self.stats.last_rx = time.time()
        self._handle_result(result, None)
        self._check_socket()

    # -- keepalive / liveness ------------------------------------------------
    def _keepalive(self) -> bool:
        """Keep the hub session alive and detect a dead one.

        The gateway ignores heartbeats and closes a session after ~30 s
        without a request it recognises.  A status query does count, and the
        gateway answers one addressed to itself (no cid) reliably.  So after
        ``quiet_after`` seconds without an answer to anything we sent, the
        next target of a rotation is queried: the gateway every
        ``gateway_probe_every`` cycles, children that answer queries in
        between.

        An unanswered probe alone never drops the session (a child may be
        unpowered).  The session is declared dead only when consecutive probes
        fail on at least two distinct targets, one of them the gateway, or
        when ``probe_fail_limit`` consecutive probes fail.
        """
        now = time.time()
        if self._probe_at:
            if now - self._probe_at <= self._probe_timeout:
                return True
            self._probe_missed(now)
            if self._session_dead():
                streak = ",".join(self._fail_streak)
                self._fail_streak = []
                self._drop("keepalive probes unanswered")
                log.warning("keepalive gave up", extra={"targets": streak})
                return False
            # try the next target right away
            return self._send_probe(self._next_target(), now)
        if now - self._last_exchange > self._quiet_after:
            return self._send_probe(self._next_target(), now)
        return True

    def _probe_context(self) -> dict[str, Any]:
        """The outstanding probe, for DEBUG lines about incoming data."""
        if not self._probe_at:
            return {"probe": None}
        return {"probe": self._probe_target,
                "probe_age": round(time.time() - self._probe_at, 1)}

    def _session_dead(self) -> bool:
        streak = self._fail_streak
        if len(streak) >= self._fail_limit:
            return True
        return GATEWAY in streak and len(set(streak)) >= 2

    def _next_target(self) -> str:
        self._cycle += 1
        if not self._rotation or (self._cycle - 1) % self._gw_every == 0:
            return GATEWAY
        self._rot_idx %= len(self._rotation)
        target = self._rotation[self._rot_idx]
        self._rot_idx += 1
        return target

    def _send_probe(self, target: str, now: float) -> bool:
        log.debug("keepalive probe", extra={"target": target})
        self._probe_at = now
        self._probe_target = target
        sig = self._signals[target]
        sig["probes"] += 1
        if target == GATEWAY:
            self._op = "gateway_query"
            res = self._gw.status(nowait=True)
            if isinstance(res, dict) and "Err" in res:
                self._handle_result(res, None)
                return self._gw is not None
            return self._check_socket()
        self._do_refresh(target)
        return self._gw is not None

    def _probe_answered(self, target: str, now: float) -> None:
        """``target`` (GATEWAY or a child) answered a query we sent."""
        self._signals[target]["last_probe_ok"] = now
        if target != GATEWAY:
            self._misses.pop(target, None)
        if self._probe_target == target:
            self._hub_answered(now)
        else:
            self._last_exchange = now
            self._fail_streak = []

    def _hub_answered(self, now: float) -> None:
        """The hub answered a request: the session is alive and not idle."""
        self._last_exchange = now
        self._fail_streak = []
        self._probe_at = 0.0

    def _probe_missed(self, now: float) -> None:
        target = self._probe_target or GATEWAY
        self._probe_at = 0.0
        self._fail_streak.append(target)
        self._signals[target]["missed"] += 1
        log.info("keepalive probe unanswered", extra={
            "target": target, "streak": len(self._fail_streak)})
        if target != GATEWAY:
            self._misses[target] += 1
            if self._misses[target] >= self._miss_limit:
                self._leave_rotation(target, "never answers")

    def _join_rotation(self, child_id: str) -> None:
        self._unprobeable.discard(child_id)
        if child_id not in self._rotation:
            self._rotation.append(child_id)
            log.info("child joined keepalive rotation", extra={"child": child_id})

    def _leave_rotation(self, child_id: str, why: str) -> None:
        if child_id in self._rotation:
            self._rotation.remove(child_id)
            log.info("child left keepalive rotation", extra={"child": child_id, "why": why})
        self._misses.pop(child_id, None)

    def _signal(self, target: str, now: float) -> None:
        sig = self._signals.get(target)
        if sig is not None:
            sig["last_signal"] = now

    def _child_for_900(self) -> str | None:
        """Which child a cid-less 900 answers: the outstanding child probe, or
        the only child with a status query in flight."""
        if self._probe_at and self._probe_target not in (None, GATEWAY):
            return self._probe_target
        mono = time.monotonic()
        recent = [c for c, t in self._awaiting_status.items() if mono - t < STATUS_REPLY_WINDOW]
        return recent[0] if len(recent) == 1 else None

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
        if self._gw is not None:
            self._check_unconfirmed()
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
        prev = self._unconfirmed.get(cmd.child_id)
        tries = prev[2] + 1 if prev and prev[0] == cmd.dps else 1
        self._unconfirmed[cmd.child_id] = (dict(cmd.dps), time.monotonic(), tries)

    def _check_unconfirmed(self) -> None:
        """Re-send a set the gateway never confirmed.

        The gateway sometimes goes deaf for up to ~30 s before closing the
        session; a command written in that window is silently lost.  Sets are
        idempotent (absolute values, never toggles), so re-sending is safe.
        """
        now = time.monotonic()
        for child_id, (dps, sent, tries) in list(self._unconfirmed.items()):
            if now - sent < self._confirm_timeout:
                continue
            if tries > self._set_retries:
                del self._unconfirmed[child_id]
                log.warning("set never confirmed by hub", extra={"child": child_id, "dps": dps})
                continue
            log.info("set not confirmed, re-sending", extra={"child": child_id, "try": tries + 1})
            self._do_set(_Command("set", child_id, dps, now + self._confirm_timeout))

    def _confirm(self, child_id: str, dps: dict[str, Any]) -> None:
        pending = self._unconfirmed.get(child_id)
        if pending and all(str(k) in dps and dps[str(k)] == v for k, v in pending[0].items()):
            del self._unconfirmed[child_id]

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
        self._awaiting_status[child_id] = time.monotonic()
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
                return False
            now = time.time()
            self._signal(GATEWAY, now)
            if code == "900":
                # the hub answered (it is alive); the child it answered for
                # has no data to give: a sleepy sensor, useless as a probe.
                child = expected_child or self._child_for_900()
                if child:
                    self._awaiting_status.pop(child, None)
                    if child not in self._unprobeable:
                        self._unprobeable.add(child)
                        self._leave_rotation(child, "answers 900")
                        log.info("child not probeable (900)", extra={"child": child})
                # proves the hub hears us, so it settles the outstanding probe
                self._hub_answered(now)
            payload = result.get("Payload")
            # 900 "json obj data unvalid" is how the gateway answers a status
            # query for a sub-device it has no data for (sleeping sensor):
            # expected, counted in the health stats, not worth a log line.
            level = logging.DEBUG if code == "900" else logging.INFO
            log.log(level, "hub returned error", extra={
                "err": code, "child": expected_child,
                "payload": str(payload)[:160] if payload else None})
            return False

        data = result.get("data") if isinstance(result.get("data"), dict) else {}
        cid = result.get("cid") or data.get("cid")
        dev = result.get("device")
        if not cid and dev is not None:
            cid = getattr(dev, "cid", None)
        dps = result.get("dps") or data.get("dps")
        log.debug("hub message", extra={
            "cid": cid, "dps": dps, "keys": sorted(result), **self._probe_context()})
        now = time.time()
        self._signal(GATEWAY, now)
        if not cid:
            if isinstance(dps, dict) and dps:
                # the gateway's own DPs: the answer to a keepalive query
                self._probe_answered(GATEWAY, now)
            else:
                log.debug("hub message without cid ignored", extra={"keys": sorted(result)})
            return False
        child_id = self._cid_to_child.get(cid)
        if child_id is None:
            log.debug("message for unknown cid", extra={"cid": cid})
            return False
        if not isinstance(dps, dict) or not dps:
            return False
        self._signal(child_id, now)
        # A reply to our own status query is not a push; everything else is.
        sent = self._awaiting_status.pop(child_id, None)
        age = time.monotonic() - sent if sent is not None else None
        awaited = age is not None and age < STATUS_REPLY_WINDOW
        is_push = child_id != expected_child and not awaited
        if (age is not None and age < LATE_REPLY_WINDOW) or child_id == expected_child:
            if not awaited and child_id != expected_child:
                log.debug("late status answer", extra={"child": child_id, "age": round(age, 1)})
            # it answers status queries, so it is a valid keepalive target
            self._join_rotation(child_id)
            self._probe_answered(child_id, now)
        dps = {str(k): v for k, v in dps.items()}
        self._confirm(child_id, dps)
        self._on_dps(child_id, dps, is_push)
        return True
