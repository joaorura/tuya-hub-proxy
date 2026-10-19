"""A fake Wi-Fi Tuya device (protocol 3.3) that fronts one Zigbee sub-device.

Each instance listens on ``bind_ip:6668`` and answers with the child's real
device id and a proxy-generated local key.  To tuya-local it looks like an
ordinary stand-alone Wi-Fi device, so no ``cid`` / gateway logic is involved
on the Home Assistant side.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import sys
import time
from typing import Any, Awaitable, Callable, Protocol

from . import protocol33 as p

log = logging.getLogger(__name__)


class Backend(Protocol):
    """What a fake device needs from the proxy core."""

    def current_dps(self, child_id: str) -> dict[str, Any]: ...
    def set_dps(self, child_id: str, dps: dict[str, Any]) -> Awaitable[bool]: ...
    def request_refresh(self, child_id: str, force: bool = False) -> None: ...


class _Session:
    def __init__(self, dev: "FakeDevice", reader: asyncio.StreamReader,
                 writer: asyncio.StreamWriter) -> None:
        self.dev = dev
        self.reader = reader
        self.writer = writer
        self.peer = writer.get_extra_info("peername")
        self.decoder = p.FrameDecoder(dev.key)
        self.push_seq = 0
        self.lock = asyncio.Lock()  # one writer at a time on this socket

    async def send(self, frame: bytes) -> None:
        async with self.lock:
            self.writer.write(frame)
            await self.writer.drain()

    async def push_status(self, dps: dict[str, Any]) -> None:
        self.push_seq += 1
        body = {"devId": self.dev.device_id, "dps": dps, "t": int(time.time())}
        await self.send(p.encode_response(p.STATUS, self.push_seq, self.dev.key, body))


class FakeDevice:
    def __init__(self, name: str, device_id: str, key: str, host: str, port: int,
                 backend: Backend, idle_timeout: float = 120.0) -> None:
        self.name = name
        self.device_id = device_id
        self.key = key.encode("latin1")
        self.host = host
        self.port = port
        self.backend = backend
        self.idle_timeout = idle_timeout
        self.sessions: set[_Session] = set()
        self.server: asyncio.base_events.Server | None = None
        self.counters = {"connections": 0, "requests": 0, "controls": 0,
                         "pushes": 0, "protocol_errors": 0}

    # ---------------------------------------------------------------- server
    async def start(self) -> None:
        self.server = await asyncio.start_server(self._handle, sock=self._listen_socket())
        sock = self.server.sockets[0].getsockname()
        self.port = sock[1]  # resolves port 0 in tests
        log.info("fake device listening",
                 extra={"child": self.name, "bind": f"{self.host}:{self.port}"})

    def _listen_socket(self) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # IP_FREEBIND (Linux): bind even if the alias IP is not configured yet,
        # so the service does not race NetworkManager at boot.
        if sys.platform.startswith("linux"):
            try:
                sock.setsockopt(socket.IPPROTO_IP, getattr(socket, "IP_FREEBIND", 15), 1)
            except OSError:
                pass
        sock.bind((self.host, self.port))
        sock.listen(16)
        sock.setblocking(False)
        return sock

    async def stop(self) -> None:
        server, self.server = self.server, None
        if server:
            server.close()
        for s in list(self.sessions):
            s.writer.close()
        if server:
            await server.wait_closed()

    async def set_available(self, available: bool) -> None:
        """Offline: drop every client and stop listening, so new connections
        are refused.  Online: listen again on the same address."""
        if available:
            if self.server is None:
                await self.start()
            return
        server, self.server = self.server, None
        if server:
            server.close()
        for s in list(self.sessions):
            s.writer.close()
        if server:
            try:
                await asyncio.wait_for(server.wait_closed(), 2)
            except asyncio.TimeoutError:
                pass

    async def push(self, dps: dict[str, Any]) -> None:
        """Send a spontaneous STATUS (cmd 8) to every connected client."""
        if not dps:
            return
        for s in list(self.sessions):
            try:
                await s.push_status(dps)
                self.counters["pushes"] += 1
            except (ConnectionError, OSError):
                s.writer.close()

    # ------------------------------------------------------------ per client
    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        s = _Session(self, reader, writer)
        self.sessions.add(s)
        self.counters["connections"] += 1
        log.info("client connected", extra={"child": self.name, "peer": str(s.peer)})
        try:
            while True:
                # tuya-local heartbeats every ~5 s; a silent client is gone.
                data = await asyncio.wait_for(reader.read(4096), self.idle_timeout)
                if not data:
                    break
                for req in s.decoder.feed(data):
                    self.counters["requests"] += 1
                    await self._dispatch(s, req)
        except p.ProtocolError as exc:
            self.counters["protocol_errors"] += 1
            log.warning("protocol error, closing client",
                        extra={"child": self.name, "peer": str(s.peer), "error": str(exc)})
        except (asyncio.TimeoutError, ConnectionError, OSError):
            pass
        finally:
            self.sessions.discard(s)
            writer.close()
            log.info("client disconnected", extra={"child": self.name, "peer": str(s.peer)})

    def _status_body(self) -> dict[str, Any]:
        return {"devId": self.device_id, "dps": self.backend.current_dps(self.device_id)}

    async def _dispatch(self, s: _Session, req: p.Request) -> None:
        cmd, seq, data = req.cmd, req.seqno, req.data or {}
        log.debug("request", extra={"child": self.name, "cmd": cmd, "seq": seq})

        if cmd in (p.DP_QUERY, p.DP_QUERY_NEW):
            await s.send(p.encode_response(cmd, seq, self.key, self._status_body()))
            # answer from cache now; refresh from the hub in the background
            self.backend.request_refresh(self.device_id)

        elif cmd == p.HEART_BEAT:
            await s.send(p.encode_response(cmd, seq, self.key))

        elif cmd in (p.CONTROL, p.CONTROL_NEW):
            dps = data.get("dps")
            if dps is None and isinstance(data.get("data"), dict):
                dps = data["data"].get("dps")
            if not isinstance(dps, dict) or not dps:
                await s.send(p.encode_response(cmd, seq, self.key, retcode=1))
                return
            if all(v is None for v in dps.values()):
                # "device22" style query (CONTROL_NEW with null values)
                await s.send(p.encode_response(cmd, seq, self.key, self._status_body()))
                return
            self.counters["controls"] += 1
            try:
                ok = await self.backend.set_dps(self.device_id, dps)
            except Exception as exc:
                log.warning("control failed", extra={"child": self.name, "error": str(exc)})
                ok = False
            # Real firmware acks CONTROL with an empty frame; the new state
            # follows as a STATUS push once the hub confirms it.
            await s.send(p.encode_response(cmd, seq, self.key, retcode=0 if ok else 1))

        elif cmd == p.UPDATEDPS:
            await s.send(p.encode_response(cmd, seq, self.key))
            self.backend.request_refresh(self.device_id, force=True)
            # push what we have so the client gets an answer even if the
            # (sleepy) child never replies through the hub
            await s.push_status(self.backend.current_dps(self.device_id))

        else:
            log.debug("unsupported command ignored", extra={"child": self.name, "cmd": cmd})
            await s.send(p.encode_response(cmd, seq, self.key, retcode=1))
