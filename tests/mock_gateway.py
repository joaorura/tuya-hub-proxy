"""A protocol-3.4 Tuya Zigbee gateway simulator, built on tinytuya's primitives.

It implements the device side of the 3.4 session-key negotiation, so the
proxy's real ``tinytuya.Device`` talks to it exactly as it would to a hub.
It records how many sessions are open and negotiating at the same time, which
is what the "never two sessions" tests assert on.
"""

from __future__ import annotations

import hmac
import json
import os
import socket
import struct
import threading
import time
from hashlib import sha256

from tinytuya.core import command_types as CT
from tinytuya.core import header as H
from tinytuya.core.crypto_helper import AESCipher
from tinytuya.core.message_helper import TuyaMessage, pack_message, parse_header, unpack_message

HDR = struct.calcsize(H.MESSAGE_HEADER_FMT_55AA)


class _Conn:
    def __init__(self, gw: "MockGateway", sock: socket.socket) -> None:
        self.gw = gw
        self.sock = sock
        self.key = gw.real_key  # becomes the session key after negotiation
        self.negotiated = False
        self.wlock = threading.Lock()

    # -- framing -------------------------------------------------------
    def _recv_frame(self) -> TuyaMessage | None:
        buf = b""
        while len(buf) < HDR:
            chunk = self.sock.recv(HDR - len(buf))
            if not chunk:
                return None
            buf += chunk
        hdr = parse_header(buf)
        while len(buf) < hdr.total_length:
            chunk = self.sock.recv(hdr.total_length - len(buf))
            if not chunk:
                return None
            buf += chunk
        return unpack_message(buf, hmac_key=self.key, header=hdr, no_retcode=True)

    def send(self, cmd: int, seqno: int, body: bytes | None, *, key: bytes | None = None,
             header: bool | None = None) -> None:
        key = key or self.key
        payload = struct.pack(">I", 0)
        if body:
            if header is None:
                header = cmd not in H.NO_PROTOCOL_HEADER_CMDS
            raw = (H.PROTOCOL_34_HEADER + body) if header else body
            payload += AESCipher(key).encrypt(raw, use_base64=False)
        frame = pack_message(TuyaMessage(seqno, cmd, 0, payload, 0, True,
                                         H.PREFIX_55AA_VALUE, None), hmac_key=key)
        with self.wlock:
            self.sock.sendall(frame)

    def send_json(self, cmd: int, seqno: int, obj: dict) -> None:
        self.send(cmd, seqno, json.dumps(obj, separators=(",", ":")).encode())

    def _decrypt(self, payload: bytes, key: bytes) -> bytes:
        if not payload:
            return b""
        raw = AESCipher(key).decrypt(payload, use_base64=False, decode_text=False)
        if raw.startswith(H.PROTOCOL_VERSION_BYTES_34):
            raw = raw[len(H.PROTOCOL_34_HEADER):]
        return raw

    # -- session ---------------------------------------------------------
    def serve(self) -> None:
        gw = self.gw
        try:
            # step 1: client nonce
            m = self._recv_frame()
            if m is None or m.cmd != CT.SESS_KEY_NEG_START:
                return
            with gw.lock:
                gw.negotiating += 1
                gw.max_negotiating = max(gw.max_negotiating, gw.negotiating)
                gw.negotiations += 1
                reject = gw.reject_negotiations > 0
                if reject:
                    gw.reject_negotiations -= 1
            try:
                if reject:
                    # what a confused hub does: answer garbage -> client reports 914
                    self.send(CT.SESS_KEY_NEG_RESP, m.seqno, os.urandom(48), header=False)
                    time.sleep(0.05)
                    return
                local_nonce = self._decrypt(m.payload, gw.real_key)[:16]
                remote_nonce = os.urandom(16)
                proof = hmac.new(gw.real_key, local_nonce, sha256).digest()
                self.send(CT.SESS_KEY_NEG_RESP, m.seqno, remote_nonce + proof, header=False)
                # step 3: client proves it got our nonce
                m = self._recv_frame()
                if m is None or m.cmd != CT.SESS_KEY_NEG_FINISH:
                    return
                want = hmac.new(gw.real_key, remote_nonce, sha256).digest()
                if self._decrypt(m.payload, gw.real_key)[:32] != want:
                    return
                xored = bytes(a ^ b for a, b in zip(local_nonce, remote_nonce))
                self.key = AESCipher(gw.real_key).encrypt(xored, use_base64=False, pad=False)
                self.negotiated = True
            finally:
                with gw.lock:
                    gw.negotiating -= 1
            self._command_loop()
        except (OSError, ValueError):
            pass
        finally:
            gw._unregister(self)

    def _command_loop(self) -> None:
        gw = self.gw
        while True:
            m = self._recv_frame()
            if m is None:
                return
            body = self._decrypt(m.payload, self.key)
            data = json.loads(body) if body else {}
            gw.received.append((m.cmd, data))
            if gw.mute:
                continue
            if m.cmd == CT.HEART_BEAT:
                if gw.answer_heartbeats:
                    self.send(CT.HEART_BEAT, m.seqno, None)
            elif m.cmd in (CT.DP_QUERY_NEW, CT.DP_QUERY):
                cid = data.get("cid")
                if not cid:
                    gw.gateway_queries += 1
                    if gw.own_dps is not None and not gw.gateway_silent:
                        # the gateway's own DPs, without a cid (as the real hub does)
                        self.send_json(m.cmd, m.seqno, {"dps": gw.own_dps})
                    continue
                if cid in gw.err900:
                    # what the hub says for a sub-device it has no data for
                    self.send(m.cmd, m.seqno, b"json obj data unvalid")
                    continue
                if cid in gw.silent:
                    continue  # sleepy Zigbee device: the hub never answers
                if cid in gw.children:
                    self.send_json(m.cmd, m.seqno, {"cid": cid, "dps": gw.children[cid]})
            elif m.cmd in (CT.CONTROL_NEW, CT.CONTROL):
                if gw.ignore_controls > 0:
                    gw.ignore_controls -= 1
                    continue
                inner = data.get("data", data)
                cid, dps = inner.get("cid"), inner.get("dps", {})
                self.send(m.cmd, m.seqno, None)  # empty ack
                if cid in gw.children:
                    gw.children[cid].update(dps)
                    gw.push(cid, dps)


class MockGateway:
    def __init__(self, real_key: str, children: dict[str, dict]) -> None:
        self.real_key = real_key.encode("latin1")
        self.children = {cid: dict(d) for cid, d in children.items()}
        self.silent: set[str] = set()
        self.err900: set[str] = set()  # children answered with "json obj data unvalid"
        self.own_dps: dict | None = {"4": False, "32": "normal"}
        self.gateway_silent = False  # ignore queries addressed to the gateway itself
        self.gateway_queries = 0
        self.lock = threading.Lock()
        self.conns: list[_Conn] = []
        self.connections_total = 0
        self.max_active = 0
        self.negotiating = 0
        self.max_negotiating = 0
        self.negotiations = 0
        self.reject_negotiations = 0
        self.answer_heartbeats = True
        self.ignore_controls = 0  # silently drop the next N control commands
        self.mute = False  # keep the socket open but never answer anything
        self.received: list[tuple[int, dict]] = []
        self._srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._srv.bind(("127.0.0.1", 0))
        self._srv.listen(8)
        self.port = self._srv.getsockname()[1]
        self._push_seq = 1000
        self._running = True
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        while self._running:
            try:
                sock, _ = self._srv.accept()
            except OSError:
                return
            c = _Conn(self, sock)
            with self.lock:
                self.conns.append(c)
                self.connections_total += 1
                self.max_active = max(self.max_active, len(self.conns))
            threading.Thread(target=c.serve, daemon=True).start()

    def _unregister(self, c: _Conn) -> None:
        with self.lock:
            if c in self.conns:
                self.conns.remove(c)
        try:
            c.sock.close()
        except OSError:
            pass

    @property
    def active(self) -> int:
        with self.lock:
            return len(self.conns)

    @property
    def sessions(self) -> int:
        """Connections that completed the session-key negotiation."""
        with self.lock:
            return sum(1 for c in self.conns if c.negotiated)

    def push(self, cid: str, dps: dict, *, nested: bool = True) -> None:
        """Spontaneous report from a child (cmd 8), as a real hub sends it."""
        self.children.setdefault(cid, {}).update(dps)
        self._push_seq += 1
        if nested:
            obj = {"protocol": 4, "t": int(time.time()), "data": {"cid": cid, "dps": dps}}
        else:
            obj = {"cid": cid, "dps": dps, "t": int(time.time())}
        with self.lock:
            conns = [c for c in self.conns if c.negotiated]
        for c in conns:
            try:
                c.send_json(CT.STATUS, self._push_seq, obj)
            except OSError:
                pass

    def drop_all(self) -> None:
        with self.lock:
            conns = list(self.conns)
        for c in conns:
            try:
                c.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            c.sock.close()

    def close(self) -> None:
        self._running = False
        self._srv.close()
        self.drop_all()
