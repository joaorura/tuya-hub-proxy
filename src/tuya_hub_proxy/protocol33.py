"""Server side of the Tuya LAN protocol 3.3 (the "device" end of the socket).

Framing (55AA format, as used by protocol 3.1-3.4)::

    00 00 55 AA | seqno (u32) | cmd (u32) | length (u32) |
    [retcode (u32), only device -> client] | payload | crc32 (u32) | 00 00 AA 55

``length`` counts everything after the length field (retcode + payload + crc +
suffix).  For 3.3 the payload is AES-128-ECB (PKCS#7) encrypted with the
device local key.  Every command except the ones in
``tinytuya.NO_PROTOCOL_HEADER_CMDS`` carries a plain ``b"3.3" + 12 * b"\\0"``
header in front of the ciphertext.

All byte-level work (header parsing, CRC, AES) is delegated to tinytuya so the
bytes we emit are exactly what tinytuya-based clients (tuya-local) expect.
"""

from __future__ import annotations

import json
import struct
from dataclasses import dataclass
from typing import Any

from tinytuya.core import command_types as CT
from tinytuya.core import header as H
from tinytuya.core.crypto_helper import AESCipher
from tinytuya.core.exceptions import DecodeError
from tinytuya.core.message_helper import (
    TuyaMessage,
    pack_message,
    parse_header,
    unpack_message,
)

VERSION_BYTES = H.PROTOCOL_VERSION_BYTES_33
VERSION_HEADER = H.PROTOCOL_33_HEADER  # b"3.3" + 12 zero bytes
HEADER_LEN = struct.calcsize(H.MESSAGE_HEADER_FMT_55AA)
#: Smallest client frame: header + crc + suffix (empty payload, no retcode).
MIN_CLIENT_FRAME = HEADER_LEN + struct.calcsize(H.MESSAGE_END_FMT_55AA)

# Re-exported command ids used by the proxy.
CONTROL = CT.CONTROL
STATUS = CT.STATUS
HEART_BEAT = CT.HEART_BEAT
DP_QUERY = CT.DP_QUERY
CONTROL_NEW = CT.CONTROL_NEW
DP_QUERY_NEW = CT.DP_QUERY_NEW
UPDATEDPS = CT.UPDATEDPS


class ProtocolError(Exception):
    """Unrecoverable framing error: the connection should be dropped."""


@dataclass(frozen=True)
class Request:
    """A decoded client -> device frame."""

    seqno: int
    cmd: int
    data: dict[str, Any] | None  # decoded JSON, None for an empty payload
    crc_good: bool


def _cipher(key: bytes) -> AESCipher:
    return AESCipher(key)


def encrypt_payload(cmd: int, body: bytes, key: bytes) -> bytes:
    """Encrypt ``body`` and add the 3.3 version header when ``cmd`` needs it."""
    if not body:
        return b""
    enc = _cipher(key).encrypt(body, use_base64=False)
    if cmd not in H.NO_PROTOCOL_HEADER_CMDS:
        enc = VERSION_HEADER + enc
    return enc


def decrypt_payload(payload: bytes, key: bytes) -> dict[str, Any] | None:
    """Decrypt a 3.3 payload (with or without version header) into JSON."""
    if not payload:
        return None
    if payload.startswith(VERSION_BYTES):
        payload = payload[len(VERSION_HEADER):]
    if not payload:
        return None
    if len(payload) % 16:
        raise ProtocolError(f"ciphertext length {len(payload)} is not a multiple of 16")
    try:
        raw = _cipher(key).decrypt(payload, use_base64=False, decode_text=False)
    except Exception as exc:  # wrong key -> bad padding
        raise ProtocolError(f"cannot decrypt payload (wrong key?): {exc}") from exc
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError(f"payload is not JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ProtocolError("payload JSON is not an object")
    return data


def encode_response(
    cmd: int,
    seqno: int,
    key: bytes,
    data: dict[str, Any] | None = None,
    retcode: int = 0,
) -> bytes:
    """Build a device -> client frame (always carries a retcode)."""
    body = b""
    if data is not None:
        # tinytuya sends compact JSON; real firmware does the same.
        body = json.dumps(data, separators=(",", ":")).encode("utf-8")
    payload = struct.pack(H.MESSAGE_RETCODE_FMT, retcode) + encrypt_payload(cmd, body, key)
    msg = TuyaMessage(seqno, cmd, retcode, payload, 0, True, H.PREFIX_55AA_VALUE, None)
    return pack_message(msg)


def encode_request(cmd: int, seqno: int, key: bytes, data: dict[str, Any] | None) -> bytes:
    """Build a client -> device frame (no retcode).  Used by tests/tools."""
    body = json.dumps(data, separators=(",", ":")).encode() if data is not None else b""
    msg = TuyaMessage(seqno, cmd, 0, encrypt_payload(cmd, body, key), 0, True,
                      H.PREFIX_55AA_VALUE, None)
    return pack_message(msg)


class FrameDecoder:
    """Incremental decoder for the client -> device byte stream."""

    def __init__(self, key: bytes) -> None:
        self._key = key
        self._buf = bytearray()

    def feed(self, data: bytes) -> list[Request]:
        self._buf += data
        out: list[Request] = []
        while True:
            # resync on the 55AA prefix, discarding garbage
            idx = self._buf.find(H.PREFIX_55AA_BIN)
            if idx < 0:
                # keep a possible partial prefix at the end
                del self._buf[: max(0, len(self._buf) - 3)]
                return out
            if idx:
                del self._buf[:idx]
            if len(self._buf) < HEADER_LEN:
                return out
            try:
                hdr = parse_header(bytes(self._buf[:HEADER_LEN]))
            except DecodeError as exc:
                raise ProtocolError(str(exc)) from exc
            if len(self._buf) < hdr.total_length:
                return out
            frame = bytes(self._buf[: hdr.total_length])
            del self._buf[: hdr.total_length]
            try:
                msg = unpack_message(frame, header=hdr, no_retcode=True)
            except DecodeError as exc:
                raise ProtocolError(str(exc)) from exc
            if not msg.crc_good:
                # Real firmware silently drops corrupt frames; so do we.
                continue
            out.append(Request(msg.seqno, msg.cmd, decrypt_payload(msg.payload, self._key),
                               msg.crc_good))
