"""Minimal HTTP endpoint (``GET /health``) bound to localhost.  No dependencies."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Callable

log = logging.getLogger(__name__)


async def start_health_server(host: str, port: int,
                              snapshot: Callable[[], dict[str, Any]]) -> asyncio.base_events.Server:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            line = await asyncio.wait_for(reader.readline(), 5)
            while (await asyncio.wait_for(reader.readline(), 5)) not in (b"\r\n", b"\n", b""):
                pass  # skip headers
            parts = line.decode("latin1").split()
            path = parts[1] if len(parts) > 1 else "/"
            if path.split("?")[0] in ("/health", "/"):
                snap = snapshot()
                status = "200 OK" if snap.get("ok") else "503 Service Unavailable"
                body = json.dumps(snap, indent=1, default=str).encode()
            else:
                status, body = "404 Not Found", b'{"error":"not found"}'
            writer.write(
                f"HTTP/1.1 {status}\r\nContent-Type: application/json\r\n"
                f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode() + body
            )
            await writer.drain()
        except (asyncio.TimeoutError, ConnectionError):
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, host, port, reuse_address=True)
    log.info("health endpoint listening", extra={"bind": f"{host}:{port}"})
    return server
