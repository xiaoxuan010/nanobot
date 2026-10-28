"""Small shared HTTP listener for gateway health and optional metrics."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from email.parser import BytesHeaderParser
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nanobot.metrics import GatewayMetrics

_MAX_CONNECTIONS = 64
_READ_TIMEOUT_SECONDS = 2.0
_MAX_REQUEST_HEAD_BYTES = 4096


async def start_health_server(
    host: str,
    port: int,
    *,
    health_payload: Callable[[], tuple[bool, dict[str, object]]],
    metrics: GatewayMetrics | None = None,
) -> asyncio.Server:
    """Bind the existing health contract and return its caller-owned server."""
    slots = asyncio.Semaphore(_MAX_CONNECTIONS)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if slots.locked():
            writer.close()
            return
        async with slots:
            try:
                data = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=_READ_TIMEOUT_SECONDS)
                request_line, raw_headers = data.split(b"\r\n", 1)
                parts = request_line.decode("utf-8", errors="replace").split(" ")
                method, path = parts[:2] if len(parts) >= 2 else ("", "")
                if method == "GET" and path == "/health":
                    ready, payload = health_payload()
                    body = json.dumps(payload).encode()
                    status = "200 OK" if ready else "503 Service Unavailable"
                    content_type = "application/json"
                elif method == "GET" and path == "/metrics" and metrics is not None:
                    headers = BytesHeaderParser().parsebytes(raw_headers)
                    body, content_type = metrics.render(headers.get("Accept", ""))
                    status = "200 OK"
                else:
                    body, status, content_type = b"Not Found", "404 Not Found", "text/plain"
                head = (
                    f"HTTP/1.0 {status}\r\n"
                    f"Content-Type: {content_type}\r\n"
                    f"Content-Length: {len(body)}\r\n"
                    "Connection: close\r\n\r\n"
                ).encode()
                writer.write(head + body)
                await writer.drain()
            except (asyncio.TimeoutError, ConnectionError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
                pass
            finally:
                writer.close()

    return await asyncio.start_server(handle, host, port, limit=_MAX_REQUEST_HEAD_BYTES - 4)
