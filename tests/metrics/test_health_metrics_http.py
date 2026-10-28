"""HTTP format negotiation uses the same listener as health checks."""

import asyncio
import json

import pytest
from prometheus_client.openmetrics.parser import text_string_to_metric_families as parse_openmetrics

from nanobot.gateway.health import start_health_server
from nanobot.metrics import GatewayMetrics, GatewayMetricsState


@pytest.mark.parametrize("enabled", [False, True])
async def test_metrics_opt_in_and_health_status_are_independent(enabled):
    metrics = GatewayMetrics(version="http-test") if enabled else None
    if metrics is not None:
        metrics.bind_state(lambda: GatewayMetricsState(
            ready=False, maintenance=True, channels={}, connections={}, queues={},
            queue_oldest_enqueued_at={}, outbound_deliveries_pending=0,
        ))
    server = await start_health_server(
        "127.0.0.1", 0, health_payload=lambda: (False, {"ready": False, "maintenance": True}),
        metrics=metrics,
    )
    port = server.sockets[0].getsockname()[1]

    async def request(path, accept="", fragmented=False):
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(f"GET {path} HTTP/1.1\r\nHost: localhost\r\n".encode())
        await writer.drain()
        if fragmented:
            await asyncio.sleep(0.02)
        writer.write(f"Accept: {accept}\r\n\r\n".encode())
        await writer.drain()
        response = await reader.read()
        writer.close()
        await writer.wait_closed()
        return response.split(b"\r\n\r\n", 1)

    async with server:
        headers, body = await request("/health")
        assert b"503 Service Unavailable" in headers
        assert json.loads(body) == {"ready": False, "maintenance": True}
        headers, body = await request("/metrics")
        if not enabled:
            assert b"404 Not Found" in headers
        else:
            assert b"200 OK" in headers
            assert b"text/plain; version=0.0.4" in headers
            assert b"nanobot_gateway_ready 0.0" in body
            assert b"nanobot_gateway_maintenance 1.0" in body
            headers, body = await request(
                "/metrics", "application/openmetrics-text; version=1.0.0", fragmented=True,
            )
            assert b"application/openmetrics-text; version=1.0.0" in headers
            assert body.endswith(b"# EOF\n")
            assert list(parse_openmetrics(body.decode()))
