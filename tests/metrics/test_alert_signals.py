"""Alert inputs must describe unfinished work and terminal operational results."""

from __future__ import annotations

import asyncio
import json
from contextlib import suppress

import pytest
from test_channel_metrics import RecordingChannel
from test_gateway_metrics import SequenceProvider, call, sample, spec
from websockets.asyncio.server import serve

from nanobot.agent.runner import AgentRunner
from nanobot.bus.delivery import DeliveryRejectedError, DeliveryRequest
from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.manager import ChannelManager
from nanobot.channels.napcat.runtime import NapcatChannel, NapcatConfig
from nanobot.config.schema import Config
from nanobot.metrics import GatewayMetrics, GatewayMetricsState
from nanobot.providers.base import LLMResponse, LLMUsage


async def test_bus_queue_age_advances_to_next_message_and_clears(monkeypatch):
    metrics = GatewayMetrics(version="test")
    bus = MessageBus(metrics=metrics)
    monkeypatch.setattr("time.time", lambda: 1000.0)
    first = InboundMessage("test", "user", "chat", "first")
    await bus.publish_inbound(first)
    monkeypatch.setattr("time.time", lambda: 1030.0)
    await bus.publish_inbound(InboundMessage("test", "user", "chat", "second"))
    assert bus.inbound_oldest_enqueued_at == 1000.0
    assert await bus.consume_inbound() is first
    assert bus.inbound_oldest_enqueued_at == 1030.0
    await bus.consume_inbound()
    assert bus.inbound_oldest_enqueued_at == 0


async def test_oldest_run_tracks_concurrent_work_and_cancellation(monkeypatch):
    metrics = GatewayMetrics(version="test")
    starts = asyncio.Queue()
    release = [asyncio.Event(), asyncio.Event()]

    class WaitingProvider(SequenceProvider):
        async def chat(self, **kwargs):
            # Identify each independently controlled provider, without inspecting runner state.
            starts.put_nowait(self.index)
            await release[self.index].wait()
            return LLMResponse(content="done", usage=LLMUsage.reported(input_tokens=1, output_tokens=1))

    runners = []
    for index, now in enumerate((1000.0, 1030.0)):
        monkeypatch.setattr("time.time", lambda now=now: now)
        provider = WaitingProvider(iter([]))
        provider.index = index
        task = asyncio.create_task(AgentRunner(metrics=metrics).run(spec(provider)))
        runners.append(task)
        await asyncio.wait_for(starts.get(), 5)
    labels = {"agent_type": "main", "source": "user"}
    assert sample(metrics, "nanobot_agent_oldest_run_start_time_seconds", **labels) == 1000
    release[0].set()
    await runners[0]
    assert sample(metrics, "nanobot_agent_oldest_run_start_time_seconds", **labels) == 1030
    runners[1].cancel()
    with pytest.raises(asyncio.CancelledError):
        await runners[1]
    assert sample(metrics, "nanobot_agent_oldest_run_start_time_seconds", **labels) == 0


def test_primary_alert_counters_export_zero_before_first_result():
    metrics = GatewayMetrics(version="test")
    assert sample(metrics, "nanobot_agent_runs_total", agent_type="main", source="user", outcome="error") == 0
    metrics.register_provider("custom", "selected-agent")
    labels = {"provider": "custom", "model": "selected-agent", "source": "user"}
    assert sample(metrics, "nanobot_llm_requests_total", **labels, outcome="error") == 0
    metrics.record_llm_call(call(None, finish_reason="error"))
    assert sample(metrics, "nanobot_llm_requests_total", **labels, outcome="error") == 1
    metrics.register_provider("custom", "selected-agent")
    assert sample(metrics, "nanobot_llm_requests_total", **labels, outcome="error") == 1


@pytest.mark.parametrize("durable,error,expected", [
    (False, DeliveryRejectedError("rejected"), "failed"),
    (True, OSError("receipt lost"), "unknown"),
])
async def test_send_terminal_result_is_distinct_from_attempts(tmp_path, durable, error, expected):
    metrics = GatewayMetrics(version="test")
    bus = MessageBus(metrics=metrics)

    class FailingChannel(RecordingChannel):
        async def send(self, message):
            raise error

    config = Config.model_validate({"channels": {"websocket": {"enabled": False}}})
    manager = ChannelManager(config, bus, config_path=tmp_path / "config.json", metrics=metrics)
    channel = FailingChannel(bus)
    manager.channels["recording"] = channel
    task = asyncio.create_task(manager.start_all())
    await asyncio.wait_for(channel.started.wait(), 5)
    try:
        labels = {"channel": "recording", "kind": "message", "outcome": expected}
        assert sample(metrics, "nanobot_channel_delivery_results_total", **labels) == 0
        await bus.publish_outbound(OutboundMessage(
            "recording", "chat", "reply",
            delivery=DeliveryRequest("run", "recording:chat", None) if durable else None,
        ))
        async with asyncio.timeout(5):
            while manager.has_pending_delivery:
                await asyncio.sleep(0.01)
        assert sample(metrics, "nanobot_channel_delivery_results_total", **labels) == 1
        assert sample(metrics, "nanobot_channel_oldest_pending_delivery_start_time_seconds", channel="recording") == 0
    finally:
        await manager.stop_all()
        await task


async def test_napcat_connection_state_goes_down_during_reconnect():
    accepted = asyncio.Queue()

    async def onebot(ws):
        accepted.put_nowait(ws)
        async for frame in ws:
            request = json.loads(frame)
            await ws.send(json.dumps({"echo": request["echo"], "status": "ok", "retcode": 0,
                                      "data": {"user_id": 123, "nickname": "fixture"}}))

    async with serve(onebot, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        channel = NapcatChannel(NapcatConfig(ws_url=f"ws://127.0.0.1:{port}"), MessageBus())
        assert channel.is_connected is False
        task = asyncio.create_task(channel.start())
        try:
            peer = await asyncio.wait_for(accepted.get(), 5)
            async with asyncio.timeout(5):
                while not channel.is_connected:
                    await asyncio.sleep(0.01)
            assert channel.is_running is True
            await peer.close()
            async with asyncio.timeout(5):
                while channel.is_connected:
                    await asyncio.sleep(0.01)
            assert channel.is_running is True
            assert channel.is_connected is False
        finally:
            await channel.stop()
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


async def test_pending_send_age_includes_destination_wait_and_clears_on_stop(tmp_path, monkeypatch):
    metrics = GatewayMetrics(version="test")
    bus = MessageBus(metrics=metrics)
    entered = asyncio.Queue()
    release_first = asyncio.Event()

    class WaitingChannel(RecordingChannel):
        async def send(self, message):
            entered.put_nowait(message.content)
            if message.content == "first":
                await release_first.wait()
            else:
                await asyncio.Event().wait()

    config = Config.model_validate({"channels": {"websocket": {"enabled": False}}})
    manager = ChannelManager(config, bus, config_path=tmp_path / "config.json", metrics=metrics)
    channel = WaitingChannel(bus)
    manager.channels["recording"] = channel
    task = asyncio.create_task(manager.start_all())
    await asyncio.wait_for(channel.started.wait(), 5)
    try:
        monkeypatch.setattr("time.time", lambda: 1000.0)
        await bus.publish_outbound(OutboundMessage("recording", "chat", "first"))
        assert await asyncio.wait_for(entered.get(), 5) == "first"
        monkeypatch.setattr("time.time", lambda: 1030.0)
        await bus.publish_outbound(OutboundMessage("recording", "chat", "second"))
        async with asyncio.timeout(5):
            while manager.outbound_deliveries_pending != 2:
                await asyncio.sleep(0.01)
        assert sample(metrics, "nanobot_channel_oldest_pending_delivery_start_time_seconds", channel="recording") == 1000
        release_first.set()
        assert await asyncio.wait_for(entered.get(), 5) == "second"
        assert sample(metrics, "nanobot_channel_oldest_pending_delivery_start_time_seconds", channel="recording") == 1030
        await bus.publish_outbound(OutboundMessage("recording", "chat", "third"))
        async with asyncio.timeout(5):
            while manager.outbound_deliveries_pending != 2:
                await asyncio.sleep(0.01)
    finally:
        await manager.stop_all()
        await task
    assert sample(metrics, "nanobot_channel_oldest_pending_delivery_start_time_seconds", channel="recording") == 0
    labels = {"channel": "recording", "kind": "message"}
    assert sample(metrics, "nanobot_channel_delivery_results_total", **labels, outcome="returned") == 1
    assert sample(metrics, "nanobot_channel_delivery_results_total", **labels, outcome="unknown") == 1
    assert sample(metrics, "nanobot_channel_delivery_results_total", **labels, outcome="cancelled") == 1


async def test_disabling_channel_removes_its_state_samples(tmp_path, monkeypatch):
    from nanobot.config import loader

    payload = {"agents": {"defaults": {"workspace": str(tmp_path / "workspace")}},
               "channels": {"websocket": {"enabled": False}, "napcat": {"enabled": True}}}
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(loader, "_current_config_path", config_path)
    metrics = GatewayMetrics(version="test")
    manager = ChannelManager(Config.model_validate(payload), MessageBus(metrics=metrics), config_path=config_path, metrics=metrics)
    metrics.bind_state(lambda: GatewayMetricsState(
        ready=False, maintenance=False,
        channels={name: status["running"] for name, status in manager.get_status().items()},
        connections=manager.get_connection_status(), queues={}, queue_oldest_enqueued_at={},
        outbound_deliveries_pending=manager.outbound_deliveries_pending,
    ))
    assert sample(metrics, "nanobot_channel_connected", channel="napcat") == 0
    assert sample(metrics, "nanobot_channel_oldest_pending_delivery_start_time_seconds", channel="napcat") == 0
    result = await manager.apply_channel_feature_action("disable", "napcat")
    assert result["ok"] is True
    assert sample(metrics, "nanobot_channel_running", channel="napcat") is None
    assert sample(metrics, "nanobot_channel_connected", channel="napcat") is None
    assert sample(metrics, "nanobot_channel_oldest_pending_delivery_start_time_seconds", channel="napcat") is None
