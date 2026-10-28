"""Observe adapter attempts through the public bus and manager lifecycle."""

import asyncio

from prometheus_client.parser import text_string_to_metric_families

from nanobot.bus.events import OutboundMessage
from nanobot.bus.outbound_events import StreamDeltaEvent, StreamedResponseEvent
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.channels.manager import ChannelManager
from nanobot.config.schema import Config
from nanobot.metrics import GatewayMetrics


class RecordingChannel(BaseChannel):
    name = "recording"
    display_name = "Recording"

    def __init__(self, bus):
        super().__init__({}, bus)
        self.started = asyncio.Event()
        self.stopped = asyncio.Event()
        self.delivered = asyncio.Event()
        self.streamed = asyncio.Event()
        self.attempts = 0

    async def start(self):
        self._running = True
        self.started.set()
        await self.stopped.wait()

    async def stop(self):
        self._running = False
        self.stopped.set()

    async def send(self, msg):
        self.attempts += 1
        if self.attempts == 1:
            raise OSError("temporary transport failure")
        self.delivered.set()

    async def send_delta(self, chat_id, delta, metadata, **kwargs):
        self.streamed.set()


async def test_adapter_retry_and_stream_attempts_have_separate_results(tmp_path):
    metrics = GatewayMetrics(version="test")
    bus = MessageBus(metrics=metrics)
    config = Config.model_validate({"channels": {"websocket": {"enabled": False}, "sendMaxRetries": 2}})
    manager = ChannelManager(config, bus, config_path=tmp_path / "config.json", metrics=metrics)
    channel = RecordingChannel(bus)
    manager.channels["recording"] = channel
    task = asyncio.create_task(manager.start_all())
    await asyncio.wait_for(channel.started.wait(), 5)
    try:
        await bus.publish_outbound(OutboundMessage("recording", "private-chat", "answer"))
        await asyncio.wait_for(channel.delivered.wait(), 5)
        await bus.publish_outbound(OutboundMessage(
            "recording", "private-chat", "", event=StreamDeltaEvent(content="delta"),
        ))
        await asyncio.wait_for(channel.streamed.wait(), 5)
        await bus.publish_outbound(OutboundMessage(
            "recording", "private-chat", "", event=StreamedResponseEvent(),
        ))
        async with asyncio.timeout(5):
            while manager.has_pending_delivery:
                await asyncio.sleep(0.01)
        body, _ = metrics.render()
        attempts = {
            (sample.labels["kind"], sample.labels["outcome"]): sample.value
            for family in text_string_to_metric_families(body.decode())
            for sample in family.samples
            if sample.name == "nanobot_channel_send_attempts_total"
        }
        assert attempts == {("message", "error"): 1, ("message", "returned"): 1, ("stream", "returned"): 1}
        from test_gateway_metrics import sample
        assert sample(metrics, "nanobot_channel_delivery_results_total", channel="recording", kind="message", outcome="returned") == 1
        assert sample(metrics, "nanobot_channel_delivery_results_total", channel="recording", kind="message", outcome="failed") == 0
        assert b"private-chat" not in body
        assert manager.outbound_deliveries_pending == 0
    finally:
        await manager.stop_all()
        await task
