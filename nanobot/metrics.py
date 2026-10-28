"""Gateway metrics supporting actionable alerts and their diagnosis."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from functools import partial
from typing import Literal

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    PlatformCollector,
    ProcessCollector,
)
from prometheus_client.core import GaugeMetricFamily, Metric
from prometheus_client.exposition import choose_encoder

from nanobot.llm_usage.context import LLMUsageSource
from nanobot.llm_usage.models import LLMCallRecord

AgentType = Literal["main", "subagent"]
RunOutcome = Literal["success", "error", "cancelled", "limit_reached"]
CallOutcome = Literal["success", "error", "cancelled"]
SendKind = Literal["message", "stream", "reasoning", "event"]
SendOutcome = Literal["returned", "error", "cancelled"]
DeliveryOutcome = Literal["returned", "failed", "unknown", "deferred", "cancelled"]

_SOURCES: tuple[LLMUsageSource, ...] = ("user", "api", "cron", "dream", "system")
_AGENT_TYPES: tuple[AgentType, ...] = ("main", "subagent")
_RUN_BUCKETS = (0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1200, 3600)
_CALL_BUCKETS = (0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600)
_SEND_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120)


@dataclass(frozen=True, slots=True)
class GatewayMetricsState:
    """An event-loop-local snapshot; values describe observed runtime facts."""

    ready: bool
    maintenance: bool
    channels: Mapping[str, bool]
    connections: Mapping[str, bool]
    queues: Mapping[str, int]
    queue_oldest_enqueued_at: Mapping[str, float]
    outbound_deliveries_pending: int


class _GatewayStateCollector:
    def __init__(self, read: Callable[[], GatewayMetricsState]) -> None:
        self._read = read

    def describe(self) -> Iterable[Metric]:
        return ()

    def collect(self) -> Iterable[Metric]:
        state = self._read()
        yield GaugeMetricFamily("nanobot_gateway_ready", "Gateway /health readiness.", value=int(state.ready))
        yield GaugeMetricFamily("nanobot_gateway_maintenance", "Runtime admission is paused.", value=int(state.maintenance))
        yield GaugeMetricFamily("nanobot_outbound_deliveries_pending", "Scheduled outbound tasks not yet finished, including active sends.", value=state.outbound_deliveries_pending)
        for name, help_text, values in (
            ("nanobot_channel_running", "Channel adapter reports that it is running.", state.channels),
            ("nanobot_channel_connected", "Transport connection observed by adapters with a connection contract.", state.connections),
        ):
            metric = GaugeMetricFamily(name, help_text, labels=["channel"])
            for channel, value in values.items():
                metric.add_metric([channel], int(value))
            yield metric
        depth = GaugeMetricFamily("nanobot_queue_depth", "Entries currently in each runtime queue.", labels=["queue"])
        for queue, count in state.queues.items():
            depth.add_metric([queue], count)
        yield depth
        oldest = GaugeMetricFamily("nanobot_queue_oldest_enqueued_timestamp_seconds", "Oldest queued entry's enqueue time; zero for an empty queue.", labels=["queue"])
        for queue, timestamp in state.queue_oldest_enqueued_at.items():
            oldest.add_metric([queue], timestamp)
        yield oldest


class GatewayMetrics:
    """Own one registry and the bounded identity maps for unfinished work."""

    def __init__(self, *, version: str) -> None:
        self.registry = CollectorRegistry()
        PlatformCollector(registry=self.registry)
        ProcessCollector(registry=self.registry)
        Gauge("nanobot_build_info", "Running nanobot version.", ["version"], registry=self.registry).labels(version).set(1)
        self._inbound = Counter("nanobot_inbound_messages_total", "Messages admitted to the inbound bus.", ["channel"], registry=self.registry)
        self._runs = Counter("nanobot_agent_runs_total", "Finished AgentRunner executions.", ["agent_type", "source", "outcome"], registry=self.registry)
        self._run_duration = Histogram("nanobot_agent_run_duration_seconds", "AgentRunner execution including preparation and cleanup.", ["agent_type", "source"], buckets=_RUN_BUCKETS, registry=self.registry)
        self._runs_active = Gauge("nanobot_agent_runs_in_progress", "AgentRunner executions currently in progress.", ["agent_type", "source"], registry=self.registry)
        self._oldest_run = Gauge("nanobot_agent_oldest_run_start_time_seconds", "Start time of the oldest active execution; zero when idle.", ["agent_type", "source"], registry=self.registry)
        self._active_runs: dict[tuple[AgentType, LLMUsageSource], dict[object, float]] = {}
        for agent_type in _AGENT_TYPES:
            for source in _SOURCES:
                active: dict[object, float] = {}
                self._active_runs[agent_type, source] = active
                self._oldest_run.labels(agent_type, source).set_function(partial(self._oldest, active))
                self._runs_active.labels(agent_type, source).set(0)
                for outcome in ("success", "error", "cancelled", "limit_reached"):
                    self._runs.labels(agent_type, source, outcome).inc(0)
        self._llm_requests = Counter("nanobot_llm_requests_total", "Completed nanobot provider call attempts.", ["provider", "model", "source", "outcome"], registry=self.registry)
        self._llm_duration = Histogram("nanobot_llm_request_duration_seconds", "Duration of one nanobot provider call attempt.", ["provider", "model"], buckets=_CALL_BUCKETS, registry=self.registry)
        usage_labels = ["provider", "model", "usage_source"]
        self._tokens = Counter("nanobot_llm_tokens_total", "Normalized total tokens, retaining provider additional usage.", usage_labels, registry=self.registry)
        self._input_tokens = Counter("nanobot_llm_input_tokens_total", "Logical input tokens, including cache reads and writes.", usage_labels, registry=self.registry)
        self._output_tokens = Counter("nanobot_llm_output_tokens_total", "Output tokens.", usage_labels, registry=self.registry)
        self._tools = Counter("nanobot_tool_calls_total", "Tool call processing outcomes, including rejected calls.", ["tool", "outcome"], registry=self.registry)
        self._tool_duration = Histogram("nanobot_tool_call_duration_seconds", "Tool call processing including validation.", ["tool"], buckets=_CALL_BUCKETS, registry=self.registry)
        self._sends = Counter("nanobot_channel_send_attempts_total", "ChannelManager adapter operations; returned does not confirm delivery.", ["channel", "kind", "outcome"], registry=self.registry)
        self._send_duration = Histogram("nanobot_channel_send_duration_seconds", "One adapter operation, excluding queue and retry waits.", ["channel", "kind"], buckets=_SEND_BUCKETS, registry=self.registry)
        self._delivery_results = Counter("nanobot_channel_delivery_results_total", "Logical outbound operation results after retry or durable outbox classification; returned does not confirm recipient delivery.", ["channel", "kind", "outcome"], registry=self.registry)
        self._oldest_delivery = Gauge("nanobot_channel_oldest_pending_delivery_start_time_seconds", "Oldest scheduled outbound operation including destination and concurrency waits; zero when idle.", ["channel"], registry=self.registry)
        self._pending_deliveries: dict[str, dict[object, float]] = {}

    @staticmethod
    def _oldest(active: Mapping[object, float]) -> float:
        return min(active.values(), default=0.0)

    def bind_state(self, read: Callable[[], GatewayMetricsState]) -> None:
        self.registry.register(_GatewayStateCollector(read))

    def render(self, accept: str = "") -> tuple[bytes, str]:
        encoder, content_type = choose_encoder(accept)
        return encoder(self.registry), content_type

    def register_provider(self, provider: str, model: str) -> None:
        for source in _SOURCES:
            for outcome in ("success", "error", "cancelled"):
                self._llm_requests.labels(provider, model, source, outcome).inc(0)

    def register_channel(self, channel: str) -> None:
        if channel in self._pending_deliveries:
            return
        active: dict[object, float] = {}
        self._pending_deliveries[channel] = active
        self._oldest_delivery.labels(channel).set_function(partial(self._oldest, active))
        for kind in ("message", "stream", "reasoning", "event"):
            for outcome in ("returned", "failed", "unknown", "deferred", "cancelled"):
                self._delivery_results.labels(channel, kind, outcome).inc(0)

    def unregister_channel(self, channel: str) -> None:
        if channel in self._pending_deliveries:
            del self._pending_deliveries[channel]
            self._oldest_delivery.remove(channel)

    def inbound_received(self, channel: str) -> None:
        self._inbound.labels(channel).inc()

    def run_started(self, agent_type: AgentType, source: LLMUsageSource) -> object:
        token = object()
        self._active_runs[agent_type, source][token] = time.time()
        self._runs_active.labels(agent_type, source).inc()
        return token

    def run_finished(self, agent_type: AgentType, source: LLMUsageSource, token: object, outcome: RunOutcome, duration: float) -> None:
        del self._active_runs[agent_type, source][token]
        self._runs_active.labels(agent_type, source).dec()
        self._runs.labels(agent_type, source, outcome).inc()
        self._run_duration.labels(agent_type, source).observe(duration)

    def record_llm_call(self, call: LLMCallRecord) -> None:
        outcome = "cancelled" if call.finish_reason == "cancelled" else "error" if call.finish_reason == "error" else "success"
        self._llm_requests.labels(call.provider, call.model, call.source, outcome).inc()
        self._llm_duration.labels(call.provider, call.model).observe(call.duration_ms / 1000)
        usage = call.usage
        if usage is not None:
            labels = (call.provider, call.model, usage.source)
            self._tokens.labels(*labels).inc(usage.total_tokens)
            self._input_tokens.labels(*labels).inc(usage.input_tokens)
            self._output_tokens.labels(*labels).inc(usage.output_tokens)

    def tool_finished(self, tool: str, outcome: CallOutcome, duration: float) -> None:
        self._tools.labels(tool, outcome).inc()
        self._tool_duration.labels(tool).observe(duration)

    def send_finished(self, channel: str, kind: SendKind, outcome: SendOutcome, duration: float) -> None:
        self._sends.labels(channel, kind, outcome).inc()
        self._send_duration.labels(channel, kind).observe(duration)

    def delivery_started(self, channel: str) -> object:
        token = object()
        self._pending_deliveries[channel][token] = time.time()
        return token

    def delivery_ended(self, channel: str, token: object) -> None:
        del self._pending_deliveries[channel][token]

    def delivery_result(self, channel: str, kind: SendKind, outcome: DeliveryOutcome) -> None:
        self._delivery_results.labels(channel, kind, outcome).inc()
