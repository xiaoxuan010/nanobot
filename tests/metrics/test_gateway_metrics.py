"""Prometheus contracts observed through exported samples and runtime entry points."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator

import pytest
from prometheus_client.parser import text_string_to_metric_families

from nanobot.agent.hook import AgentHook, AgentHookContext
from nanobot.agent.loop import AgentLoop
from nanobot.agent.runner import AgentRunner, AgentRunSpec
from nanobot.agent.tools.base import Tool, ToolResult
from nanobot.agent.tools.execution import execute_tool_calls
from nanobot.agent.tools.registry import ToolRegistry
from nanobot.bus.events import InboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.config.schema import Config
from nanobot.llm_usage.models import LLMCallRecord
from nanobot.llm_usage.types import LLMUsage
from nanobot.metrics import GatewayMetrics, GatewayMetricsState
from nanobot.providers.base import LLMProvider, LLMResponse, ToolCallRequest
from nanobot.runtime_admission import RuntimeMaintenanceError
from nanobot.utils.llm_runtime import LLMRuntime


def sample(metrics: GatewayMetrics, name: str, **labels: str) -> float | None:
    body, _ = metrics.render()
    for family in text_string_to_metric_families(body.decode()):
        for value in family.samples:
            if value.name == name and value.labels == labels:
                return value.value
    return None


def call(usage: LLMUsage | None, *, finish_reason: str = "stop") -> LLMCallRecord:
    return LLMCallRecord(
        started_at_ms=1000, duration_ms=250, provider="custom", model="selected-agent",
        source="user", stream=True, finish_reason=finish_reason, usage=usage,
    )


def test_reported_estimated_and_missing_usage_keep_distinct_meanings():
    metrics = GatewayMetrics(version="test")
    metrics.record_llm_call(call(LLMUsage.reported(
        input_tokens=100, output_tokens=20, total_tokens=125, cache_read_tokens=60,
    ).with_timing(generation_ms=200, ttft_ms=50)))
    metrics.record_llm_call(call(LLMUsage.estimated(input_tokens=30, output_tokens=5)))
    metrics.record_llm_call(call(None, finish_reason="error"))
    labels = {"provider": "custom", "model": "selected-agent"}
    assert sample(metrics, "nanobot_llm_tokens_total", **labels, usage_source="reported") == 125
    assert sample(metrics, "nanobot_llm_input_tokens_total", **labels, usage_source="reported") == 100
    assert sample(metrics, "nanobot_llm_output_tokens_total", **labels, usage_source="estimated") == 5
    assert sample(metrics, "nanobot_llm_tokens_total", **labels, usage_source="estimated") == 35
    assert sample(metrics, "nanobot_llm_request_duration_seconds_count", **labels) == 3
    metrics.record_llm_call(call(LLMUsage.reported(
        input_tokens=40, output_tokens=2, cache_read_tokens=0, cache_write_tokens=0,
    )))
    assert sample(metrics, "nanobot_llm_input_tokens_total", **labels, usage_source="reported") == 140


def test_live_state_removes_channels_and_registries_are_isolated():
    metrics, other = GatewayMetrics(version="one"), GatewayMetrics(version="two")
    channels = {"napcat": True}
    queues = {"bus_inbound": 2, "session_inbox": 3, "bus_outbound": 4, "outbound_buffer": 1}
    metrics.bind_state(lambda: GatewayMetricsState(
        ready=False, maintenance=True, channels=channels, connections={"napcat": False},
        queues=queues, queue_oldest_enqueued_at={},
        outbound_deliveries_pending=5,
    ))
    assert sample(metrics, "nanobot_gateway_ready") == 0
    assert sample(metrics, "nanobot_gateway_maintenance") == 1
    assert sample(metrics, "nanobot_channel_running", channel="napcat") == 1
    assert sample(metrics, "nanobot_queue_depth", queue="session_inbox") == 3
    assert sample(metrics, "nanobot_outbound_deliveries_pending") == 5
    channels.clear()
    queues["session_inbox"] = 0
    assert sample(metrics, "nanobot_channel_running", channel="napcat") is None
    assert sample(metrics, "nanobot_queue_depth", queue="session_inbox") == 0
    metrics.record_llm_call(call(None))
    assert sample(other, "nanobot_llm_requests_total", provider="custom", model="selected-agent", source="user", outcome="success") is None
    assert sample(other, "nanobot_build_info", version="two") == 1


async def test_bus_counts_only_accepted_input():
    metrics = GatewayMetrics(version="test")
    bus = MessageBus(metrics=metrics)
    message = InboundMessage("napcat", "sender-secret", "chat-secret", "content-secret")
    await bus.publish_inbound(message)
    assert bus.inbound_size == 1
    assert sample(metrics, "nanobot_inbound_messages_total", channel="napcat") == 1
    assert await bus.consume_inbound() is message
    with bus.admission.pause_if_idle(lambda: False):
        with pytest.raises(RuntimeMaintenanceError):
            await bus.publish_inbound(message)
    assert sample(metrics, "nanobot_inbound_messages_total", channel="napcat") == 1
    body, _ = metrics.render()
    assert b"sender-secret" not in body and b"chat-secret" not in body and b"content-secret" not in body


class SequenceProvider(LLMProvider):
    _CHAT_RETRY_DELAYS = (0,)

    def __init__(self, responses: Iterator[LLMResponse]) -> None:
        super().__init__(provider_name="custom")
        self.responses = responses
        self.entered = asyncio.Event()

    async def chat(self, **_kwargs):
        self.entered.set()
        return next(self.responses)

    def get_default_model(self):
        return "selected-agent"


async def no_compaction(_messages, _summary):
    return None


def spec(provider, **kwargs):
    return AgentRunSpec(
        initial_messages=[{"role": "user", "content": "hello"}],
        tools=ToolRegistry(), runtime=LLMRuntime.capture(provider, "selected-agent", context_window_tokens=32000),
        max_iterations=1, max_tool_result_chars=1000, consolidate_history=no_compaction,
        llm_usage_source="user", **kwargs,
    )


async def test_retry_attempts_and_final_agent_result_are_independent():
    metrics = GatewayMetrics(version="test")
    provider = SequenceProvider(iter([
        LLMResponse(content="timeout", finish_reason="error", error_kind="timeout"),
        LLMResponse(content="answer", usage=LLMUsage.reported(input_tokens=10, output_tokens=2)),
    ]))
    provider.set_llm_call_observer(metrics.record_llm_call)
    result = await AgentRunner(metrics=metrics).run(spec(provider))
    assert result.final_content == "answer"
    labels = {"provider": "custom", "model": "selected-agent", "source": "user"}
    assert sample(metrics, "nanobot_llm_requests_total", **labels, outcome="error") == 1
    assert sample(metrics, "nanobot_llm_requests_total", **labels, outcome="success") == 1
    assert sample(metrics, "nanobot_agent_runs_total", agent_type="main", source="user", outcome="success") == 1
    assert sample(metrics, "nanobot_agent_runs_in_progress", agent_type="main", source="user") == 0


@pytest.mark.parametrize("response,outcome", [
    (LLMResponse(content="no", finish_reason="error", error_kind="authentication"), "error"),
    (LLMResponse(content=None), "error"),
])
async def test_runner_returned_errors_are_not_success(response, outcome):
    metrics = GatewayMetrics(version="test")
    provider = SequenceProvider(iter([response] * 10))
    run = spec(provider)
    run.max_iterations = 4
    await AgentRunner(metrics=metrics, agent_type="subagent").run(run)
    assert sample(metrics, "nanobot_agent_runs_total", agent_type="subagent", source="user", outcome=outcome) == 1
    assert sample(metrics, "nanobot_agent_runs_in_progress", agent_type="subagent", source="user") == 0


async def test_prompt_preparation_error_is_counted():
    metrics = GatewayMetrics(version="test")
    provider = SequenceProvider(iter([]))

    def fail_transcript(_input):
        raise OSError("unreadable prompt")

    from nanobot.agent.context import TranscriptInput
    run = spec(provider)
    run.initial_messages = None
    run.transcript_input = TranscriptInput(history=[], current_message="hello")
    run.transcript_builder = fail_transcript
    with pytest.raises(OSError, match="unreadable prompt"):
        await AgentRunner(metrics=metrics).run(run)
    assert sample(metrics, "nanobot_agent_runs_total", agent_type="main", source="user", outcome="error") == 1
    assert sample(metrics, "nanobot_agent_run_duration_seconds_count", agent_type="main", source="user") == 1
    assert sample(metrics, "nanobot_agent_runs_in_progress", agent_type="main", source="user") == 0


async def test_cancelled_agent_settles_once():
    metrics = GatewayMetrics(version="test")
    entered = asyncio.Event()

    class BlockingProvider(SequenceProvider):
        async def chat(self, **_kwargs):
            entered.set()
            await asyncio.Event().wait()

    provider = BlockingProvider(iter([]))
    task = asyncio.create_task(AgentRunner(metrics=metrics).run(spec(provider)))
    await asyncio.wait_for(entered.wait(), 5)
    assert sample(metrics, "nanobot_agent_runs_in_progress", agent_type="main", source="user") == 1
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert sample(metrics, "nanobot_agent_runs_total", agent_type="main", source="user", outcome="cancelled") == 1
    assert sample(metrics, "nanobot_agent_run_duration_seconds_count", agent_type="main", source="user") == 1
    assert sample(metrics, "nanobot_agent_runs_in_progress", agent_type="main", source="user") == 0


async def test_iteration_limit_is_separate_from_error_and_success():
    metrics = GatewayMetrics(version="test")
    provider = SequenceProvider(iter([
        LLMResponse(content=None, tool_calls=[ToolCallRequest(id="one", name="missing", arguments={})]),
    ]))
    await AgentRunner(metrics=metrics).run(spec(provider, finalize_on_max_iterations=False))
    assert sample(metrics, "nanobot_agent_runs_total", agent_type="main", source="user", outcome="limit_reached") == 1


async def test_main_and_subagent_are_wired_to_the_same_registry(tmp_path):
    metrics = GatewayMetrics(version="test")
    bus = MessageBus(metrics=metrics)
    provider = SequenceProvider(iter([
        LLMResponse(content="main answer", usage=LLMUsage.reported(input_tokens=10, output_tokens=2)),
        LLMResponse(content="child answer", usage=LLMUsage.reported(input_tokens=20, output_tokens=3)),
    ]))
    loop = AgentLoop(bus, provider, tmp_path, tool_registry=ToolRegistry(), metrics=metrics)
    await loop.process_direct("hello")
    await loop.subagents.spawn(
        "answer the child task", runtime=LLMRuntime.capture(provider, "selected-agent", context_window_tokens=32000),
    )
    await asyncio.wait_for(bus.consume_inbound(), 10)
    assert sample(metrics, "nanobot_agent_runs_total", agent_type="main", source="user", outcome="success") == 1
    # A standalone child task inherits the system source when no parent run is bound.
    assert sample(metrics, "nanobot_agent_runs_total", agent_type="subagent", source="system", outcome="success") == 1
    await loop.aclose()


class OutcomeTool(Tool):
    name = "outcome"
    description = "Returns a requested result"
    parameters = {"type": "object", "properties": {"mode": {"type": "string"}}, "required": ["mode"]}
    concurrency_safe = True

    async def execute(self, mode):
        if mode == "error":
            return ToolResult.error("tool reported failure")
        if mode == "raise":
            raise OSError("tool raised")
        if mode == "cancel":
            raise asyncio.CancelledError
        return "ok"


@pytest.mark.parametrize("name,args,outcome,label", [
    ("outcome", {"mode": "ok"}, "success", "outcome"),
    ("outcome", {"mode": "error"}, "error", "outcome"),
    ("outcome", {"mode": "raise"}, "error", "outcome"),
    ("outcome", {}, "error", "outcome"),
    ("invented-secret-name", {}, "error", "unregistered"),
    ("outcome", {"mode": "cancel"}, "cancelled", "outcome"),
])
async def test_tool_outcomes_include_rejections_and_cancellation(name, args, outcome, label):
    metrics = GatewayMetrics(version="test")
    tools = ToolRegistry()
    tools.register(OutcomeTool())
    operation = execute_tool_calls(
        tools, [ToolCallRequest(id="one", name=name, arguments=args)], concurrent=True,
        external_lookup_counts={}, workspace_violation_counts={}, hook=AgentHook(),
        context=AgentHookContext(iteration=0, messages=[]), metrics=metrics,
    )
    if outcome == "cancelled":
        with pytest.raises(asyncio.CancelledError):
            await operation
    else:
        await operation
    assert sample(metrics, "nanobot_tool_calls_total", tool=label, outcome=outcome) == 1
    assert sample(metrics, "nanobot_tool_call_duration_seconds_count", tool=label) == 1
    assert b"invented-secret-name" not in metrics.render()[0]


async def test_cancellation_settles_each_concurrent_tool_call():
    metrics = GatewayMetrics(version="test")
    entered = asyncio.Queue()

    class BlockingTool(OutcomeTool):
        async def execute(self, mode):
            entered.put_nowait(mode)
            await asyncio.Event().wait()

    tools = ToolRegistry()
    tools.register(BlockingTool())
    task = asyncio.create_task(execute_tool_calls(
        tools, [ToolCallRequest(id=str(n), name="outcome", arguments={"mode": str(n)}) for n in range(3)],
        concurrent=True, external_lookup_counts={}, workspace_violation_counts={},
        hook=AgentHook(), context=AgentHookContext(iteration=0, messages=[]), metrics=metrics,
    ))
    for _ in range(3):
        await asyncio.wait_for(entered.get(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert sample(metrics, "nanobot_tool_calls_total", tool="outcome", outcome="cancelled") == 3
    assert sample(metrics, "nanobot_tool_call_duration_seconds_count", tool="outcome") == 3


def test_metrics_switch_is_opt_in_and_serializes_with_camel_case():
    assert Config().gateway.metrics_enabled is False
    config = Config.model_validate({"gateway": {"metricsEnabled": True}})
    assert config.gateway.metrics_enabled is True
    assert config.model_dump(by_alias=True)["gateway"]["metricsEnabled"] is True
