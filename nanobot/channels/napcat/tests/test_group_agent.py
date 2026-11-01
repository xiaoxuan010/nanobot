"""Behavior of group observations, actions, platform history and runtime steer."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from websockets.asyncio.server import serve

from nanobot.agent.loop import AgentLoop
from nanobot.agent.tools.context import RequestContext, request_context
from nanobot.agent.tools.message import MessageTool
from nanobot.agent.tools.registry import ToolRegistry
from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.napcat import group_agent
from nanobot.channels.napcat.group_agent import (
    OBSERVATION_KEY,
    GroupAgentConfig,
    GroupMessage,
    GroupObserver,
)
from nanobot.channels.napcat.runtime import NapcatChannel, NapcatConfig
from nanobot.providers.base import GenerationSettings, LLMResponse, ToolCallRequest
from nanobot.security.workspace_access import (
    bind_workspace_scope,
    default_workspace_scope,
    reset_workspace_scope,
)


def message(mid=1, user=7, text="hello", **extra):
    return {
        "post_type": "message", "message_type": "group", "group_id": 100, "message_id": mid,
        "user_id": user, "time": 1700000000 + mid, "sender": {"nickname": str(user)}, "group_name": "Test group",
        "message": [{"type": "text", "data": {"text": text}}], **extra,
    }


def channel():
    result = NapcatChannel(NapcatConfig(
        allow_from=["7"], allow_groups=["100"], group_agent=GroupAgentConfig(enabled=True, groups=["100"]),
    ), MessageBus())
    result._self_id = 42
    return result


class Clock:
    def __init__(self):
        self.now = 0
        self.waiters = []

    async def sleep(self, delay):
        event = asyncio.Event()
        self.waiters.append((self.now + delay, event))
        await event.wait()

    async def advance(self, seconds):
        self.now += seconds
        for due, event in self.waiters:
            if due <= self.now:
                event.set()
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_fixed_window_and_directed_deadline_are_observable(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(group_agent, "time", SimpleNamespace(monotonic=lambda: clock.now))
    monkeypatch.setattr(group_agent, "asyncio", SimpleNamespace(
        sleep=clock.sleep, create_task=asyncio.create_task, current_task=asyncio.current_task,
        gather=asyncio.gather,
    ))
    bus = MessageBus()
    observer = GroupObserver(GroupAgentConfig(), "napcat", bus.publish_inbound)
    observer.observe(GroupMessage.parse(message()), own=False)
    await asyncio.sleep(0)
    await clock.advance(110)
    observer.observe(GroupMessage.parse(message(2)), own=False)
    await clock.advance(10)
    batch = await asyncio.wait_for(bus.consume_inbound(), 1)
    assert [m["message_id"] for m in batch.metadata[OBSERVATION_KEY]] == ["1", "2"]
    observer.observe(GroupMessage.parse(message(3)), own=False)
    await asyncio.sleep(0)
    await clock.advance(20)
    observer.observe(GroupMessage.parse(message(4)), own=False, directed=True)
    await asyncio.sleep(0)
    await clock.advance(10)
    directed = await asyncio.wait_for(bus.consume_inbound(), 1)
    assert [m["message_id"] for m in directed.metadata[OBSERVATION_KEY]] == ["3", "4"]
    await observer.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("echo_first", [True, False])
async def test_own_echo_and_send_receipt_do_not_wake_or_duplicate(echo_first):
    ch = channel()
    own = message(9, user=42, post_type="message_sent")
    if echo_first:
        await ch._dispatch_frame(json.dumps(own))
        await asyncio.sleep(0)
    ch.group_observer.acknowledged("100", "9")
    if not echo_first:
        await ch._dispatch_frame(json.dumps(own))
        await asyncio.sleep(0)
    assert ch.bus.inbound_size == 0
    await ch._on_message(message())
    await ch._on_message(message())
    await ch._on_message(message(2, user=8))
    await ch.group_observer.flush("100")
    batch = await ch.bus.consume_inbound()
    assert [m["message_id"] for m in batch.metadata[OBSERVATION_KEY]] == ["1", "2"]
    assert batch.session_key == "napcat:group:100"
    await ch.stop()


@pytest.mark.asyncio
async def test_manual_self_message_is_background_until_external_input():
    ch = channel()
    await ch._on_message(message(9, user=42, post_type="message_sent"))
    assert ch.bus.inbound_size == 0
    await ch._on_message(message())
    await ch.group_observer.flush("100")
    batch = await ch.bus.consume_inbound()
    assert [m["user_id"] for m in batch.metadata[OBSERVATION_KEY]] == ["7", "42"]
    await ch.stop()


@pytest.mark.asyncio
async def test_history_reports_actual_scan_and_rejects_other_groups(tmp_path):
    ch = channel()
    page = [message(3, text="find this"), message(2, text="other")]
    ch._call_action = AsyncMock(side_effect=[
        {"data": {"messages": page}},
        {"data": {"nickname": "7", "role": "member", "title": ""}},
        {"data": {"messages": page}},
        {"data": {"message_id": 88}},
        {"data": message(9, group_id=200)},
    ])
    policy = ch.turn_policy(InboundMessage("napcat", "7", "group:100", ""), "napcat:group:100", ToolRegistry())
    assert policy is not None
    with request_context(RequestContext("napcat", "group:100", workspace=tmp_path)):
        result = await policy.tools.execute("search_chat_history", {"keyword": "find", "count": 2})
        parsed = json.loads(result)
        assert parsed["messages"] == "QQ group 100 Test group\n[2023/11/15 06:13] (7)7 #3: find this"
        assert parsed["scanned_count"] == 2
        assert parsed["complete"] is False
        assert parsed["stop_reason"] == "no_progress"
        reply = await policy.tools.execute("reply_message", {
            "message_id": "3", "content": "found",
        })
        assert not reply.is_error
        rejected = await policy.tools.execute("get_message", {"message_id": "9"})
        assert rejected.is_error
    assert ch._call_action.call_args_list[0].args[0] == "get_group_msg_history"
    await ch.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("tool", ["get_message", "get_chat_history"])
async def test_queries_render_lines_and_keep_targets_available_for_reply(tmp_path, tool):
    ch = channel()
    record = message(9, text="earlier question")
    ch._call_action = AsyncMock(side_effect=[
        {"data": record if tool == "get_message" else {"messages": [record]}},
        {"data": {"nickname": "7", "role": "member", "title": ""}},
        {"data": {"message_id": 88}},
    ])
    policy = ch.turn_policy(InboundMessage("napcat", "7", "group:100", ""), "napcat:group:100", ToolRegistry())
    assert policy is not None
    with request_context(RequestContext("napcat", "group:100", workspace=tmp_path)):
        result = await policy.tools.execute(tool, {"message_id": "9"} if tool == "get_message" else {})
        field = "message" if tool == "get_message" else "messages"
        assert json.loads(result)[field] == "QQ group 100 Test group\n[2023/11/15 06:13] (7)7 #9: earlier question"
        reply = await policy.tools.execute("reply_message", {"message_id": "9", "content": "answer"})
        assert not reply.is_error
    assert ch._call_action.call_args.args[1]["message"][0] == {"type": "reply", "data": {"id": "9"}}
    await ch.stop()


@pytest.mark.asyncio
async def test_runtime_steer_updates_reply_targets_and_avoids_final_roundtrip(tmp_path):
    ch = channel()
    await ch._on_message(message())
    await ch.group_observer.flush("100")
    first = await ch.bus.consume_inbound()
    pending = asyncio.Queue()
    provider = MagicMock()
    provider.get_default_model.return_value = "test-model"
    provider.generation = GenerationSettings()
    provider.aclose = AsyncMock()
    provider.can_resume_conversation_state.return_value = False
    provider.estimate_prompt_tokens.return_value = (1000, "test")
    calls = []

    async def model(**kwargs):
        calls.append(kwargs["messages"])
        if len(calls) == 1:
            await ch._on_message(message(2, text="new question"))
            await ch.group_observer.flush("100")
            await pending.put(await ch.bus.consume_inbound())
            return LLMResponse(content="internal preamble", tool_calls=[
                ToolCallRequest(id="a", name="react_message", arguments={"message_id": "1", "emoji_id": "76", "continue_response": True}),
            ])
        if len(calls) == 2:
            return LLMResponse(content=None, tool_calls=[
                ToolCallRequest(id="b", name="reply_message", arguments={"message_id": "2", "content": "answer"}),
            ])
        raise AssertionError("Unexpected extra model call")

    provider.chat_stream_with_retry = model
    ch._call_action = AsyncMock(return_value={"data": {"message_id": 88}})
    loop = AgentLoop(bus=ch.bus, provider=provider, workspace=tmp_path, model="test-model", max_iterations=5)
    loop.turn_policy_provider = ch.turn_policy
    try:
        result = await loop._process_message(first, pending_queue=pending)
        assert result is None
        assert len(calls) == 2
        assert any("[2023/11/15 06:13] (7)7 #2: new question" in str(row.get("content")) for row in calls[1])
        send = ch._call_action.call_args_list[-1]
        assert send.args[0] == "send_msg"
        assert send.args[1]["message"][0] == {"type": "reply", "data": {"id": "2"}}
        while not ch.bus.outbound.empty():
            await ch.send(await ch.bus.consume_outbound())
        assert [call.args[0] for call in ch._call_action.call_args_list] == [
            "get_group_member_info", "set_msg_emoji_like", "send_msg",
        ]
    finally:
        await ch.stop()
        await loop.aclose()
        await loop.sessions.state.aclose()


@pytest.mark.asyncio
async def test_enabled_group_blocks_implicit_final_and_internal_errors():
    ch = channel()
    ch._call_action = AsyncMock()
    await ch.send(OutboundMessage("napcat", "group:100", "automatic reply"))
    await ch.send(OutboundMessage("napcat", "group:100", "Sorry, I encountered an error."))
    ch._call_action.assert_not_awaited()
    await ch.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["napcat", "system"])
@pytest.mark.parametrize("content", ["internal response", ""])
async def test_all_turn_sources_suppress_text_without_extra_finalization(tmp_path, source, content):
    ch = channel()
    provider = MagicMock()
    provider.get_default_model.return_value = "test-model"
    provider.generation = GenerationSettings()
    provider.can_resume_conversation_state.return_value = False
    provider.estimate_prompt_tokens.return_value = (1000, "test")
    provider.aclose = AsyncMock()
    provider.chat_stream_with_retry = AsyncMock(return_value=LLMResponse(content=content))
    loop = AgentLoop(bus=ch.bus, provider=provider, workspace=tmp_path, model="test-model")
    loop.turn_policy_provider = ch.turn_policy
    try:
        result = await loop._process_message(InboundMessage(
            source, "7" if source == "napcat" else "subagent", "group:100" if source == "napcat" else "napcat:group:100",
            "observe", session_key_override="napcat:group:100",
        ))
        assert result is None
        assert provider.chat_stream_with_retry.await_count == 1
    finally:
        await loop.aclose()
        await loop.sessions.state.aclose()
        await ch.stop()


@pytest.mark.asyncio
async def test_real_websocket_observation_action_ack_and_self_echo(tmp_path):
    ch = channel()
    ch.config.group_agent.batch_seconds = 0.01
    requests = []

    async def napcat(ws):
        async for payload in ws:
            request = json.loads(payload)
            if request["action"] == "get_login_info":
                await ws.send(json.dumps({"status": "ok", "retcode": 0, "echo": request["echo"], "data": {"user_id": 42}}))
                await ws.send(json.dumps(message(self_id=42)))
            else:
                requests.append(request)
                # The platform event is intentionally sent before the action acknowledgement.
                await ws.send(json.dumps(message(88, user=42, post_type="message_sent", self_id=42)))
                await ws.send(json.dumps({"status": "ok", "retcode": 0, "echo": request["echo"], "data": {"message_id": 88}}))

    async with serve(napcat, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        ch.config.ws_url = f"ws://127.0.0.1:{port}"
        connection = asyncio.create_task(ch._run_once())
        provider = MagicMock()
        provider.get_default_model.return_value = "test-model"
        provider.generation = GenerationSettings()
        provider.can_resume_conversation_state.return_value = False
        provider.estimate_prompt_tokens.return_value = (1000, "test")
        provider.aclose = AsyncMock()
        provider.chat_stream_with_retry = AsyncMock(return_value=LLMResponse(content=None, tool_calls=[
            ToolCallRequest(id="send", name="send_message", arguments={"content": "visible answer"}),
        ]))
        loop = AgentLoop(bus=ch.bus, provider=provider, workspace=tmp_path, model="test-model")
        loop.turn_policy_provider = ch.turn_policy
        try:
            batch = await asyncio.wait_for(ch.bus.consume_inbound(), 3)
            response = await asyncio.wait_for(loop._process_message(batch), 5)
            assert response is None
            assert [request["action"] for request in requests] == ["get_group_member_info", "send_msg"]
            assert requests[1]["params"]["message"] == [{"type": "text", "data": {"text": "visible answer"}}]
            assert provider.chat_stream_with_retry.await_count == 1
            await asyncio.sleep(0)
            assert ch.bus.inbound_size == 0
        finally:
            await ch.stop()
            await connection
            await loop.aclose()
            await loop.sessions.state.aclose()


@pytest.mark.asyncio
async def test_slow_media_preparation_preserves_batch_order():
    ready, release = asyncio.Event(), asyncio.Event()

    async def prepare(item):
        if item.message_id == "1":
            ready.set()
            await release.wait()
        return item

    bus = MessageBus()
    observer = GroupObserver(GroupAgentConfig(), "napcat", bus.publish_inbound, prepare)
    observer.observe(GroupMessage.parse(message()), own=False)
    first = asyncio.create_task(observer.flush("100"))
    await ready.wait()
    observer.observe(GroupMessage.parse(message(2)), own=False)
    second = asyncio.create_task(observer.flush("100"))
    await asyncio.sleep(0)
    assert bus.inbound_size == 0
    release.set()
    await asyncio.gather(first, second)
    batches = [await bus.consume_inbound(), await bus.consume_inbound()]
    assert [batch.metadata[OBSERVATION_KEY][0]["message_id"] for batch in batches] == ["1", "2"]
    await observer.close()


@pytest.mark.asyncio
async def test_unknown_delivery_and_partial_query_are_errors(tmp_path):
    ch = channel()
    ch._call_action = AsyncMock(side_effect=[
        TimeoutError("no acknowledgement"),
        {"data": {"messages": [message(3, text="match"), message(2)]}},
        {"data": {"nickname": "7", "role": "member", "title": ""}},
        TimeoutError("history disconnected"),
    ])
    policy = ch.turn_policy(InboundMessage("napcat", "7", "group:100", ""), "napcat:group:100", ToolRegistry())
    assert policy is not None
    with request_context(RequestContext("napcat", "group:100", workspace=tmp_path)):
        action = await policy.tools.execute("send_message", {"content": "answer"})
        assert action.is_error
        assert json.JSONDecoder().raw_decode(action)[0]["status"] == "unknown"
        history = await policy.tools.execute("search_chat_history", {"keyword": "match"})
        assert history.is_error
        parsed = json.JSONDecoder().raw_decode(history)[0]
        assert parsed["scanned_count"] == 2
        assert parsed["stop_reason"] == "source_error"
    assert ch._call_action.await_count == 4
    await ch.stop()


@pytest.mark.asyncio
async def test_message_alias_uses_group_action_and_silence_is_normal_completion(tmp_path):
    ch = channel()
    ch._call_action = AsyncMock(return_value={"data": {"message_id": 88}})
    other_destination = AsyncMock()
    tools = ToolRegistry()
    tools.register(MessageTool(send_callback=other_destination, workspace=tmp_path))
    policy = ch.turn_policy(InboundMessage("napcat", "7", "group:100", ""), "napcat:group:100", tools)
    assert policy is not None
    with request_context(RequestContext("napcat", "group:100", workspace=tmp_path)):
        sent = await policy.tools.execute("message", {"content": "answer"})
        assert not sent.is_error
        ch._call_action.assert_awaited_once()
        assert ch._call_action.call_args.args[1]["group_id"] == "100"
        other_destination.assert_not_awaited()
        silent = await policy.tools.execute("stay_silent", {})
        assert not silent.is_error
        assert not silent.continue_response
    await ch.stop()


@pytest.mark.asyncio
async def test_group_media_uses_existing_workspace_restriction(tmp_path):
    ch = channel()
    ch._call_action = AsyncMock()
    ch._build_image_segment = AsyncMock()
    policy = ch.turn_policy(InboundMessage("napcat", "7", "group:100", ""), "napcat:group:100", ToolRegistry())
    assert policy is not None
    token = bind_workspace_scope(default_workspace_scope(tmp_path, True))
    try:
        with request_context(RequestContext("napcat", "group:100", workspace=tmp_path)):
            result = await policy.tools.execute("send_message", {"content": "image", "media": [str(tmp_path.parent / "private.png")]})
            assert result.is_error
            assert json.JSONDecoder().raw_decode(result)[0]["status"] == "failed"
        ch._build_image_segment.assert_not_awaited()
        ch._call_action.assert_not_awaited()
    finally:
        reset_workspace_scope(token)
        await ch.stop()


@pytest.mark.asyncio
async def test_registered_commands_bypass_window_but_other_slash_messages_are_observations(tmp_path):
    ch = channel()
    provider = MagicMock()
    provider.get_default_model.return_value = "test-model"
    provider.generation = GenerationSettings()
    provider.aclose = AsyncMock()
    loop = AgentLoop(bus=ch.bus, provider=provider, workspace=tmp_path, model="test-model")
    loop.turn_policy_provider = ch.turn_policy
    ch.command_predicate = loop.commands.is_dispatchable_command
    try:
        await ch._on_message(message(user=8, text="/help"))
        command = await asyncio.wait_for(ch.bus.consume_inbound(), 1)
        response = await loop._process_message(command)
        assert response is not None and response.tool_action and response.content
        ch._call_action = AsyncMock(return_value={"data": {"message_id": 88}})
        ch._ws = MagicMock()
        await ch.send(response)
        ch._call_action.assert_awaited_once()
        await ch._on_message(message(2, user=8, text="/ordinary-group-text"))
        assert ch.bus.inbound_size == 0
        await ch.group_observer.flush("100")
        batch = await ch.bus.consume_inbound()
        assert [m["message_id"] for m in batch.metadata[OBSERVATION_KEY]] == ["2"]
    finally:
        await ch.stop()
        await loop.aclose()
        await loop.sessions.state.aclose()


@pytest.mark.asyncio
async def test_mature_batch_does_not_duplicate_self_event_while_receipt_is_pending(tmp_path):
    ch = channel()
    entered, receipt = asyncio.Event(), asyncio.Event()

    async def send(action, params):
        if action == "get_group_member_info":
            return {"data": {"nickname": "7", "role": "member", "title": ""}}
        await ch._on_message(message(88, user=42, post_type="message_sent"))
        entered.set()
        await receipt.wait()
        return {"data": {"message_id": 88}}

    ch._call_action = send
    policy = ch.turn_policy(InboundMessage("napcat", "7", "group:100", ""), "napcat:group:100", ToolRegistry())
    assert policy is not None
    try:
        with request_context(RequestContext("napcat", "group:100", workspace=tmp_path)):
            action = asyncio.create_task(policy.tools.execute("send_message", {"content": "answer"}))
            await asyncio.wait_for(entered.wait(), 1)
            await ch._on_message(message())
            await ch.group_observer.flush("100")
            batch = await ch.bus.consume_inbound()
            assert [m["message_id"] for m in batch.metadata[OBSERVATION_KEY]] == ["1"]
            receipt.set()
            assert not (await action).is_error
            await ch._on_message(message(2))
            await ch.group_observer.flush("100")
            next_batch = await ch.bus.consume_inbound()
            assert [m["message_id"] for m in next_batch.metadata[OBSERVATION_KEY]] == ["2"]
    finally:
        receipt.set()
        await ch.stop()
