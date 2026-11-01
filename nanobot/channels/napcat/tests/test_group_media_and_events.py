"""QQ platform metadata, directed notices, and native transcription integration."""

import asyncio
import base64
import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from nanobot.agent.tools.context import RequestContext, request_context
from nanobot.agent.tools.registry import ToolRegistry
from nanobot.audio.transcription import EffectiveTranscriptionConfig
from nanobot.bus.events import InboundMessage
from nanobot.channels.napcat import runtime as napcat_runtime
from nanobot.channels.napcat.group_agent import OBSERVATION_KEY, GroupMessage
from nanobot.channels.napcat.tests.test_group_agent import channel, message


@pytest.mark.asyncio
async def test_group_message_title_comes_from_member_info_and_is_cached_by_member():
    ch = channel()
    ch._call_action = AsyncMock(return_value={
        "data": {"nickname": "7", "role": "member", "title": "队长"},
    })
    try:
        first = await ch._enrich_group_message(
            GroupMessage.parse(message(1, sender={"nickname": "7", "role": "member"}))
        )
        second = await ch._enrich_group_message(
            GroupMessage.parse(message(2, sender={"nickname": "7", "role": "member"}))
        )
        assert (first.role, first.title) == ("member", "队长")
        assert (second.role, second.title) == ("member", "队长")
        ch._call_action.assert_awaited_once_with(
            "get_group_member_info", {"group_id": 100, "user_id": 7, "no_cache": True},
        )
    finally:
        await ch.stop()


@pytest.mark.asyncio
async def test_failed_group_member_info_lookup_is_not_cached():
    ch = channel()
    ch._call_action = AsyncMock(side_effect=[
        RuntimeError("temporary failure"),
        {"data": {"nickname": "7", "role": "member", "title": "队长"}},
    ])
    try:
        first = await ch._enrich_group_message(
            GroupMessage.parse(message(1, sender={"nickname": "7", "role": "member"}))
        )
        second = await ch._enrich_group_message(
            GroupMessage.parse(message(2, sender={"nickname": "7", "role": "member"}))
        )
        assert first.title == ""
        assert second.title == "队长"
        assert ch._call_action.await_count == 2
    finally:
        await ch.stop()


@pytest.mark.asyncio
async def test_group_member_info_cache_expires(monkeypatch):
    now = [0.0]
    monkeypatch.setattr(napcat_runtime.time, "monotonic", lambda: now[0])
    ch = channel()
    ch._call_action = AsyncMock(side_effect=[
        {"data": {"nickname": "7", "role": "member", "title": "旧头衔"}},
        {"data": {"nickname": "7", "role": "member", "title": "新头衔"}},
    ])
    try:
        first = await ch._enrich_group_message(
            GroupMessage.parse(message(1, sender={"nickname": "7", "role": "member"}))
        )
        now[0] = 301.0
        second = await ch._enrich_group_message(
            GroupMessage.parse(message(2, sender={"nickname": "7", "role": "member"}))
        )
        assert first.title == "旧头衔"
        assert second.title == "新头衔"
        assert ch._call_action.await_count == 2
    finally:
        await ch.stop()


@pytest.mark.asyncio
async def test_poking_bot_wakes_directed_observation_but_is_not_a_quote_target(tmp_path):
    ch = channel()
    ch.config.group_agent.directed_batch_seconds = 0.01

    async def action(name, args):
        if name == "get_group_info":
            return {"data": {"group_name": "Development"}}
        if name == "get_group_member_info":
            return {"data": {"nickname": "Alice"}}
        raise AssertionError(name)

    ch._call_action = AsyncMock(side_effect=action)
    notice = {"notice_type": "notify", "sub_type": "poke", "group_id": 100,
              "user_id": 7, "target_id": 42, "time": 1700000000}
    try:
        await ch._on_notice({**notice, "target_id": 8})
        assert ch.bus.inbound_size == 0
        await ch._on_notice({**notice, "user_id": 8})
        batch = await asyncio.wait_for(ch.bus.consume_inbound(), 1)
        event = batch.metadata[OBSERVATION_KEY][0]
        assert "event: [poke you]" in batch.content
        assert event["message_id"] not in batch.content
        policy = ch.turn_policy(batch, batch.session_key, ToolRegistry())
        assert policy is not None
        with request_context(RequestContext("napcat", "group:100", workspace=tmp_path)):
            result = await policy.tools.execute("reply_message", {"message_id": event["message_id"], "content": "hello"})
            assert result.is_error
        assert all(call.args[0] != "send_msg" for call in ch._call_action.call_args_list)
    finally:
        await ch.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("tool", ["get_message", "get_chat_history"])
async def test_batch_and_query_enrich_group_mentions_quotes_and_original_image_paths(tmp_path, tool):
    ch = channel()
    image_path = (tmp_path / "original.png").as_posix()
    ch._download_image = AsyncMock(return_value=image_path)
    raw = message(1, group_name="", message=[
        {"type": "reply", "data": {"id": "99"}},
        {"type": "at", "data": {"qq": "8"}},
        {"type": "image", "data": {"url": "https://example.com/original.png"}},
    ])

    async def action(name, args):
        if name == "get_group_info":
            return {"data": {"group_name": "Development"}}
        if name == "get_group_member_info":
            return {"data": {"nickname": "Bob"}}
        if name == "get_msg":
            return {"data": message(99, text="earlier question", sender={"nickname": "Alice"}) if str(args["message_id"]) == "99" else raw}
        if name == "get_group_msg_history":
            return {"data": {"messages": [raw]}}
        raise AssertionError(name)

    ch._call_action = AsyncMock(side_effect=action)
    try:
        await ch._on_message(raw)
        await ch.group_observer.flush("100")
        batch = await ch.bus.consume_inbound()
        assert batch.content.startswith("QQ group 100 Development\n")
        assert "[reply #99 (7)Alice 2023/11/15 06:14: earlier question]@8(Bob)" in batch.content
        assert batch.content.count(f"[image: {image_path}]") == 1
        assert "https://example.com/original.png" not in batch.content
        assert batch.media == [image_path]
        image_record = next(
            segment for segment in batch.metadata[OBSERVATION_KEY][0]["message"]
            if segment["type"] == "image"
        )
        assert image_record["data"]["url"] == "https://example.com/original.png"
        assert raw["message"][0]["data"] == {"id": "99"}
        policy = ch.turn_policy(batch, batch.session_key, ToolRegistry())
        assert policy is not None
        with request_context(RequestContext("napcat", "group:100", workspace=tmp_path)):
            queried = await policy.tools.execute(tool, {"message_id": "1"} if tool == "get_message" else {"count": 1})
            text = json.loads(queried)["message" if tool == "get_message" else "messages"]
            assert "Development" in text and "@8(Bob)" in text and "earlier question" in text
            assert text.count(f"[image: {image_path}]") == 1
            assert "https://example.com/original.png" not in text
    finally:
        await ch.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("configured", [True, False])
async def test_voice_tool_uses_napcat_conversion_and_native_transcription(tmp_path, monkeypatch, configured):
    from nanobot.audio import transcription

    ch = channel()
    ch._media_root = tmp_path
    config = EffectiveTranscriptionConfig(
        True, "openai", "whisper-1", None, "test-key" if configured else "", "", 120, 25,
    )
    monkeypatch.setattr(transcription, "resolve_transcription_config", lambda _: config)

    async def transcribe(path, actual_config):
        assert actual_config is config
        assert Path(path).read_bytes() == b"converted audio"
        return "recognized speech"

    transcriber = AsyncMock(side_effect=transcribe)
    monkeypatch.setattr(transcription, "transcribe_audio_file", transcriber)
    ch._call_action = AsyncMock(return_value={"data": {"base64": base64.b64encode(b"converted audio").decode()}})
    record = message(message=[{"type": "record", "data": {"file": "voice.silk"}}])
    inbound = InboundMessage("napcat", "7", "group:100", "", metadata={OBSERVATION_KEY: [GroupMessage.parse(record).wire()]})
    policy = ch.turn_policy(inbound, "napcat:group:100", ToolRegistry())
    assert policy is not None
    try:
        with request_context(RequestContext("napcat", "group:100", workspace=tmp_path)):
            result = await policy.tools.execute("get_voice_transcript", {"message_id": "1"})
        if configured:
            assert json.loads(result)["transcript"] == "recognized speech"
            ch._call_action.assert_awaited_once_with("get_record", {"file": "voice.silk", "out_format": "mp3"})
            transcriber.assert_awaited_once()
        else:
            assert result.is_error
            ch._call_action.assert_not_awaited()
            transcriber.assert_not_awaited()
        assert list(tmp_path.glob("voice-*.mp3")) == []
    finally:
        await ch.stop()
