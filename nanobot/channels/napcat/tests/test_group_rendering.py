"""Model-facing group messages retain usable identities and content boundaries."""

from datetime import datetime

import pytest

from nanobot.bus.queue import MessageBus
from nanobot.channels.napcat.group_agent import (
    OBSERVATION_KEY,
    GroupAgentConfig,
    GroupMessage,
    GroupObserver,
)
from nanobot.channels.napcat.group_rendering import render_group_messages


@pytest.mark.asyncio
async def test_batch_keeps_structured_observations_and_renders_ordered_message_lines():
    segments = [
        {"type": "reply", "data": {"id": "99"}},
        {"type": "at", "data": {"qq": "42"}},
        {"type": "text", "data": {"text": "第一行\n第二行，字面量\\n"}},
        {"type": "image", "data": {"url": "https://example.com/cat.png", "file": "cat.png"}},
        {"type": "face", "data": {"id": "76"}},
        {"type": "record", "data": {"file": "voice.amr"}},
    ]
    first = GroupMessage("101", "100", "7", 1700000000, "Alice\n管理员", segments, real_seq="500")
    second = GroupMessage("4", "100", "8", 1700000001, "Bob", [
        {"type": "text", "data": {"text": "收到"}},
    ])
    bus = MessageBus()
    observer = GroupObserver(GroupAgentConfig(), "napcat", bus.publish_inbound)
    try:
        observer.observe(second, own=False)
        observer.observe(first, own=False)
        await observer.flush("100")
        batch = await bus.consume_inbound()
        assert batch.content.splitlines() == [
            "QQ group 100",
            "[2023/11/15 06:13] (7)Alice\\n管理员 #101: "
            "[reply #99]@42第一行\\n第二行，字面量\\\\n"
            "[image: https://example.com/cat.png][face 76][voice]",
            "(8)Bob #4: 收到",
        ]
        observed = batch.metadata[OBSERVATION_KEY]
        assert observed[0]["message_id"] == "101"
        assert observed[0]["real_seq"] == "500"
        assert observed[0]["time"] == 1700000000
        assert observed[0]["message"] == segments
        assert observed[0]["sender"]["nickname"] == "Alice\n管理员"
    finally:
        await observer.close()


def test_repeated_prefixes_are_scoped_to_each_block_and_dates_remain_explicit():
    def text(value):
        return [{"type": "text", "data": {"text": value}}]

    next_day = int(datetime.fromisoformat("2023-11-16T00:00:00+08:00").timestamp())
    messages = [
        GroupMessage("1", "100", "7", 1700000000, "Alice", text("first")),
        GroupMessage("2", "100", "7", 1700000001, "Alice", text("continued")),
        GroupMessage("3", "100", "8", 1700000002, "Bob", text("answer")),
        GroupMessage("4", "100", "8", 1700000041, "Bob", text("next minute")),
        GroupMessage("5", "100", "8", next_day, "Bob", text("next day")),
        GroupMessage("6", "100", "7", next_day + 10, "Alice", text("back")),
    ]
    assert render_group_messages(messages).splitlines() == [
        "QQ group 100",
        "[2023/11/15 06:13] (7)Alice #1: first",
        "#2: continued",
        "(8)Bob #3: answer",
        "[06:14] #4: next minute",
        "[2023/11/16 00:00] #5: next day",
        "(7)Alice #6: back",
    ]
    # A query or steer block can start with a sender and minute already seen
    # in another block; it still has to provide its own complete context.
    assert render_group_messages(messages[1:2]) == "QQ group 100\n[2023/11/15 06:13] (7)Alice #2: continued"


def test_group_roles_and_member_titles_are_rendered():
    def message(message_id, user_id, nickname, role, title=""):
        return GroupMessage(
            message_id, "100", user_id, 1700000000, nickname,
            [{"type": "text", "data": {"text": "hello"}}], role=role, title=title,
        )

    messages = [
        message("1", "7", "Owner", "owner"),
        message("2", "8", "Admin", "admin"),
        message("3", "9", "Member", "member", "队长"),
        message("4", "10", "Plain", "member"),
    ]
    assert render_group_messages(messages).splitlines() == [
        "QQ group 100",
        "[2023/11/15 06:13] (7)Owner [群主] #1: hello",
        "(8)Admin [管理员] #2: hello",
        "(9)Member [头衔:队长] #3: hello",
        "(10)Plain #4: hello",
    ]


@pytest.mark.asyncio
async def test_observation_round_trip_preserves_group_role_and_title():
    message = GroupMessage(
        "1", "100", "7", 1700000000, "Alice",
        [{"type": "text", "data": {"text": "hello"}}], role="member", title="队长",
    )
    bus = MessageBus()
    observer = GroupObserver(GroupAgentConfig(), "napcat", bus.publish_inbound)
    try:
        observer.observe(message, own=False)
        await observer.flush("100")
        batch = await bus.consume_inbound()
        wire = batch.metadata[OBSERVATION_KEY][0]
        restored = GroupMessage.parse(wire)
        assert (restored.role, restored.title) == ("member", "队长")
    finally:
        await observer.close()
