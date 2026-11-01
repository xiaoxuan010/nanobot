"""In-memory group observations and fixed-window admission to the agent runtime."""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, cast

from loguru import logger
from pydantic import Field

from nanobot.bus.events import InboundMessage
from nanobot.channels.napcat.group_rendering import render_group_messages
from nanobot.config_base import Base

OBSERVATION_KEY = "_napcat_observation"


class GroupAgentConfig(Base):
    enabled: bool = False
    groups: list[str] = Field(default_factory=list)
    batch_seconds: float = Field(default=120, gt=0)
    directed_batch_seconds: float = Field(default=10, gt=0)


@dataclass(frozen=True)
class GroupMessage:
    message_id: str
    group_id: str
    sender_id: str
    timestamp: int
    name: str
    segments: list[dict[str, Any]]
    media: tuple[str, ...] = ()
    real_seq: str = ""
    group_name: str = ""
    targetable: bool = True
    received_at_ns: int = field(default_factory=time.time_ns)
    role: str = ""
    title: str = ""

    @classmethod
    def parse(cls, raw: dict[str, Any], media: tuple[str, ...] = ()) -> GroupMessage:
        if raw.get("message_id") is None or raw.get("group_id") is None or raw.get("user_id") is None:
            raise ValueError("Group message requires message_id, group_id and user_id")
        sender_value = raw.get("sender", {})
        sender = cast(dict[str, Any], sender_value) if isinstance(sender_value, dict) else {}
        body = raw.get("message", [])
        if isinstance(body, str):
            segments = [{"type": "text", "data": {"text": body}}]
        elif isinstance(body, list) and all(isinstance(item, dict) for item in cast(list[object], body)):
            segments = cast(list[dict[str, Any]], body)
        else:
            raise ValueError("Group message content must be text or message segments")
        return cls(
            str(raw["message_id"]), str(raw["group_id"]), str(raw["user_id"]),
            int(raw.get("time") or 0), str(sender.get("card") or sender.get("nickname") or ""),
            segments, media, str(raw.get("real_seq") or ""),
            str(raw.get("group_name") or ""), raw.get("targetable", True) is True,
            role=str(sender.get("role") or ""), title=str(sender.get("title") or ""),
        )

    def wire(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id, "group_id": self.group_id, "user_id": self.sender_id,
            "time": self.timestamp,
            "sender": {"nickname": self.name, "role": self.role, "title": self.title},
            "message": self.segments,
            "real_seq": self.real_seq,
            "group_name": self.group_name, "targetable": self.targetable,
        }


@dataclass
class _GroupBuffer:
    pending: list[GroupMessage] = field(default_factory=list)
    own: dict[str, GroupMessage] = field(default_factory=dict)
    seen: OrderedDict[str, None] = field(default_factory=OrderedDict)
    deadline: float = 0
    timer: asyncio.Task[None] | None = None
    publish_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    active_sends: int = 0


class GroupObserver:
    def __init__(
        self, config: GroupAgentConfig, channel: str,
        publish: Callable[[InboundMessage], Awaitable[None]],
        prepare: Callable[[GroupMessage], Awaitable[GroupMessage]] | None = None,
    ) -> None:
        self.config = config
        self.channel = channel
        self.publish = publish
        self.prepare = prepare
        self.buffers: dict[str, _GroupBuffer] = {}
        self._tasks: set[asyncio.Task[None]] = set()

    def enabled(self, group_id: str) -> bool:
        return self.config.enabled and group_id in self.config.groups

    def seen(self, group_id: str, message_id: str) -> bool:
        state = self.buffers.get(group_id)
        return state is not None and message_id in state.seen

    def observe(self, message: GroupMessage, *, own: bool, directed: bool = False, received_at: float | None = None) -> None:
        state = self.buffers.setdefault(message.group_id, _GroupBuffer())
        if message.message_id in state.seen:
            return
        self._remember(state, message.message_id)
        if own:
            state.own[message.message_id] = message
            return
        state.pending.append(message)
        delay = self.config.directed_batch_seconds if directed else self.config.batch_seconds
        due = (received_at if received_at is not None else time.monotonic()) + delay
        if state.timer is None or due < state.deadline:
            if state.timer is not None:
                state.timer.cancel()
            state.deadline = due
            state.timer = asyncio.create_task(self._flush_at(message.group_id, due))
            self._tasks.add(state.timer)
            state.timer.add_done_callback(self._finished)

    def _finished(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            logger.error("Group batch submission failed: {}", task.exception())

    @staticmethod
    def _remember(state: _GroupBuffer, message_id: str) -> None:
        state.seen[message_id] = None
        while len(state.seen) > 2000:
            old, _ = state.seen.popitem(last=False)
            state.own.pop(old, None)

    def acknowledged(self, group_id: str, message_id: str) -> None:
        state = self.buffers.setdefault(group_id, _GroupBuffer())
        self._remember(state, message_id)
        state.own.pop(message_id, None)

    @contextmanager
    def sending(self, group_id: str) -> Generator[None, None, None]:
        state = self.buffers.setdefault(group_id, _GroupBuffer())
        state.active_sends += 1
        try:
            yield
        finally:
            state.active_sends -= 1

    async def _flush_at(self, group_id: str, due: float) -> None:
        await asyncio.sleep(max(0, due - time.monotonic()))
        await self.flush(group_id)

    async def flush(self, group_id: str) -> None:
        state = self.buffers[group_id]
        if state.timer is not None and state.timer is not asyncio.current_task():
            state.timer.cancel()
        state.timer = None
        messages, state.pending = state.pending, []
        if not messages:
            return
        # A self event can precede its send receipt. Keep it pending until the
        # in-flight sends settle, so a mature external batch cannot duplicate it.
        background: list[GroupMessage] = []
        if not state.active_sends:
            background, state.own = list(state.own.values()), {}
        # Serialize transport preparation, not model execution: a slow image in
        # an earlier batch must not allow a later batch to overtake it.
        async with state.publish_lock:
            ordered = sorted([*background, *messages], key=lambda message: message.timestamp)
            if self.prepare is not None:
                ordered = [await self.prepare(message) for message in ordered]
            records = [message.wire() for message in ordered]
            await self.publish(InboundMessage(
                channel=self.channel, sender_id=messages[-1].sender_id, chat_id=f"group:{group_id}",
                session_key_override=f"{self.channel}:group:{group_id}",
                content=render_group_messages(ordered),
                media=[path for message in ordered for path in message.media],
                metadata={
                    OBSERVATION_KEY: records, "is_group": True,
                    "context_metadata": {"napcat_observation": [
                        {**record, "received_at_ns": message.received_at_ns, "media": list(message.media)}
                        for record, message in zip(records, ordered)
                    ]},
                },
            ))

    async def close(self) -> None:
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.buffers.clear()
