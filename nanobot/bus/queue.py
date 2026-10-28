"""Queued delivery of messages and typed events between core and channels."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import time
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any, TypeVar, overload

from loguru import logger

from nanobot.bus.events import InboundMessage, OutboundMessage
from nanobot.bus.outbound_events import outbound_message_for_event
from nanobot.events import AgentEvent
from nanobot.runtime_admission import RuntimeAdmission

if TYPE_CHECKING:
    from nanobot.metrics import GatewayMetrics

_EventT = TypeVar("_EventT", bound=AgentEvent)
_ItemT = TypeVar("_ItemT")
EventHandler = Callable[[AgentEvent], Awaitable[None] | None]


class TimestampedQueue(asyncio.Queue[_ItemT]):
    """FIFO with enqueue timestamps when runtime metrics are enabled."""

    def __init__(self, *, track_age: bool):
        super().__init__()
        self._track_age = track_age
        self._enqueued_at: deque[float] = deque()

    def _put(self, item: _ItemT) -> None:
        super()._put(item)
        if self._track_age:
            self._enqueued_at.append(time.time())

    def _get(self) -> _ItemT:
        item = super()._get()
        if self._track_age:
            self._enqueued_at.popleft()
        return item

    @property
    def oldest_enqueued_at(self) -> float:
        return self._enqueued_at[0] if self._enqueued_at else 0.0


class MessageBus:
    """
    Async message bus that decouples chat channels from the agent core.

    Channels push messages to the inbound queue. Core operations publish text,
    media, or typed events to the same routed outbound queue, independently of
    whether an LLM produced them. Channel adapters own their wire projection.

    Local subscribers are awaited by ``publish``; channel delivery is queued by
    ``publish_event``. Local state transitions never wait for network sends.
    """

    def __init__(self, *, admission: RuntimeAdmission | None = None, metrics: GatewayMetrics | None = None):
        self._metrics = metrics
        self.admission = admission if admission is not None else RuntimeAdmission()
        self.inbound = TimestampedQueue[InboundMessage](track_age=metrics is not None)
        self.outbound = TimestampedQueue[OutboundMessage](track_age=metrics is not None)
        self._handlers: list[EventHandler] = []
        self._pending: set[asyncio.Task[None]] = set()
        self.outbound_recorder: Callable[[OutboundMessage], None] | None = None

    async def publish_inbound(self, msg: InboundMessage) -> None:
        """Publish a message from a channel to the agent."""
        self.admission.check()
        # This queue is unbounded. Admission and enqueue must not yield apart.
        self.inbound.put_nowait(msg)
        if self._metrics is not None:
            self._metrics.inbound_received(msg.channel)

    async def consume_inbound(self) -> InboundMessage:
        """Consume the next inbound message (blocks until available)."""
        return await self.inbound.get()

    async def publish_outbound(self, msg: OutboundMessage) -> None:
        """Queue a routed message or event for its channel."""
        if self.outbound_recorder is not None:
            self.outbound_recorder(msg)
        await self.outbound.put(msg)

    async def publish_event(
        self,
        event: AgentEvent,
        *,
        channel: str,
        chat_id: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        """Queue a typed event using the existing outbound delivery contract.

        The bus transports event values without inspecting their fields. Known
        events retain their text fallback; channel adapters decide how to render
        or ignore events they receive.
        """
        await self.publish_outbound(outbound_message_for_event(
            channel=channel, chat_id=chat_id, event=event, metadata=metadata,
        ))

    async def consume_outbound(self) -> OutboundMessage:
        """Consume the next outbound message (blocks until available)."""
        return await self.outbound.get()

    @property
    def inbound_size(self) -> int:
        """Number of pending inbound messages."""
        return self.inbound.qsize()

    @property
    def outbound_size(self) -> int:
        """Number of pending outbound messages."""
        return self.outbound.qsize()

    @property
    def inbound_oldest_enqueued_at(self) -> float:
        return self.inbound.oldest_enqueued_at

    @property
    def outbound_oldest_enqueued_at(self) -> float:
        return self.outbound.oldest_enqueued_at

    @property
    def has_pending_events(self) -> bool:
        """Scheduled local subscribers may still start agent background work."""
        return bool(self._pending)

    @overload
    def subscribe(
        self, handler: Callable[[_EventT], Awaitable[None] | None],
        event_type: type[_EventT],
    ) -> Callable[[], None]: ...

    @overload
    def subscribe(
        self, handler: EventHandler, event_type: None = None,
    ) -> Callable[[], None]: ...

    def subscribe(
        self,
        handler: Callable[..., Awaitable[None] | None],
        event_type: type[AgentEvent] | None = None,
    ) -> Callable[[], None]:
        """Connect an ordered, awaited handler; return its idempotent disconnect.

        The overloads bind handler and event type. The erased callable exists
        only at this heterogeneous dispatch boundary, behind the type filter.
        """
        active = True

        def entry(event: AgentEvent) -> Awaitable[None] | None:
            if active and (event_type is None or isinstance(event, event_type)):
                return handler(event)
            return None
        self._handlers.append(entry)

        def _unsubscribe() -> None:
            nonlocal active
            active = False
            with contextlib.suppress(ValueError):
                self._handlers.remove(entry)

        return _unsubscribe

    async def publish(self, event: AgentEvent) -> None:
        """Await local subscribers in registration order, without channel delivery."""
        for handler in list(self._handlers):
            try:
                result = handler(event)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                logger.exception("event handler failed for {}", type(event).__name__)

    def publish_nowait(self, event: AgentEvent) -> asyncio.Task[None] | None:
        """Schedule local dispatch, retaining it until completion.

        Unlike ``publish``, the caller does not wait for handlers. This does not
        turn individual handlers into independent workers or change their order.
        Separate publications may interleave; this is not a global event FIFO.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.debug("dropping event without a running loop: {}", type(event).__name__)
            return None
        task = loop.create_task(self.publish(event))
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)
        return task

    async def drain(self) -> None:
        """Finish scheduled dispatches after producers stop, before disconnecting."""
        while self._pending:
            pending = tuple(self._pending)
            await asyncio.gather(*pending, return_exceptions=True)
            self._pending.difference_update(pending)
