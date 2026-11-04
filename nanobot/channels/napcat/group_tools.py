"""Turn-local QQ actions and history queries backed by the live NapCat channel."""

from __future__ import annotations

import asyncio
import base64
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from loguru import logger

from nanobot.agent.tools.base import Tool, ToolResult
from nanobot.agent.tools.context import current_request_context
from nanobot.agent.tools.path_utils import resolve_workspace_path
from nanobot.agent.tools.registry import ToolRegistry
from nanobot.agent.turn_policy import TurnPolicy
from nanobot.bus.events import InboundMessage
from nanobot.channels.napcat.group_agent import OBSERVATION_KEY, GroupMessage, GroupObserver
from nanobot.channels.napcat.group_context import prepare_group_context
from nanobot.channels.napcat.group_rendering import render_group_messages
from nanobot.security.workspace_access import current_tool_workspace
from nanobot.utils.helpers import strip_think
from nanobot.utils.prompt_templates import render_template

ActionCaller = Callable[[str, dict[str, Any]], Awaitable[dict[str, Any]]]
ImageBuilder = Callable[[str], Awaitable[dict[str, Any] | None]]
MessageEnricher = Callable[[GroupMessage], Awaitable[GroupMessage]]
VoiceTranscriber = Callable[[GroupMessage], Awaitable[str]]
VoiceSynthesizer = Callable[[str, str | None], Awaitable[bytes]]


class ActionRejectedError(RuntimeError):
    """The platform explicitly rejected an API action."""


class ActionOutcomeUnknownError(RuntimeError):
    """A submitted action lost its acknowledgement; retry may duplicate it."""


@dataclass
class GroupRun:
    channel: str
    group_id: str
    call: ActionCaller
    image: ImageBuilder
    observer: GroupObserver
    enrich: MessageEnricher | None = None
    transcribe: VoiceTranscriber | None = None
    synthesize: VoiceSynthesizer | None = None
    messages: dict[str, GroupMessage] = field(default_factory=dict)
    users: set[str] = field(default_factory=set)
    outcomes: list[str] = field(default_factory=list)
    sent_ids: set[str] = field(default_factory=set)

    def observe(self, message: InboundMessage) -> None:
        values = message.metadata.get(OBSERVATION_KEY, [])
        if not isinstance(values, list):
            raise ValueError("Invalid group observation")
        for value in cast(list[object], values):
            if not isinstance(value, dict):
                raise ValueError("Invalid group message")
            self.accept(GroupMessage.parse(cast(dict[str, Any], value)))

    def accept(self, message: GroupMessage) -> None:
        if message.group_id != self.group_id:
            raise ValueError("Message does not belong to the active group")
        self.messages[message.message_id] = message
        self.users.add(message.sender_id)
        for segment in message.segments:
            data = segment.get("data")
            if segment.get("type") == "at" and isinstance(data, dict):
                user = str(cast(dict[str, Any], data).get("qq", ""))
                if user.isdecimal():
                    self.users.add(user)

    def complete(self) -> None:
        if not self.outcomes:
            outcome = "no_action"
        elif "unknown" in self.outcomes:
            outcome = "action_unknown"
        elif "failed" in self.outcomes:
            outcome = "action_partial" if "succeeded" in self.outcomes else "action_failed"
        elif self.outcomes == ["silent"]:
            outcome = "silence_selected"
        else:
            outcome = "action_succeeded"
        logger.info("Group agent: channel={} group={} outcome={}", self.channel, self.group_id, outcome)

    async def get_message(self, message_id: str) -> GroupMessage:
        if message_id in self.messages and not self.messages[message_id].targetable:
            raise ValueError("This event has no platform message ID")
        response = await self.call("get_msg", {"message_id": message_id})
        data = response.get("data")
        if not isinstance(data, dict):
            raise ValueError("NapCat returned no message")
        message = GroupMessage.parse(cast(dict[str, Any], data))
        self.accept(message)
        if self.enrich is not None:
            message = await self.enrich(message)
            self.accept(message)
        return message

    async def history(self, anchor: str, count: int) -> list[GroupMessage]:
        response = await self.call("get_group_msg_history", {
            "group_id": self.group_id, "message_seq": anchor or "0", "count": count,
            "reverse_order": True,
        })
        data = response.get("data")
        if not isinstance(data, dict):
            raise ValueError("NapCat returned no history")
        rows = cast(dict[str, Any], data).get("messages")
        if not isinstance(rows, list):
            raise ValueError("NapCat returned invalid history")
        messages: list[GroupMessage] = []
        for row in cast(list[object], rows):
            if not isinstance(row, dict):
                raise ValueError("NapCat returned an invalid message")
            message = GroupMessage.parse(cast(dict[str, Any], row))
            self.accept(message)
            if self.enrich is not None:
                message = await self.enrich(message)
                self.accept(message)
            messages.append(message)
        return messages

    async def send(self, content: str, media: list[str], reply: str, mentions: list[str]) -> dict[str, Any]:
        segments: list[dict[str, Any]] = []
        if reply:
            if reply not in self.messages and reply not in self.sent_ids and reply.startswith("#"):
                reply = reply[1:]
            if reply not in self.messages and reply not in self.sent_ids:
                raise ValueError("Read the target message in this group before replying to it")
            if reply in self.messages and not self.messages[reply].targetable:
                raise ValueError("This event cannot be quoted as a message")
            segments.append({"type": "reply", "data": {"id": reply}})
        for user_id in mentions:
            if user_id not in self.users:
                raise ValueError("Mention target has not been observed in this group")
            segments.append({"type": "at", "data": {"qq": user_id}})
        text = strip_think(content).strip()
        if text:
            segments.append({"type": "text", "data": {"text": text}})
        context = current_request_context()
        if context is None or context.workspace is None:
            raise ValueError("QQ actions require an active agent request")
        access = current_tool_workspace(context.workspace)
        for ref in media:
            if ref.startswith(("http://", "https://")):
                path = ref
            elif access.restrict_to_workspace:
                path = str(resolve_workspace_path(ref, context.workspace, access.allowed_root))
            else:
                file = Path(ref).expanduser()
                path = str(file if file.is_absolute() else context.workspace / file)
            image = await self.image(path)
            if image is None:
                raise ValueError("Attachment could not be converted to a QQ image")
            segments.append(image)
        if not text and not media:
            raise ValueError("A message requires content or media")
        return await self._send_segments(segments)

    async def send_voice(self, text: str, instruction: str | None) -> dict[str, Any]:
        if self.synthesize is None:
            raise ValueError("Speech synthesis is unavailable")
        text = strip_think(text).strip()
        if not text:
            raise ValueError("A voice message requires text")
        audio = await self.synthesize(text, instruction)
        return await self._send_segments([{
            "type": "record", "data": {"file": "base64://" + base64.b64encode(audio).decode()},
        }])

    async def _send_segments(self, segments: list[dict[str, Any]]) -> dict[str, Any]:
        with self.observer.sending(self.group_id):
            response = await self.call("send_msg", {
                "message_type": "group", "group_id": self.group_id, "message": segments,
            })
            data = response.get("data")
            if not isinstance(data, dict) or cast(dict[str, Any], data).get("message_id") is None:
                raise ActionOutcomeUnknownError("NapCat accepted send but returned no message_id")
            message_id = str(cast(dict[str, Any], data)["message_id"])
            self.observer.acknowledged(self.group_id, message_id)
        self.sent_ids.add(message_id)
        return {"status": "succeeded", "message_id": message_id}


_DESCRIPTIONS = {
    "send_message": "Send a message to the active QQ group, optionally with images and mentions.",
    "send_voice": "Synthesize text with the configured voice and send one native voice message to the active QQ group. Optionally give a speaking-style instruction. A successful result confirms the QQ send receipt.",
    "reply_message": "Reply to an observed or retrieved message in the active QQ group. The message_id may include or omit a leading #.",
    "react_message": "Add an emoji reaction to an observed or retrieved group message.",
    "poke_user": "Poke a user observed in the active QQ group.",
    "stay_silent": "End this response without another visible action. Previously completed actions remain.",
    "get_message": "Retrieve a message from NapCat by ID. The message must belong to this group.",
    "get_voice_transcript": "Transcribe voice attachments from an observed or retrieved message in this group using the configured transcription provider.",
    "get_chat_history": "Read a page of group history from NapCat. A page is not proof of complete history.",
    "search_chat_history": "Scan bounded pages from NapCat and filter by keyword, sender and time. Inspect the returned scan coverage and continuation anchor before concluding that no matching message exists.",
}


class QQGroupTool(Tool):
    _plugin_discoverable = False

    def __init__(self, name: str, run: GroupRun) -> None:
        self._name = name
        self.run = run

    @property
    def name(self) -> str:
        return self._name

    @property
    def read_only(self) -> bool:
        return self.name in {"get_message", "get_voice_transcript", "get_chat_history", "search_chat_history"}

    @property
    def description(self) -> str:
        return _DESCRIPTIONS[self.name]

    @property
    def parameters(self) -> dict[str, Any]:
        fields: dict[str, Any] = {}
        required: list[str] = []
        if self.name in {"send_message", "reply_message"}:
            fields.update({
                "content": {
                    "type": "string",
                    "description": "要实际发送到 QQ 群的文字。你在工具调用之外直接写出的文字不会发送到群里。",
                },
                "media": {"type": "array", "items": {"type": "string"}},
                "mentions": {"type": "array", "items": {"type": "string"}},
            })
            required.append("content")
        if self.name == "send_voice":
            fields.update({
                "text": {
                    "type": "string", "minLength": 1,
                    "description": "要合成为语音并实际发送到 QQ 群的文字。你在工具调用之外直接写出的文字不会发送到群里。",
                },
                "instruction": {"type": "string", "description": "Optional speaking style, emotion or delivery instruction for synthesis."},
            })
            required.append("text")
        if self.name in {"reply_message", "react_message", "get_message", "get_voice_transcript"}:
            fields["message_id"] = {"type": "string"}
            required.append("message_id")
        if self.name == "react_message":
            fields["emoji_id"] = {"type": "string"}
            required.append("emoji_id")
        if self.name == "poke_user":
            fields["user_id"] = {"type": "string"}
            required.append("user_id")
        if self.name in {"get_chat_history", "search_chat_history"}:
            fields.update({
                "anchor": {"type": "string", "description": "Message ID of the oldest page boundary; omit for latest history."},
                "count": {"type": "integer", "minimum": 1, "maximum": 100},
            })
        if self.name == "search_chat_history":
            fields.update({
                "keyword": {"type": "string"}, "sender_id": {"type": "string"},
                "since": {"type": "integer", "minimum": 0},
                "until": {"type": "integer", "minimum": 0},
                "max_pages": {"type": "integer", "minimum": 1, "maximum": 20},
            })
        if not self.read_only and self.name != "stay_silent":
            fields["continue_response"] = {
                "type": "boolean", "default": False,
                "description": "Default false: finish after this response's tool calls succeed. Set true only when you need their results for further work; the runtime executes the entire tool group before calling the model again. Queries, failed calls, and unknown outcomes return results to the model for handling. No extra model request is needed to verify success or acknowledge sending.",
            }
        return {"type": "object", "properties": fields, "required": required, "additionalProperties": False}

    async def execute(self, **kwargs: Any) -> ToolResult:
        try:
            result = await self._execute(kwargs)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if not self.read_only:
                uncertain = isinstance(error, ActionOutcomeUnknownError)
                self.run.outcomes.append("unknown" if uncertain else "failed")
                status = "unknown" if uncertain else "failed"
            else:
                status = "query_failed"
            output = ToolResult.error(json.dumps({
                "status": status, "error": str(error),
                "note": "An unknown delivery may already have reached QQ; do not blindly resend." if status == "unknown" else "",
            }, ensure_ascii=False))
        else:
            records = result.pop("_napcat_query_observation", None)
            output = ToolResult(
                json.dumps(result, ensure_ascii=False),
                is_error=bool(result.get("source_error")),
                continue_response=self.read_only or kwargs.get("continue_response", False) is True,
            )
            if records is not None:
                output.context_metadata = {"napcat_query_observation": records}
        if not self.read_only:
            output.context_metadata = {"napcat_action": {
                "group_id": self.run.group_id, "completed_at_ns": time.time_ns(),
            }}
        return output

    async def _execute(self, args: dict[str, Any]) -> dict[str, Any]:
        if self.name == "get_message":
            message = await self.run.get_message(args["message_id"])
            return {"message": render_group_messages([message]), "_napcat_query_observation": [message.wire()]}
        if self.name == "get_voice_transcript":
            if self.run.transcribe is None:
                raise ValueError("Voice transcription is unavailable")
            message = self.run.messages.get(args["message_id"]) or await self.run.get_message(args["message_id"])
            return {"message_id": message.message_id, "transcript": await self.run.transcribe(message)}
        if self.name in {"get_chat_history", "search_chat_history"}:
            return await self._query(args)
        if self.name == "send_voice":
            result = await self.run.send_voice(args["text"], args.get("instruction"))
        elif self.name in {"send_message", "reply_message"}:
            result = await self.run.send(
                args["content"], args.get("media", []),
                args.get("message_id", "") if self.name == "reply_message" else "",
                args.get("mentions", []),
            )
        elif self.name == "react_message":
            if args["message_id"] not in self.run.messages and args["message_id"] not in self.run.sent_ids:
                raise ValueError("Read this group's target message before reacting")
            if args["message_id"] in self.run.messages and not self.run.messages[args["message_id"]].targetable:
                raise ValueError("This event cannot receive a message reaction")
            await self.run.call("set_msg_emoji_like", {
                "message_id": args["message_id"], "emoji_id": args["emoji_id"], "set": True,
            })
            result = {"status": "succeeded"}
        elif self.name == "poke_user":
            if args["user_id"] not in self.run.users:
                raise ValueError("Poke target has not been observed in this group")
            await self.run.call("group_poke", {"group_id": self.run.group_id, "user_id": args["user_id"]})
            result = {"status": "succeeded"}
        else:
            self.run.outcomes.append("silent")
            return {"status": "silence_selected"}
        self.run.outcomes.append("succeeded")
        return result

    async def _query(self, args: dict[str, Any]) -> dict[str, Any]:
        anchor = str(args.get("anchor", ""))
        count = int(args.get("count", 50))
        pages = int(args.get("max_pages", 5)) if self.name == "search_chat_history" else 1
        scanned: dict[str, GroupMessage] = {}
        matched: list[GroupMessage] = []
        reason = "budget_exhausted"
        error: str | None = None
        for _ in range(pages):
            try:
                messages = await self.run.history(anchor, count)
            except (ValueError, RuntimeError, TimeoutError, OSError) as exc:
                if not scanned:
                    raise
                reason, error = "source_error", str(exc)
                break
            fresh = [message for message in messages if message.message_id not in scanned]
            if not fresh:
                reason = "no_progress"
                break
            for message in fresh:
                scanned[message.message_id] = message
                text = "\n".join(
                    str(cast(dict[str, Any], segment["data"]).get("text") or cast(dict[str, Any], segment["data"]).get("summary") or "")
                    for segment in message.segments if isinstance(segment.get("data"), dict)
                )
                if (
                    str(args.get("keyword", "")).casefold() in text.casefold()
                    and (not args.get("sender_id") or args["sender_id"] == message.sender_id)
                    and (not args.get("since") or message.timestamp >= int(args["since"]))
                    and (not args.get("until") or message.timestamp <= int(args["until"]))
                ):
                    matched.append(message)
            oldest = min(messages, key=lambda message: (
                message.timestamp, int(message.real_seq) if message.real_seq.isdecimal() else 0,
            ))
            if oldest.message_id == anchor:
                reason = "no_progress"
                break
            anchor = oldest.message_id
            if args.get("since") and oldest.timestamp < int(args["since"]):
                reason = "time_boundary"
                break
        times = [message.timestamp for message in scanned.values()]
        return {
            "messages": render_group_messages(matched), "scanned_count": len(scanned), "scanned_from": min(times) if times else None,
            "scanned_to": max(times) if times else None, "stop_reason": reason,
            "complete": False, "next_anchor": anchor, "source_error": error,
            "_napcat_query_observation": [message.wire() for message in sorted(matched, key=lambda item: item.timestamp)],
        }


class GroupMessageAlias(Tool):
    """Preserve other-channel messaging while binding current-group sends to QQ actions."""

    _plugin_discoverable = False

    def __init__(self, original: Tool, send: QQGroupTool) -> None:
        self.original = original
        self.send = send

    @property
    def name(self) -> str:
        return "message"

    @property
    def description(self) -> str:
        return "For this QQ group use send_message or reply_message. This alias follows the same group action policy; other destinations retain their existing messaging behavior."

    @property
    def parameters(self) -> dict[str, Any]:
        return self.original.parameters

    async def execute(self, **kwargs: Any) -> Any:
        run = self.send.run
        if (kwargs.get("channel") or run.channel) == run.channel and (kwargs.get("chat_id") or f"group:{run.group_id}") == f"group:{run.group_id}":
            if kwargs.get("buttons"):
                return ToolResult.error("QQ group actions do not support inline keyboard buttons")
            return await self.send.execute(content=kwargs.get("content", ""), media=kwargs.get("media") or [])
        return await self.original.execute(**kwargs)


def group_turn_policy(
    message: InboundMessage, group_id: str, channel: str, tools: ToolRegistry,
    call: ActionCaller, image: ImageBuilder, observer: GroupObserver,
    *, enrich: MessageEnricher | None = None, transcribe: VoiceTranscriber | None = None,
    synthesize: VoiceSynthesizer | None = None,
) -> TurnPolicy:
    run = GroupRun(channel, group_id, call, image, observer, enrich, transcribe, synthesize)
    run.observe(message)
    registry = ToolRegistry()
    for name in tools.tool_names:
        tool = tools.get(name)
        if tool is not None:
            registry.register(tool)
    for name in _DESCRIPTIONS:
        if name == "send_voice" and synthesize is None:
            continue
        registry.register(QQGroupTool(name, run))
    original = tools.get("message")
    if original is not None:
        registry.register(GroupMessageAlias(original, QQGroupTool("send_message", run)))
    return TurnPolicy(
        registry, render_template("agent/qq_group_policy.md"), True, run.observe, run.complete,
        prepare_messages=prepare_group_context,
        system_prompt_prefix=(
            "QQ 群消息格式说明（北京时间）："
            "每条消息行格式为 `[YYYY/MM/DD HH:MM] (QQ号)昵称 #消息编号: 正文`；"
            "同一分钟的后续消息省略时间；每位发送者首次显示 QQ 号和完整昵称，后续使用昵称缩写，昵称变化时重新显示完整身份。"
            "行内 `#消息编号` 是当前消息 ID，`[reply #编号 ...]` 中的编号是被引用消息 ID。"
        ),
    )
