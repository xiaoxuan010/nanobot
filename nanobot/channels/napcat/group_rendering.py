"""One compact, model-facing projection of structured QQ group messages."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast
from zoneinfo import ZoneInfo

from nanobot.utils.helpers import image_placeholder_text

if TYPE_CHECKING:
    from nanobot.channels.napcat.group_agent import GroupMessage

BEIJING_TIMEZONE = ZoneInfo("Asia/Shanghai")


def _compact(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).replace(
        "\u2028", "\\u2028",
    ).replace("\u2029", "\\u2029")


def _inline(value: object) -> str:
    # Escape rather than collapse whitespace: code and literal backslashes
    # must survive without allowing message text to introduce another record.
    return _compact(str(value))[1:-1]


def _segment(segment: dict[str, Any]) -> str:
    raw = segment.get("data")
    if not isinstance(raw, dict):
        return f"[{_compact(segment)}]"
    data = cast(dict[str, Any], raw)
    kind = segment.get("type")
    if kind == "text":
        return _inline(data.get("text", ""))
    if kind == "reply":
        identity = f" ({_inline(data['sender_id'])}){_inline(data.get('sender_name', ''))}" if data.get("sender_id") else ""
        timestamp = (
            " " + datetime.fromtimestamp(int(data["time"]), BEIJING_TIMEZONE).strftime("%Y/%m/%d %H:%M")
            if data.get("time") else ""
        )
        text = str(data.get("text") or "")
        preview = ": " + _inline(text[:20] + ("…" if len(text) > 20 else "")) if text else ""
        return f"[reply #{_inline(data.get('id', ''))}{identity}{timestamp}{preview}]"
    if kind == "at":
        name = f"({_inline(data['name'])})" if data.get("name") else ""
        return f"@{_inline(data.get('qq', ''))}{name}"
    if kind == "poke":
        target = "you" if data.get("target_is_self") else f"@{_inline(data.get('qq', ''))}"
        text = f" [QQ display text: {_compact(str(data['text']))}]" if data.get("text") else ""
        return f"[poke {target}]{text}"
    if kind == "face":
        return f"[face {_inline(data.get('id', ''))}]"
    if kind == "image":
        source = data.get("local_path") or data.get("url") or data.get("file")
        summary = f" {_inline(data['summary'])}" if data.get("summary") else ""
        return image_placeholder_text(_inline(source) if source else None) + summary
    if kind == "record":
        return "[voice]"
    # Retain other OneBot component payloads rather than replacing them with
    # an uninformative placeholder. Only the common components need shortcuts.
    return f"[{_inline(kind)} {_compact(data)}]"


def render_group_messages(messages: Sequence[GroupMessage]) -> str:
    """Render a group header and one chronological line per message."""
    if not messages:
        return ""
    name = f" {_inline(messages[0].group_name)}" if messages[0].group_name else ""
    lines = [f"QQ group {_inline(messages[0].group_id)}{name}"]
    previous_minute: datetime | None = None
    previous_sender: tuple[str, str, str, str] | None = None
    for message in sorted(messages, key=lambda item: item.timestamp):
        minute = datetime.fromtimestamp(message.timestamp, BEIJING_TIMEZONE).replace(second=0, microsecond=0)
        if previous_minute is None or minute.date() != previous_minute.date():
            timestamp = minute.strftime("%Y/%m/%d %H:%M")
            time_prefix = f"[{timestamp}] "
        elif minute != previous_minute:
            time_prefix = f"[{minute:%H:%M}] "
        else:
            time_prefix = ""
        sender = (message.sender_id, message.name, message.role, message.title)
        sender_prefix = (
            f"({_inline(message.sender_id)}){_inline(message.name)}{_sender_title(message)} "
            if sender != previous_sender else ""
        )
        body = "".join(_segment(segment) for segment in message.segments)
        identity = f"#{_inline(message.message_id)}" if message.targetable else "event"
        lines.append(f"{time_prefix}{sender_prefix}{identity}: {body}")
        previous_minute, previous_sender = minute, sender
    return "\n".join(lines)


def _sender_title(message: GroupMessage) -> str:
    if message.role == "owner":
        return " [群主]"
    if message.role == "admin":
        return " [管理员]"
    if message.title:
        return f" [头衔:{_inline(message.title)}]"
    return ""
