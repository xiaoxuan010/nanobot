"""One compact, model-facing projection of structured QQ group messages."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast
from zoneinfo import ZoneInfo

from nanobot.utils.helpers import display_image_path, image_placeholder_text

if TYPE_CHECKING:
    from nanobot.channels.napcat.group_agent import GroupMessage

BEIJING_TIMEZONE = ZoneInfo("Asia/Shanghai")


def _compact(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).replace(
        "\u2028", "\\u2028",
    ).replace("\u2029", "\\u2029")


def inline_text(value: object) -> str:
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
        return inline_text(data.get("text", ""))
    if kind == "reply":
        identity = f" ({inline_text(data['sender_id'])}){inline_text(data.get('sender_name', ''))}" if data.get("sender_id") else ""
        timestamp = (
            " " + datetime.fromtimestamp(int(data["time"]), BEIJING_TIMEZONE).strftime("%Y/%m/%d %H:%M")
            if data.get("time") else ""
        )
        text = str(data.get("text") or "")
        preview = ": " + inline_text(text[:20] + ("…" if len(text) > 20 else "")) if text else ""
        return f"[reply #{inline_text(data.get('id', ''))}{identity}{timestamp}{preview}]"
    if kind == "at":
        name = f"({inline_text(data['name'])})" if data.get("name") else ""
        return f"@{inline_text(data.get('qq', ''))}{name}"
    if kind == "poke":
        target = "you" if data.get("target_is_self") else f"@{inline_text(data.get('qq', ''))}"
        text = f" [QQ display text: {_compact(str(data['text']))}]" if data.get("text") else ""
        return f"[poke {target}]{text}"
    if kind == "face":
        return f"[face {inline_text(data.get('id', ''))}]"
    if kind == "image":
        source = data.get("local_path") or data.get("url") or data.get("file")
        summary = f" {inline_text(data['summary'])}" if data.get("summary") else ""
        return image_placeholder_text(inline_text(display_image_path(source)) if source else None) + summary
    if kind == "record":
        return "[voice]"
    # Retain other OneBot component payloads rather than replacing them with
    # an uninformative placeholder. Only the common components need shortcuts.
    return f"[{inline_text(kind)} {_compact(data)}]"


def render_group_messages(messages: Sequence[GroupMessage]) -> str:
    """Render a group header and one chronological line per message."""
    if not messages:
        return ""
    name = f" {inline_text(messages[0].group_name)}" if messages[0].group_name else ""
    lines = [f"QQ group {inline_text(messages[0].group_id)}{name}"]
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
            f"({inline_text(message.sender_id)}){inline_text(message.name)}{sender_title(message)} "
            if sender != previous_sender else ""
        )
        body = "".join(_segment(segment) for segment in message.segments)
        identity = f"#{inline_text(message.message_id)}" if message.targetable else "event"
        lines.append(f"{time_prefix}{sender_prefix}{identity}: {body}")
        previous_minute, previous_sender = minute, sender
    return "\n".join(lines)


def sender_title(message: GroupMessage) -> str:
    if message.role == "owner":
        return " [群主]"
    if message.role == "admin":
        return " [管理员]"
    if message.title:
        return f" [头衔:{inline_text(message.title)}]"
    return ""
