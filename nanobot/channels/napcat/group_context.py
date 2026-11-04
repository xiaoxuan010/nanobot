"""Place late-admitted observations before QQ action receipts in model input."""

from __future__ import annotations

import json
from collections import defaultdict
from itertools import groupby
from typing import Any, cast

from nanobot.channels.napcat.group_agent import GroupMessage
from nanobot.channels.napcat.group_rendering import inline_text, render_group_messages, sender_title
from nanobot.runtime_context import RUNTIME_CONTEXT_MESSAGE_META
from nanobot.utils.helpers import (
    content_with_media_breadcrumbs,
    image_path_in_text,
    image_placeholder_text,
)


def _fragment(
    source: dict[str, Any], records: list[dict[str, Any]],
    original_text: str, *, last: bool,
) -> dict[str, Any]:
    text = render_group_messages([GroupMessage.parse(record) for record in records])
    paths = {path for record in records for path in record["media"]}
    all_paths = [
        path for record in source["context_metadata"]["napcat_observation"]
        for path in record["media"]
    ]
    content = source["content"]
    fragment = dict(source)
    fragment["context_metadata"] = {"napcat_observation": records}
    if isinstance(content, str):
        suffix = content[len(original_text):]
        breadcrumbs = "\n" + "\n".join(image_placeholder_text(path) for path in all_paths)
        if all_paths and suffix.endswith(breadcrumbs):
            suffix = suffix[:-len(breadcrumbs)]
        fragment["content"] = content_with_media_breadcrumbs(
            "user", text + (suffix if last else ""), [path for path in all_paths if path in paths],
        )
    else:
        blocks: list[dict[str, Any]] = []
        for block in cast(list[dict[str, Any]], content):
            if block.get("type") == "image_url":
                if block["_meta"]["path"] in paths:
                    blocks.append(block)
            elif block.get("type") == "text" and block["text"].startswith(original_text):
                suffix = block["text"][len(original_text):] if last else ""
                blocks.append({**block, "text": text + suffix})
            elif block.get("text") in {image_placeholder_text(path) for path in all_paths}:
                if block["text"] in {
                    image_placeholder_text(path) for path in paths if not image_path_in_text(path, text)
                }:
                    blocks.append(block)
            elif last:
                blocks.append(block)
        fragment["content"] = blocks
    if not last and "_meta" in fragment:
        fragment["_meta"] = {
            key: value for key, value in fragment["_meta"].items()
            if key != RUNTIME_CONTEXT_MESSAGE_META
        }
    return fragment


def prepare_group_context(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Reorder a copy; admission, wakeups and persisted transcript stay intact.

    Only move observations across receipts already preceding their batch. The
    assistant's tool request therefore stays ahead of the inserted user input.
    A batch spanning a receipt is split and rendered with fresh line prefixes.
    """
    receipts = [
        (index, message["context_metadata"]["napcat_action"])
        for index, message in enumerate(messages)
        if message.get("role") == "tool"
        and "napcat_action" in message.get("context_metadata", {})
    ]
    before: dict[int, list[dict[str, Any]]] = defaultdict(list)
    kept: list[list[dict[str, Any]]] = [[] for _ in messages]
    for index, message in enumerate(messages):
        records = message.get("context_metadata", {}).get("napcat_observation")
        if message.get("role") != "user" or not records:
            kept[index].append(message)
            continue
        destinations = [
            next((
                target for target, receipt in receipts
                if target < index and receipt["group_id"] == record["group_id"]
                and record["received_at_ns"] <= receipt["completed_at_ns"]
            ), index)
            for record in records
        ]
        if all(target == index for target in destinations):
            kept[index].append(message)
            continue
        content = message["content"]
        source_text = content if isinstance(content, str) else next(
            block["text"] for block in content
            if block.get("type") == "text" and block["text"].startswith("QQ group ")
        )
        # The stored header and one line per record delimit the observation.
        # Re-rendering old records would couple replay to today's text format.
        original_text = "\n".join(source_text.split("\n", len(records) + 1)[:len(records) + 1])
        for target, group in groupby(zip(destinations, records), key=lambda item: item[0]):
            part = [record for _, record in group]
            destination = kept[index] if target == index else before[target]
            destination.append(_fragment(
                message, part, original_text, last=part[-1] is records[-1],
            ))
    ordered = [
        message
        for index in range(len(messages))
        for message in [*before[index], *kept[index]]
    ]
    return _shorten_senders(ordered)


def _shorten_senders(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: dict[tuple[str, str], tuple[str, str, str, str]] = {}
    names: dict[str, list[tuple[str, str]]] = defaultdict(list)
    result: list[dict[str, Any]] = []
    for entry in messages:
        metadata = entry.get("context_metadata", {})
        is_tool = entry.get("role") == "tool" and "napcat_query_observation" in metadata
        records = metadata.get("napcat_query_observation" if is_tool else "napcat_observation")
        if not records or (entry.get("role") != "user" and not is_tool):
            result.append(entry)
            continue
        content = entry["content"]
        payload: dict[str, Any] = {}
        field = ""
        if is_tool:
            try:
                payload = json.loads(content)
            except json.JSONDecodeError:
                # Tool results may be truncated by the context budget.
                result.append(entry)
                continue
            field = "message" if "message" in payload else "messages"
            text = payload[field]
        else:
            text = content if isinstance(content, str) else next(
                block["text"] for block in content
                if block.get("type") == "text" and block["text"].startswith("QQ group ")
            )
        lines = text.split("\n", len(records) + 1)
        for index, record in enumerate(records, 1):
            message = GroupMessage.parse(record)
            key = (message.group_id, message.sender_id)
            previous = seen.get(key)
            if previous is None or previous[0] != message.name:
                other_names = [name for sender, name in names[message.group_id] if sender != message.sender_id]
                length = min(3, len(message.name))
                while length < len(message.name) and any(name.startswith(message.name[:length]) for name in other_names):
                    length += 1
                alias = inline_text(message.name[:length]) + "…"
                if any(name.startswith(message.name[:length]) for name in other_names):
                    alias = f"({inline_text(message.sender_id)})" + alias
                names[message.group_id].append((message.sender_id, message.name))
                sender = f"({inline_text(message.sender_id)}){inline_text(message.name)}{sender_title(message)}"
            else:
                alias = previous[1]
                sender = alias
                if (message.role, message.title) != (previous[2], previous[3]):
                    sender += sender_title(message)
            seen[key] = (message.name, alias, message.role, message.title)
            line = lines[index]
            timestamp = line[:line.index("] ") + 2] if line.startswith("[") else ""
            identity = f"#{inline_text(message.message_id)}: " if message.targetable else "event: "
            start = line.index(identity, len(timestamp))
            lines[index] = timestamp + sender + " " + line[start:]
        updated = "\n".join(lines)
        if is_tool:
            payload[field] = updated
            content = json.dumps(payload, ensure_ascii=False)
        elif isinstance(content, str):
            content = updated
        else:
            content = [
                {**block, "text": updated} if block.get("type") == "text" and block.get("text") == text else block
                for block in content
            ]
        result.append({**entry, "content": content})
    return result
