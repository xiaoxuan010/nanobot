"""Group identities remain readable and stable across model requests."""

import json

from nanobot.channels.napcat.group_agent import GroupMessage
from nanobot.channels.napcat.group_context import prepare_group_context
from nanobot.channels.napcat.group_rendering import render_group_messages


def observation(message_id, sender_id, name, text):
    message = GroupMessage(
        message_id, "100", sender_id, 1700000000, name,
        [{"type": "text", "data": {"text": text}}],
    )
    record = {**message.wire(), "received_at_ns": 1, "media": []}
    return {
        "role": "user",
        "content": render_group_messages([message]),
        "context_metadata": {"napcat_observation": [record]},
    }


def batch(*observations):
    records = [item["context_metadata"]["napcat_observation"][0] for item in observations]
    return {
        "role": "user", "content": render_group_messages([GroupMessage.parse(item) for item in records]),
        "context_metadata": {"napcat_observation": records},
    }


def test_identities_are_reintroduced_in_each_batch():
    first = observation("1", "7", "AliceGreen", "first")
    second = observation("2", "7", "AliceGreen", "second")
    renamed = observation("3", "7", "AliceBlue", "renamed")
    prepared = prepare_group_context([first, second, renamed])
    assert "(7)AliceGreen #1: first" in prepared[0]["content"]
    assert "(7)AliceGreen #2: second" in prepared[1]["content"]
    assert "(7)AliceBlue #3: renamed" in prepared[2]["content"]
    truncated = prepare_group_context([second])
    assert "(7)AliceGreen #2: second" in truncated[0]["content"]


def test_conflicting_prefixes_use_full_names_for_all_members_in_a_batch():
    first = observation("1", "7", "AliceGreen", "first")
    second = observation("2", "7", "AliceGreen", "second")
    newcomer = observation("3", "8", "AliceBlue", "hello")
    again = observation("4", "8", "AliceBlue", "again")
    text = prepare_group_context([batch(first, second, newcomer, again)])[0]["content"]
    assert "(7)AliceGreen #1: first" in text
    assert "AliceGreen #2: second" in text
    assert "(8)AliceBlue #3: hello" in text
    assert "AliceBlue #4: again" in text


def test_identical_names_keep_qq_numbers_in_a_batch():
    first = observation("1", "7", "Alice", "first")
    second = observation("2", "7", "Alice", "second")
    other = observation("3", "8", "Alice", "third")
    again = observation("4", "8", "Alice", "fourth")
    text = prepare_group_context([batch(first, second, other, again)])[0]["content"]
    assert "(7)Alice #2: second" in text
    assert "(8)Alice #3: third" in text
    assert "(8)Alice #4: fourth" in text


def test_role_change_is_visible_on_the_message_where_it_occurs():
    first = observation("1", "7", "Alice", "first")
    second = observation("2", "7", "Alice", "second")
    second["context_metadata"]["napcat_observation"][0]["sender"]["role"] = "admin"
    result = prepare_group_context([batch(first, second)])
    assert "Alice [管理员] #2: second" in result[0]["content"]


def test_history_tool_results_start_a_new_batch():
    first = observation("1", "7", "AliceGreen", "first")
    second = observation("2", "7", "AliceGreen", "history")
    tool = {
        "role": "tool", "tool_call_id": "call-1",
        "content": json.dumps({"messages": second["content"], "complete": False}, ensure_ascii=False),
        "context_metadata": {"napcat_query_observation": second["context_metadata"]["napcat_observation"]},
    }
    result = prepare_group_context([first, tool])
    assert "(7)AliceGreen #2: history" in json.loads(result[1]["content"])["messages"]
    assert json.loads(result[1]["content"])["complete"] is False


def test_appending_a_new_batch_preserves_the_existing_batch():
    first = observation("1", "7", "AliceGreen", "first")
    second = observation("2", "7", "AliceGreen", "second")
    other = observation("3", "8", "AliceBlue", "third")
    previous = prepare_group_context([batch(first, second)])
    extended = prepare_group_context([batch(first, second), batch(other)])
    assert extended[:1] == previous
