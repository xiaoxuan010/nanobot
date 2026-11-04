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


def test_identities_are_shortened_across_batches_and_reintroduced_after_truncation():
    first = observation("1", "7", "AliceGreen", "first")
    second = observation("2", "7", "AliceGreen", "second")
    renamed = observation("3", "7", "AliceBlue", "renamed")
    prepared = prepare_group_context([first, second, renamed])
    assert "(7)AliceGreen #1: first" in prepared[0]["content"]
    assert "Ali… #2: second" in prepared[1]["content"]
    assert "(7)AliceBlue #3: renamed" in prepared[2]["content"]
    truncated = prepare_group_context([second])
    assert "(7)AliceGreen #2: second" in truncated[0]["content"]


def test_appending_a_conflicting_name_keeps_the_existing_prefix_unchanged():
    first = observation("1", "7", "AliceGreen", "first")
    second = observation("2", "7", "AliceGreen", "second")
    prior = prepare_group_context([first, second])
    newcomer = observation("3", "8", "AliceBlue", "hello")
    again = observation("4", "8", "AliceBlue", "again")
    extended = prepare_group_context([first, second, newcomer, again])
    assert extended[:2] == prior
    assert "(8)AliceBlue #3: hello" in extended[2]["content"]
    assert "AliceB… #4: again" in extended[3]["content"]


def test_identical_names_remain_distinguishable_without_rewriting_history():
    first = observation("1", "7", "Alice", "first")
    second = observation("2", "7", "Alice", "second")
    prior = prepare_group_context([first, second])
    other = observation("3", "8", "Alice", "third")
    again = observation("4", "8", "Alice", "fourth")
    result = prepare_group_context([first, second, other, again])
    assert result[:2] == prior
    assert "(8)Alice #3: third" in result[2]["content"]
    assert "(8)Alice… #4: fourth" in result[3]["content"]


def test_role_change_is_visible_on_the_message_where_it_occurs():
    first = observation("1", "7", "Alice", "first")
    second = observation("2", "7", "Alice", "second")
    second["context_metadata"]["napcat_observation"][0]["sender"]["role"] = "admin"
    result = prepare_group_context([first, second])
    assert "Ali… [管理员] #2: second" in result[1]["content"]


def test_history_tool_results_share_identity_state_with_observations():
    first = observation("1", "7", "AliceGreen", "first")
    second = observation("2", "7", "AliceGreen", "history")
    tool = {
        "role": "tool", "tool_call_id": "call-1",
        "content": json.dumps({"messages": second["content"], "complete": False}, ensure_ascii=False),
        "context_metadata": {"napcat_query_observation": second["context_metadata"]["napcat_observation"]},
    }
    result = prepare_group_context([first, tool])
    assert "Ali… #2: history" in json.loads(result[1]["content"])["messages"]
    assert json.loads(result[1]["content"])["complete"] is False


def test_appending_messages_to_a_batch_preserves_its_existing_text_prefix():
    first = observation("1", "7", "AliceGreen", "first")
    second = observation("2", "7", "AliceGreen", "second")
    other = observation("3", "8", "AliceBlue", "third")
    records = [item["context_metadata"]["napcat_observation"][0] for item in (first, second, other)]

    def batch(items):
        return {
            "role": "user",
            "content": render_group_messages([GroupMessage.parse(item) for item in items]),
            "context_metadata": {"napcat_observation": items},
        }

    previous = prepare_group_context([batch(records[:2])])[0]["content"]
    extended = prepare_group_context([batch(records)])[0]["content"]
    assert extended.startswith(previous + "\n")
