"""Group image labels connect message text to the images sent to the model."""

from nanobot.channels.napcat.group_agent import GroupMessage
from nanobot.channels.napcat.group_context import prepare_group_context
from nanobot.channels.napcat.group_rendering import render_group_messages
from nanobot.utils.helpers import build_inline_image_block


def test_group_images_keep_their_message_and_path():
    messages = [
        GroupMessage("1", "100", "7", 1700000000, "Alice", [
            {"type": "text", "data": {"text": "first "}},
            {"type": "image", "data": {"local_path": "/media/a.png"}},
        ], ("/media/a.png",)),
        GroupMessage("2", "100", "8", 1700000001, "Bob", [
            {"type": "text", "data": {"text": "second "}},
            {"type": "image", "data": {"local_path": "/media/b.png"}},
        ], ("/media/b.png",)),
    ]
    blocks = [build_inline_image_block(b"a", "image/png", "/media/a.png"),
              build_inline_image_block(b"b", "image/png", "/media/b.png"),
              {"type": "text", "text": render_group_messages(messages)}]
    entry = {"role": "user", "content": blocks, "context_metadata": {"napcat_observation": [
        {**message.wire(), "media": list(message.media), "received_at_ns": 1} for message in messages
    ]}}
    content = prepare_group_context([entry])[0]["content"]
    assert '[Image #1]' in content[0]["text"] and '/media/a.png' in content[0]["text"]
    assert content[1]["image_url"]["url"].endswith("YQ==")
    assert '[Image #2]' in content[3]["text"] and '/media/b.png' in content[3]["text"]
    assert content[4]["image_url"]["url"].endswith("Yg==")
    assert "#1: first [Image #1]" in content[-1]["text"]
    assert "#2: second [Image #2]" in content[-1]["text"]
