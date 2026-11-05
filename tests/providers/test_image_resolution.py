"""Image dimensions visible to the model reflect the prepared copy."""

import base64
import io

from PIL import Image, ImageSequence

from nanobot.providers.images import prepare_inline_images


async def test_resolution_limit_and_notice_preserve_source():
    data = io.BytesIO()
    Image.new("RGB", (2400, 1800), "red").save(data, format="PNG")
    url = "data:image/png;base64," + base64.b64encode(data.getvalue()).decode()
    body = {"messages": [{"role": "user", "content": [{"type": "image_url", "image_url": {"url": url}}]}]}
    prepared = await prepare_inline_images(body, 1600)
    blocks = prepared["messages"][0]["content"]
    assert "2400x1800" in blocks[0]["text"]
    assert "1600x1200" in blocks[0]["text"]
    with Image.open(io.BytesIO(base64.b64decode(blocks[1]["image_url"]["url"].split(",", 1)[1]))) as image:
        assert image.size == (1600, 1200)
    assert body["messages"][0]["content"][0]["image_url"]["url"] == url


async def test_animated_gif_keeps_frames_and_timing_when_resized():
    data = io.BytesIO()
    frames = [Image.new("RGB", (1800, 900), color) for color in ("red", "blue")]
    frames[0].save(data, format="GIF", save_all=True, append_images=frames[1:], duration=[100, 200], loop=0)
    url = "data:image/gif;base64," + base64.b64encode(data.getvalue()).decode()
    body = {"messages": [{"role": "user", "content": [{"type": "image_url", "image_url": {"url": url}}]}]}
    prepared = await prepare_inline_images(body, 800)
    blocks = prepared["messages"][0]["content"]
    assert "1800x900" in blocks[0]["text"]
    assert "800x400" in blocks[0]["text"]
    with Image.open(io.BytesIO(base64.b64decode(blocks[1]["image_url"]["url"].split(",", 1)[1]))) as image:
        assert image.size == (800, 400)
        assert image.n_frames == 2
        assert [frame.info["duration"] for frame in ImageSequence.Iterator(image)] == [100, 200]
        assert [frame.convert("RGB").getpixel((10, 10)) for frame in ImageSequence.Iterator(image)] == [
            (255, 0, 0), (0, 0, 255),
        ]
