"""Prepare inline image copies within a shared provider request byte budget."""

from __future__ import annotations

import asyncio
import base64
import io
from collections.abc import Iterator
from copy import deepcopy
from dataclasses import dataclass
from math import ceil
from typing import Any, cast

from loguru import logger
from PIL import Image, ImageOps, ImageSequence

from nanobot.utils.helpers import SMALL_INLINE_IMAGE_BYTES

# A soft transport target, including base64 expansion, rather than an API size limit.
INLINE_IMAGE_BYTE_BUDGET = 1_000_000


@dataclass(frozen=True)
class _InlineImage:
    container: dict[str, Any]
    key: str
    detail: object
    block: dict[str, Any]
    content: list[dict[str, Any]]

    @property
    def url(self) -> str:
        return cast(str, self.container[self.key])


def _inline_images(body: dict[str, Any]) -> Iterator[_InlineImage]:
    items = body.get("input", body.get("messages"))
    # Responses also accepts a plain-text input string.
    if not isinstance(items, list):
        return
    for item in cast(list[dict[str, Any]], items):
        content = item.get("output") if item.get("type") == "function_call_output" else item.get("content")
        if not isinstance(content, list):
            continue
        for block in cast(list[dict[str, Any]], content):
            container = block
            key = "image_url"
            if block.get("type") == "image_url" and isinstance(block.get("image_url"), dict):
                container = cast(dict[str, Any], block["image_url"])
                key = "url"
            elif block.get("type") != "input_image":
                continue
            url = container.get(key)
            if (
                isinstance(url, str)
                and url.startswith("data:image/") and ";base64," in url
            ):
                yield _InlineImage(container, key, container.get("detail"), block, cast(list[dict[str, Any]], content))


@dataclass
class _ImageCopy:
    target: _InlineImage
    source: Image.Image
    format: str
    original_bytes: int
    sent_bytes: int
    sent_size: tuple[int, int]

    def encode(self, quality: int, scale: float) -> None:
        if self.format == "PNG" and scale == 1 and quality != 85:
            return
        image = self.source
        if scale < 1 and self.target.detail != "original":
            image = image.resize(  # pyright: ignore[reportUnknownMemberType]
                (ceil(image.width * scale), ceil(image.height * scale)),
                Image.Resampling.LANCZOS,
            )
        output = io.BytesIO()
        image.save(output, format=self.format, quality=quality, optimize=True)
        raw = output.getvalue()
        if len(raw) < self.sent_bytes:
            self.sent_bytes = len(raw)
            self.sent_size = image.size
            mime = "image/png" if self.format == "PNG" else "image/jpeg"
            self.target.container[self.target.key] = f"data:{mime};base64,{base64.b64encode(raw).decode()}"


async def prepare_inline_images(body: dict[str, Any], max_side: int = 1600) -> dict[str, Any]:
    """Resize images and prepare large requests off the event loop."""
    if not any(_inline_images(body)):
        return body
    return await asyncio.to_thread(_prepare_images, body, max_side)


async def prepare_message_images(messages: list[dict[str, Any]], max_side: int = 1600) -> list[dict[str, Any]]:
    """Prepare public image_url messages before provider-specific conversion."""
    prepared = await prepare_inline_images({"messages": messages}, max_side)
    return cast(list[dict[str, Any]], prepared["messages"])


def _prepare_images(body: dict[str, Any], max_side: int) -> dict[str, Any]:
    prepared = deepcopy(body)
    blocks = list(_inline_images(prepared))
    original_urls = [block.url for block in blocks]
    original_sizes: list[tuple[int, int] | None] = []
    for block in blocks:
        size: tuple[int, int] | None = None
        try:
            raw = base64.b64decode(block.url.split(";base64,", 1)[1], validate=True)
            with Image.open(io.BytesIO(raw)) as original:
                image = ImageOps.exif_transpose(original) if not getattr(original, "is_animated", False) else original
                size = original.size
                if max(image.size) <= max_side:
                    continue
                scale = max_side / max(image.size)
                target_size = (max(1, int(image.width * scale)), max(1, int(image.height * scale)))
                output = io.BytesIO()
                if getattr(original, "is_animated", False):
                    frames = [frame.convert("RGBA").resize(target_size, Image.Resampling.LANCZOS)
                              for frame in ImageSequence.Iterator(original)]
                    durations = [frame.info.get("duration", 0) for frame in ImageSequence.Iterator(original)]
                    options: dict[str, Any] = {"duration": durations, "disposal": 2}
                    if "loop" in original.info:
                        options["loop"] = original.info["loop"]
                    frames[0].save(output, format="GIF", save_all=True, append_images=frames[1:], **options)
                    mime = "image/gif"
                else:
                    fmt = "PNG" if original.format == "PNG" or "A" in image.getbands() or "transparency" in image.info else "JPEG"
                    if fmt == "JPEG" and image.mode != "RGB":
                        image = image.convert("RGB")
                    image.resize(target_size, Image.Resampling.LANCZOS).save(output, format=fmt, quality=85, optimize=True)
                    mime = "image/png" if fmt == "PNG" else "image/jpeg"
                block.container[block.key] = f"data:{mime};base64,{base64.b64encode(output.getvalue()).decode()}"
        except (ValueError, OSError, Image.DecompressionBombError) as exc:
            size = None
            logger.info("Inline image resizing skipped: type={}", type(exc).__name__)
        finally:
            original_sizes.append(size)
    total_bytes = sum(len(image.url) for image in blocks)
    if total_bytes > INLINE_IMAGE_BYTE_BUDGET:
        prepared = _prepare_copies(prepared, total_bytes)
        blocks = list(_inline_images(prepared))
    for block, original_size, original_url in reversed(list(zip(blocks, original_sizes, original_urls))):
        if original_size is None:
            continue
        raw = base64.b64decode(block.url.split(";base64,", 1)[1])
        with Image.open(io.BytesIO(raw)) as image:
            final_size = image.size
        if block.url != original_url:
            index = next(i for i, item in enumerate(block.content) if item is block.block)
            block.content.insert(index, {"type": "text", "text":
                f"Image prepared: original {original_size[0]}x{original_size[1]} pixels; "
                f"sent {final_size[0]}x{final_size[1]} pixels."})
    return prepared


def _prepare_copies(body: dict[str, Any], total_bytes: int) -> dict[str, Any]:
    prepared = deepcopy(body)
    blocks = list(_inline_images(prepared))
    copies: list[_ImageCopy] = []
    try:
        for block in blocks:
            if len(block.url) <= SMALL_INLINE_IMAGE_BYTES:
                continue
            try:
                raw = base64.b64decode(block.url.split(";base64,", 1)[1], validate=True)
                with Image.open(io.BytesIO(raw)) as original:
                    if getattr(original, "is_animated", False) is True:
                        continue
                    original.load()
                    # Preserve PNG pixels and transparency instead of flattening them into JPEG.
                    format = "PNG" if (
                        original.format == "PNG" or "A" in original.getbands()
                        or "transparency" in original.info
                    ) else "JPEG"
                    source = ImageOps.exif_transpose(original)
                    if format == "JPEG" and source.mode != "RGB":
                        source = source.convert("RGB")
                    copies.append(_ImageCopy(block, source, format, len(raw), len(raw), source.size))
            except (ValueError, OSError, Image.DecompressionBombError) as exc:
                logger.info("Inline image preparation skipped: type={}", type(exc).__name__)

        # Re-encode at the original resolution first. Bound quality loss and any later scaling.
        copies.sort(key=lambda image: image.original_bytes, reverse=True)
        sent_bytes = total_bytes
        for quality, scale in ((85, 1.0), (75, 1.0), (65, 1.0), (65, 0.85), (65, 0.75)):
            for image in copies:
                before_bytes = len(image.target.url)
                image.encode(quality, scale)
                sent_bytes += len(image.target.url) - before_bytes
                if sent_bytes <= INLINE_IMAGE_BYTE_BUDGET:
                    break
            if sent_bytes <= INLINE_IMAGE_BYTE_BUDGET:
                break
        logger.info(
            "Inline images prepared: count={} encoded_bytes_before={} "
            "encoded_bytes_after={} budget={} budget_met={} images={}",
            len(blocks), total_bytes, sent_bytes, INLINE_IMAGE_BYTE_BUDGET,
            sent_bytes <= INLINE_IMAGE_BYTE_BUDGET,
            [{
                "before_size": image.source.size, "after_size": image.sent_size,
                "before_bytes": image.original_bytes, "after_bytes": image.sent_bytes,
            } for image in copies[:16]],
        )
        return prepared
    finally:
        for image in copies:
            image.source.close()
