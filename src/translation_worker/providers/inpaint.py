"""Inpaint-провайдеры арт-слоя (карточка 58 §2.1).

Inpaint затирает оригинальный текст на странице, чтобы вёрстка перевода не
легла поверх исходных букв. ML-библиотек в воркере нет и не будет (карточка 58
§7): единственная «настоящая» реализация — HTTP-сервис, остальные —
`NoopInpainter` (dev/e2e, страница без изменений) и `FakeInpainter` (тесты,
закрашивает боксы серым).
"""

from __future__ import annotations

import json
from io import BytesIO
from typing import Protocol, runtime_checkable

import httpx
from PIL import Image, ImageDraw, UnidentifiedImageError

from translation_worker.config import Settings
from translation_worker.models import Box
from translation_worker.providers.base import ProviderError

#: Код контракта для сбоя рендера арт-слоя (карточка 58 §4).
INPAINT_FAILED = "LAYOUT_FAILED"


class InpaintError(ProviderError):
    """Inpaint недоступен, вернул не-2xx или мусор вместо страницы."""

    code = INPAINT_FAILED


@runtime_checkable
class Inpainter(Protocol):
    """Затереть текст в боксах: байты картинки → байты картинки."""

    async def erase(self, image: bytes, boxes: list[Box]) -> bytes: ...


class NoopInpainter:
    """Dev/e2e: страница не меняется (вёрстка всё равно накладывается)."""

    async def erase(self, image: bytes, boxes: list[Box]) -> bytes:
        del boxes
        return image


class HttpInpainter:
    """HTTP-сервис инпанта: multipart `image` + JSON `boxes` → байты картинки.

    Контракт сервиса намеренно минимальный (карточка 58 §2.1): поле `image` —
    файл страницы, поле `boxes` — JSON-массив рамок `{x,y,w,h}` в пикселях
    оригинала. Таймаут 120 с: инпант тяжёлый, но зависший сервис не должен
    держать задание до общего таймаута.

    Ответ проверяется, а не принимается на веру: тело обязано быть картинкой
    ТОГО ЖЕ размера, что исходник. Иначе HTML-страница ошибки с кодом 200
    уехала бы в `web.webp` как «переведённая страница», а картинка другого
    размера сдвинула бы все плашки (боксы — в координатах оригинала).
    """

    def __init__(
        self,
        url: str,
        *,
        timeout: float = 120.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._url = url
        self._timeout = timeout
        # транспорт подменяется в тестах (httpx.MockTransport), в проде — None
        self._transport = transport
        self.calls = 0

    async def erase(self, image: bytes, boxes: list[Box]) -> bytes:
        self.calls += 1
        payload = json.dumps(
            [{"x": box.x, "y": box.y, "w": box.w, "h": box.h} for box in boxes]
        )
        try:
            client_args = {"timeout": self._timeout, "transport": self._transport}
            async with httpx.AsyncClient(**client_args) as client:
                response = await client.post(
                    self._url,
                    files={"image": ("page", image, "application/octet-stream")},
                    data={"boxes": payload},
                )
        except httpx.HTTPError as exc:
            raise InpaintError(f"inpaint request failed: {exc}") from exc
        if not 200 <= response.status_code < 300:
            raise InpaintError(f"inpaint service returned HTTP {response.status_code}")
        if not response.content:
            raise InpaintError("inpaint service returned empty body")
        expected = _image_size(image)
        actual = _image_size(response.content)
        if actual is None:
            raise InpaintError("inpaint service returned a non-image body")
        if expected is not None and actual != expected:
            raise InpaintError(
                f"inpaint changed page size {expected[0]}x{expected[1]} -> {actual[0]}x{actual[1]}"
            )
        return response.content


class FakeInpainter:
    """Тесты: закрашивает боксы сплошным серым (пиксели внутри меняются)."""

    def __init__(self, color: tuple[int, int, int] = (128, 128, 128)) -> None:
        self._color = color
        self.calls: list[list[Box]] = []

    async def erase(self, image: bytes, boxes: list[Box]) -> bytes:
        self.calls.append(list(boxes))
        if not boxes:
            return image
        with Image.open(BytesIO(image)) as source:
            picture = source.convert("RGB")
        draw = ImageDraw.Draw(picture)
        for box in boxes:
            if box.w <= 0 or box.h <= 0:
                continue
            draw.rectangle(
                [box.x, box.y, box.x + box.w - 1, box.y + box.h - 1], fill=self._color
            )
        buffer = BytesIO()
        picture.save(buffer, format="PNG")
        return buffer.getvalue()


def build_inpainter(settings: Settings) -> Inpainter:
    """Инпантер по настройкам: `none` — страница без изменений."""
    if settings.inpaint_provider == "http":
        return HttpInpainter(settings.inpaint_url)
    if settings.inpaint_provider == "fake":
        return FakeInpainter()
    return NoopInpainter()


def _image_size(data: bytes) -> tuple[int, int] | None:
    """Размер картинки либо `None`, если это не картинка."""
    try:
        with Image.open(BytesIO(data)) as picture:
            return picture.size
    except (UnidentifiedImageError, OSError):
        return None


__all__ = [
    "INPAINT_FAILED",
    "FakeInpainter",
    "HttpInpainter",
    "InpaintError",
    "Inpainter",
    "NoopInpainter",
    "build_inpainter",
]
