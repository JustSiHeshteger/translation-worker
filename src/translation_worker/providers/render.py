"""Рендер арт-слоя: inpaint + вёрстка перевода (карточка 58 §2.3).

`ArtRenderer` — реализация порта `PageRenderer`: страница затирается
(инпант по рамкам блоков) и на неё верстается перевод. Сбой рендера НЕ валит
книгу: страница отдаётся оверлеем фазы 1 (`render` → `None`), и только
`strict_render` превращает сбой в `LAYOUT_FAILED` (карточка 58 §2.3).
"""

from __future__ import annotations

import asyncio

from translation_worker.config import Settings
from translation_worker.models import Layer, TextBlock
from translation_worker.providers.base import ProviderError
from translation_worker.providers.inpaint import (
    Inpainter,
    InpaintError,
    build_inpainter,
)
from translation_worker.providers.typeset import Typesetter


class ArtRenderer:
    """Инпант оригинала + вёрстка перевода в один WEBP-кадр."""

    def __init__(self, inpainter: Inpainter, typesetter: Typesetter, *, strict: bool) -> None:
        self._inpainter = inpainter
        self._typesetter = typesetter
        self._strict = strict

    async def render(self, image: bytes, layer: Layer) -> bytes | None:
        """Перерисованная страница либо `None` — работаем оверлеем фазы 1."""
        blocks = _translated_blocks(layer)
        if not blocks:
            # перерисовывать нечего: страница без текста остаётся оригиналом
            return None
        try:
            erased = await self._inpainter.erase(image, [block.box for block in blocks])
            # PIL рисует синхронно — в поток, чтобы вёрстка не держала loop
            return await asyncio.to_thread(self._typesetter.render, erased, blocks)
        except ProviderError:
            # InpaintError несёт LAYOUT_FAILED, прочие ProviderError — свой код
            if self._strict:
                raise
            return None
        except Exception as exc:  # noqa: BLE001 - рендер не должен валить книгу
            if self._strict:
                # строгий режим обязан отдать код КОНТРАКТА, а не INTERNAL_ERROR:
                # LAYOUT_FAILED есть в контракте и не входит в NON_RETRYABLE_CODES
                raise InpaintError(f"{type(exc).__name__}: {exc}") from exc
            return None


def _translated_blocks(layer: Layer) -> list[TextBlock]:
    """Блоки с непустым переводом: пустые плашки не рисуются."""
    return [block for block in layer.blocks if block.text and block.text.strip()]


def build_art_renderer(settings: Settings) -> ArtRenderer | None:
    """Собрать рендер по настройкам; `render_art=false` → рендера нет."""
    if not settings.render_art:
        return None
    typesetter = Typesetter(quality=settings.typeset_quality, font_path=settings.font_path)
    return ArtRenderer(build_inpainter(settings), typesetter, strict=settings.strict_render)


__all__ = ["ArtRenderer", "build_art_renderer"]
