"""Вёрстка блоков: кегль под рамку и перенос по словам (карточка 55 §2.13).

Кегль подбирается бинарным поиском в `[min_size, max_size]` по реальным
метрикам `PIL.ImageFont`: текст переносится по словам в ширину рамки, и кегль
уменьшается, пока перенесённый текст не уложится в бюджет высоты бабла. Бюджет
высоты — половина рамки: остальное занимают внутренние отступы бабла и «воздух»
вокруг реплики, поэтому перевод не липнет к контуру.

Если уложиться не удаётся даже на минимальном кегле, блок не теряется: кегль
остаётся минимальным, текст переносится по словам (без обрезания и многоточий),
а страница получает warning `page N block K: overflow`. Блоки с пустым или
пробельным текстом не публикуются (§4: «блок без текста отбрасывается»).
"""

from __future__ import annotations

from functools import lru_cache

from PIL import ImageFont

from translation_worker.models import Box, TextBlock

# Доля высоты бабла, доступная тексту (остальное — отступы и «воздух»).
_MAX_TEXT_HEIGHT_SHARE = 0.5
# Межстрочный интервал в долях кегля.
_LINE_HEIGHT_RATIO = 1.25


def fit_blocks(
    blocks: list[TextBlock],
    *,
    page: int,
    font_path: str = "",
    min_size: int = 10,
    max_size: int = 40,
) -> tuple[list[TextBlock], list[str]]:
    """Подобрать кегль и перенос для каждого блока; вернуть блоки и warnings страницы."""
    fitted: list[TextBlock] = []
    warnings: list[str] = []
    for index, block in enumerate(blocks, start=1):
        text = " ".join(block.text.split())
        if not text:
            continue
        font_size, lines, fits = _layout(text, block.box, font_path, min_size, max_size)
        if not fits:
            warnings.append(f"page {page} block {index}: overflow")
        fitted.append(
            TextBlock(
                id=block.id,
                box=block.box,
                kind=block.kind,
                original=block.original,
                text="\n".join(lines),
                confidence=block.confidence,
                font_size=font_size,
            )
        )
    return fitted, warnings


def _layout(
    text: str, box: Box, font_path: str, min_size: int, max_size: int
) -> tuple[int, list[str], bool]:
    """Бинарный поиск максимального кегля; вернуть (кегль, строки, уложилось ли)."""
    low, high = min_size, max_size
    best: int | None = None
    while low <= high:
        middle = (low + high) // 2
        if _fits(text, box, font_path, middle):
            best = middle
            low = middle + 1
        else:
            high = middle - 1
    size = best if best is not None else min_size
    return size, _wrap(text, _font(font_path, size), box.w), best is not None


def _fits(text: str, box: Box, font_path: str, size: int) -> bool:
    """Уложится ли текст в рамку на этом кегле (ширина строк + бюджет высоты)."""
    font = _font(font_path, size)
    lines = _wrap(text, font, box.w)
    if not lines:
        return False
    if any(font.getlength(line) > box.w for line in lines):
        return False
    return len(lines) * (size * _LINE_HEIGHT_RATIO) <= box.h * _MAX_TEXT_HEIGHT_SHARE


def _wrap(text: str, font: ImageFont.FreeTypeFont | ImageFont.ImageFont, width: int) -> list[str]:
    """Перенести текст по словам: слова не теряются и не обрезаются."""
    words = text.split()
    if not words:
        return []
    if width <= 0:
        return words
    lines: list[str] = []
    current = words[0]
    for word in words[1:]:
        candidate = f"{current} {word}"
        if font.getlength(candidate) <= width:
            current = candidate
            continue
        lines.append(current)
        current = word
    lines.append(current)
    return lines


@lru_cache(maxsize=64)
def _font(font_path: str, size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    """Шрифт нужного кегля; без TTF (тесты и dev) — встроенный шрифт Pillow."""
    if font_path:
        return ImageFont.truetype(font_path, size)
    return ImageFont.load_default(size=size)
