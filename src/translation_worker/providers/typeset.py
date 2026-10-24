"""Вёрстка перевода в баблы арт-слоя (карточка 58 §2.2).

Инвариант карточки: текст ВСЕГДА влезает в `box` — перенос по словам, при
переполнении кегль уменьшается до `MIN_FONT_SIZE` (8 px), плашка не выходит за
границы изображения. Вёрстка отделена от рисования: `layout_blocks` работает с
функцией измерения текста, поэтому перенос и подбор кегля проверяются без
шрифтов и пикселей (в тестах — детерминированная моноширинная мера).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from functools import lru_cache
from io import BytesIO

from PIL import Image, ImageDraw, ImageFont

from translation_worker.models import Box, TextBlock

#: Нижняя граница кегля (карточка 58 §2.2): меньше 8 px читать нельзя.
MIN_FONT_SIZE = 8

#: Межстрочный интервал относительно кегля.
LINE_SPACING = 1.2

#: Отступ текста внутри плашки, px.
PADDING = 4

#: Ширина обводки плашки, px (карточка 58 §2.2).
OUTLINE_WIDTH = 2

#: Радиус скругления плашки, px.
PLAQUE_RADIUS = 10

#: Цвета плашки: белая заливка, чёрная обводка, чёрный текст.
PLAQUE_FILL = (255, 255, 255)
PLAQUE_OUTLINE = (0, 0, 0)
TEXT_COLOR = (0, 0, 0)

#: Функция измерения: (текст, кегль) → ширина в пикселях.
MeasureText = Callable[[str, int], float]

_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class PlacedText:
    """Разложенный текст блока: строки, кегль и занятая рамка."""

    block_id: str
    box: Box
    lines: list[str]
    font_size: int


def layout_blocks(
    blocks: list[TextBlock],
    measure: MeasureText,
    *,
    min_font_size: int = MIN_FONT_SIZE,
) -> list[PlacedText]:
    """Разложить тексты блоков по строкам и подобрать кегль под `box`.

    Кегль подбирается от «крупного, но правдоподобного» вниз, пока текст не
    влезет по ширине и высоте; ниже `min_font_size` не опускается (читаемость
    важнее полноты). Порядок блоков сохраняется.
    """
    placed: list[PlacedText] = []
    for block in blocks:
        text = " ".join(block.text.split())
        if not text:
            continue
        if block.box.w <= 0 or block.box.h <= 0:
            # нулевая рамка — нечего рисовать (Pillow бросил бы ValueError)
            continue
        max_width = max(1.0, block.box.w - 2 * PADDING)
        max_height = max(1.0, block.box.h - 2 * PADDING)
        size = max(min_font_size, min(block.box.h // 3, block.box.w // 4, 48))
        lines = _wrap(text, measure, size, max_width)
        while size > min_font_size and not _fits(lines, measure, size, max_width, max_height):
            size -= 1
            lines = _wrap(text, measure, size, max_width)
        placed.append(
            PlacedText(block_id=block.id, box=block.box, lines=lines, font_size=size)
        )
    return placed


def _wrap(text: str, measure: MeasureText, size: int, max_width: float) -> list[str]:
    """Перенос по словам: строка не шире `max_width`, текст не теряется.

    Слово, которое не влезает в строку целиком (длинный ник, URL, «ыыы…»),
    режется по символам: иначе оно вылезло бы за плашку и за пределы бокса,
    хотя перенос «по словам» уже ничего не может сделать.
    """
    lines: list[str] = []
    current = ""
    for word in text.split():
        for index, piece in enumerate(_split_word(word, measure, size, max_width)):
            if index == 0 and current and measure(f"{current} {piece}", size) <= max_width:
                # обычное слово продолжает текущую строку
                current = f"{current} {piece}"
                continue
            if current:
                lines.append(current)
            # кусок разрезанного слова идёт отдельной строкой, без пробела:
            # пробел означал бы границу слов, а слово одно
            current = piece
    if current:
        lines.append(current)
    return lines


def _split_word(word: str, measure: MeasureText, size: int, max_width: float) -> list[str]:
    """Нарезать слово по символам, чтобы каждый кусок влезал в строку."""
    if measure(word, size) <= max_width:
        return [word]
    pieces: list[str] = []
    current = ""
    for char in word:
        candidate = current + char
        if current and measure(candidate, size) > max_width:
            pieces.append(current)
            current = char
        else:
            current = candidate
    if current:
        pieces.append(current)
    return pieces


def _fits(
    lines: list[str], measure: MeasureText, size: int, max_width: float, max_height: float
) -> bool:
    """Текст влезает целиком: каждая строка по ширине, все строки по высоте."""
    if any(measure(line, size) > max_width for line in lines):
        return False
    return len(lines) * size * LINE_SPACING <= max_height


@lru_cache(maxsize=64)
def _font(font_path: str, size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    """Шрифт нужного кегля: TTF из настроек, иначе дефолтный Pillow.

    Битый или несуществующий файл шрифта не валит страницу: арт-слой —
    косметика, деградируем на дефолтный шрифт (путь проверяется ещё на старте
    `Settings`, но файл могли удалить под работающим подом).
    """
    if font_path:
        try:
            return ImageFont.truetype(font_path, size)
        except OSError:
            _LOG.warning("font %s is unreadable, falling back to the default font", font_path)
            return ImageFont.load_default(size=size)
    return ImageFont.load_default(size=size)


def pillow_measure(font_path: str = "") -> MeasureText:
    """Мера текста по реальному шрифту (Pillow); пустой путь → дефолтный."""

    def measure(text: str, size: int) -> float:
        return float(_font(font_path, size).getlength(text))

    return measure


class Typesetter:
    """Рисование плашек и текста поверх заинпанченной страницы (WEBP)."""

    def __init__(self, quality: int = 85, font_path: str = "") -> None:
        self._quality = quality
        self._font_path = font_path

    def render(self, image: bytes, blocks: list[TextBlock]) -> bytes | None:
        """Нарисовать плашки с переводом; нечего рисовать → `None`.

        `None` — не сбой, а «вёрстка не применима» (нет текста либо все рамки
        вырождены): вызывающий код отдаёт страницу оверлеем фазы 1, а не кладёт
        неперекодированный оригинал под ключ `web.webp` с типом `image/webp`.
        """
        placed = layout_blocks(blocks, pillow_measure(self._font_path))
        if not placed:
            return None
        with Image.open(BytesIO(image)) as source:
            picture = source.convert("RGB")
        draw = ImageDraw.Draw(picture)
        for item in placed:
            self._draw_plaque(draw, item)
        buffer = BytesIO()
        picture.save(buffer, format="WEBP", quality=self._quality)
        return buffer.getvalue()

    def _draw_plaque(self, draw: ImageDraw.ImageDraw, item: PlacedText) -> None:
        box = item.box
        draw.rounded_rectangle(
            [box.x, box.y, box.x + box.w - 1, box.y + box.h - 1],
            radius=min(PLAQUE_RADIUS, box.w // 2, box.h // 2),
            fill=PLAQUE_FILL,
            outline=PLAQUE_OUTLINE,
            width=OUTLINE_WIDTH,
        )
        font = _font(self._font_path, item.font_size)
        line_height = item.font_size * LINE_SPACING
        top = box.y + (box.h - len(item.lines) * line_height) / 2
        for index, line in enumerate(item.lines):
            draw.text(
                (box.x + box.w / 2, top + index * line_height),
                line,
                font=font,
                fill=TEXT_COLOR,
                anchor="ma",
            )


__all__ = [
    "MIN_FONT_SIZE",
    "OUTLINE_WIDTH",
    "PADDING",
    "PLAQUE_FILL",
    "PLAQUE_OUTLINE",
    "TEXT_COLOR",
    "MeasureText",
    "PlacedText",
    "Typesetter",
    "layout_blocks",
    "pillow_measure",
]
