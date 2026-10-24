"""Вёрстка перевода в баблы (карточка 58 §2.2, §5).

Инварианты карточки проверяются без шрифтов: `layout_blocks` принимает функцию
измерения текста, поэтому перенос строк, подбор кегля и «текст не теряется»
проверяются детерминированной моноширинной мерой. `Typesetter.render` — PIL:
валидный WEBP, размеры сохраняются, пиксели внутри бокса меняются, вне — нет.
"""

from __future__ import annotations

import io

from PIL import Image
from translation_worker.models import Box, TextBlock
from translation_worker.providers.typeset import (
    MIN_FONT_SIZE,
    PADDING,
    Typesetter,
    layout_blocks,
)


def mono(text: str, size: int) -> float:
    """Детерминированная мера: каждый символ — 0.6 кегля."""
    return 0.6 * size * len(text)


def block(text: str, box: Box | None = None, block_id: str = "p1-b1") -> TextBlock:
    return TextBlock(
        id=block_id,
        box=box or Box(x=100, y=200, w=400, h=200),
        kind="BUBBLE",
        original="original",
        text=text,
        confidence=0.9,
    )


def test_short_text_occupies_one_line() -> None:
    placed = layout_blocks([block("Кот")], mono)

    assert len(placed) == 1
    assert placed[0].lines == ["Кот"]
    assert placed[0].font_size > MIN_FONT_SIZE


def test_long_text_wraps_by_words() -> None:
    words = " ".join(["слово"] * 40)

    placed = layout_blocks([block(words, Box(x=0, y=0, w=300, h=400))], mono)

    assert len(placed[0].lines) > 1, "длинный текст обязан переноситься"
    for line in placed[0].lines:
        assert mono(line, placed[0].font_size) <= 300 - 2 * PADDING


def test_text_is_not_lost_while_wrapping() -> None:
    words = " ".join(["котик"] * 25)

    placed = layout_blocks([block(words, Box(x=0, y=0, w=260, h=400))], mono)

    assert " ".join(placed[0].lines).split() == words.split()


def test_font_shrinks_when_text_does_not_fit() -> None:
    long_word = "ы" * 60

    placed = layout_blocks([block(long_word, Box(x=0, y=0, w=200, h=120))], mono)

    assert placed[0].font_size < 40, "кегль обязан уменьшаться под узкий бокс"


def test_word_longer_than_line_is_split_to_fit() -> None:
    word = "ы" * 60
    box = Box(x=0, y=0, w=100, h=60)

    placed = layout_blocks([block(word, box)], mono)

    for line in placed[0].lines:
        assert mono(line, placed[0].font_size) <= box.w - 2 * PADDING, (
            "слово без пробелов обязано резаться по символам, иначе текст уезжает за бокс"
        )
    assert "".join(placed[0].lines) == word, "символы при нарезке не теряются"


def test_font_shrinks_for_low_wide_box() -> None:
    wide = "альфа бета гамма дельта эпсилон дзета эта тета йота каппа лямбда мю " * 3
    box = Box(x=0, y=0, w=600, h=80)

    placed = layout_blocks([block(wide, box)], mono)

    start_size = min(box.h // 3, box.w // 4, 48)
    assert placed[0].font_size < start_size, "высота бокса обязана уменьшать кегль"
    assert len(placed[0].lines) * placed[0].font_size * 1.2 <= box.h - 2 * PADDING


def test_degenerate_box_is_skipped() -> None:
    placed = layout_blocks([block("Привет", Box(x=0, y=0, w=0, h=0))], mono)

    assert placed == [], "нулевая рамка не должна попадать в вёрстку (ValueError Pillow)"


def test_font_never_goes_below_floor() -> None:
    words = " ".join(["оченьдлинноеслово"] * 20)

    placed = layout_blocks([block(words, Box(x=0, y=0, w=40, h=40))], mono)

    assert placed[0].font_size >= MIN_FONT_SIZE


def test_placed_box_is_block_box() -> None:
    box = Box(x=10, y=20, w=300, h=150)

    placed = layout_blocks([block("Текст", box)], mono)

    assert placed[0].box == box
    assert placed[0].block_id == "p1-b1"


def test_blocks_without_text_are_skipped() -> None:
    placed = layout_blocks([block("   ")], mono)

    assert placed == []


def test_nothing_to_typeset_returns_none(page_png) -> None:
    page = page_png(400, 300)

    assert Typesetter().render(page, []) is None, (
        "нечего верстать — вызывающий код обязан отдать оверлей фазы 1, "
        "а не положить оригинал под ключ web.webp"
    )


def test_degenerate_boxes_alone_give_none(page_png) -> None:
    page = page_png(400, 300)

    assert Typesetter().render(page, [block("Привет", Box(x=0, y=0, w=0, h=0))]) is None


def test_render_produces_webp_of_the_same_size(page_png) -> None:
    page = page_png(400, 300)

    rendered = Typesetter(quality=85).render(page, [block("Привет", Box(x=20, y=20, w=300, h=120))])

    with Image.open(io.BytesIO(rendered)) as picture:
        assert picture.format == "WEBP"
        assert picture.size == (400, 300)


def test_render_paints_plaque_inside_the_box_only(page_png) -> None:
    page = page_png(400, 300, color=(200, 200, 200))
    box = Box(x=20, y=20, w=300, h=120)

    rendered = Typesetter().render(page, [block("Привет", box)])

    with Image.open(io.BytesIO(rendered)) as picture:
        pixels = picture.convert("RGB")
        dark_inside = sum(
            1
            for x in range(box.x, box.x + box.w)
            for y in range(box.y, box.y + box.h)
            if sum(pixels.getpixel((x, y))) < 3 * 120
        )
        # угол внутри плашки — заливка (белая), а не фон страницы (серый)
        corner_inside = pixels.getpixel((box.x + 6, box.y + 6))
        # всё, что вне бокса, осталось фоном страницы
        outside = {
            pixels.getpixel((x, y))
            for x in range(0, 400, 7)
            for y in range(0, 300, 7)
            if not (box.x - 2 <= x <= box.x + box.w + 1 and box.y - 2 <= y <= box.y + box.h + 1)
        }

    assert dark_inside > 0, "текст и обводка плашки обязаны дать непустые пиксели"
    assert corner_inside == (255, 255, 255), "внутри плашки белая заливка, а не фон"
    assert outside == {(200, 200, 200)}, "вне бокса страница не меняется"
