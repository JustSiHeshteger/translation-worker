"""Вёрстка блоков: перенос по словам и подбор кегля (карточка 55 §2.13).

Шрифт подбирается бинарным поиском в [10, 40] px по критерию «текст влезает
в рамку»; при переполнении на минимальном кегле блок не теряется — текст
переносится с пометкой `\\n`, а в отчёт страницы попадает warning
`page N block K: overflow`.
"""

from __future__ import annotations

from translation_worker.layout import fit_blocks
from translation_worker.models import Box, TextBlock

BOX_WIDE = Box(x=100, y=100, w=400, h=200)
BOX_SMALL = Box(x=100, y=400, w=120, h=60)


def block(text: str, box: Box = BOX_WIDE) -> TextBlock:
    return TextBlock(id="b1", box=box, kind="BUBBLE", original=text, text=text)


def test_long_text_gets_smaller_font_than_short_text():
    short_blocks, _ = fit_blocks([block("кот")], page=1)
    long_blocks, _ = fit_blocks(
        [block("Ты опоздал, кот, и я ждал тебя весь вечер у входа")], page=1
    )

    assert short_blocks[0].font_size >= long_blocks[0].font_size
    assert long_blocks[0].font_size >= 10


def test_font_size_is_never_below_minimum():
    blocks, warnings = fit_blocks(
        [block("очень длинный текст в крошечной рамке без единой возможности влезть", BOX_SMALL)],
        page=3,
    )
    assert blocks[0].font_size >= 10
    assert warnings == ["page 3 block 1: overflow"]


def test_overflow_keeps_all_words():
    text = "Ты опоздал, кот, и я ждал тебя весь вечер у самого входа в подъезд"
    blocks, _ = fit_blocks([block(text, BOX_SMALL)], page=1)

    # текст не теряется: перенос по словам, никакого обрезания/многоточия
    assert blocks[0].text.replace("\n", " ") == text
    assert "…" not in blocks[0].text


def test_wrapping_inserts_line_breaks_for_narrow_box():
    blocks, _ = fit_blocks([block("Ты опоздал, кот, и я ждал тебя", BOX_SMALL)], page=1)
    assert "\n" in blocks[0].text


def test_empty_text_blocks_are_dropped():
    blocks, warnings = fit_blocks(
        [block("   ", BOX_WIDE), block("кот", BOX_WIDE)],
        page=1,
    )
    assert [item.text for item in blocks] == ["кот"]
    assert warnings == []


def test_font_size_is_capped_at_maximum():
    blocks, _ = fit_blocks([block("кот", Box(x=0, y=0, w=2000, h=1000))], page=1)
    assert blocks[0].font_size <= 40


def test_multiple_blocks_keep_order_and_report_per_block_warnings():
    blocks, warnings = fit_blocks(
        [
            block("кот"),
            block("невероятно длинный текст, который физически не влезает в эту рамку", BOX_SMALL),
        ],
        page=5,
    )
    assert len(blocks) == 2
    assert warnings == ["page 5 block 2: overflow"]


def test_original_text_of_block_is_preserved():
    blocks, _ = fit_blocks([block("You're late, cat.")], page=1)
    assert blocks[0].original == "You're late, cat."
