"""Группировка строк OCR в блоки (карточка 55 §2.12).

Правило (фиксировано карточкой): строки принадлежат одному баблу, если
вертикальные интервалы пересекаются не менее чем на 50 % И горизонтальный
зазор не больше 0.6 высоты строки. Внутри блока строки идут по y, текст
склеивается через пробел, confidence = минимум по строкам, box = объединение.
Блоки упорядочены сверху вниз, затем слева направо. kind фазы 1 — всегда BUBBLE.
"""

from __future__ import annotations

from translation_worker.grouping import group_lines
from translation_worker.models import Box, TextLine


def line(text: str, x: int, y: int, w: int, h: int, confidence: float = 0.9) -> TextLine:
    return TextLine(text=text, box=Box(x=x, y=y, w=w, h=h), confidence=confidence)


def test_lines_of_one_bubble_are_merged_in_reading_order():
    blocks = group_lines(
        [
            line("Ты опоздал,", 100, 200, 200, 40),
            line("кот.", 100, 245, 90, 40),
            line("Второй бабл", 700, 200, 220, 40),
        ]
    )

    assert len(blocks) == 2
    first = blocks[0]
    assert first.original == "Ты опоздал, кот."
    assert first.box == Box(x=100, y=200, w=200, h=85)
    assert first.kind == "BUBBLE"


def test_blocks_are_ordered_by_position():
    blocks = group_lines(
        [
            line("низ", 100, 900, 200, 40),
            line("верх-право", 800, 100, 200, 40),
            line("верх-лево", 100, 100, 200, 40),
        ]
    )
    assert [block.original for block in blocks] == ["верх-лево", "верх-право", "низ"]


def test_box_is_union_of_merged_lines():
    blocks = group_lines(
        [
            line("a", 100, 200, 120, 40),
            line("b", 130, 240, 300, 40),
        ]
    )
    assert len(blocks) == 1
    assert blocks[0].box == Box(x=100, y=200, w=330, h=80)


def test_confidence_is_minimum_of_merged_lines():
    blocks = group_lines(
        [
            line("a", 100, 200, 120, 40, confidence=0.95),
            line("b", 100, 240, 120, 40, confidence=0.61),
        ]
    )
    assert blocks[0].confidence == 0.61


def test_low_confidence_lines_are_dropped():
    blocks = group_lines(
        [
            line("шум", 100, 200, 120, 40, confidence=0.2),
            line("текст", 100, 600, 120, 40, confidence=0.8),
        ],
        min_confidence=0.5,
    )
    assert [block.original for block in blocks] == ["текст"]


def test_far_apart_lines_are_not_merged():
    blocks = group_lines(
        [
            line("первый", 100, 200, 120, 40),
            line("второй", 100, 400, 120, 40),
        ]
    )
    assert len(blocks) == 2


def test_horizontal_gap_above_threshold_splits_bubbles():
    blocks = group_lines(
        [
            line("лево", 100, 200, 100, 40),
            # зазор 300 px > 0.6 * 40 = 24 px
            line("право", 500, 200, 100, 40),
        ]
    )
    assert len(blocks) == 2


def test_empty_input_yields_no_blocks():
    assert group_lines([]) == []


def test_blocks_have_no_ids_before_page_binding():
    """id блоков присваивает пайплайн (p{page}-b{i}): в группировке id пуст."""
    blocks = group_lines([line("a", 100, 200, 120, 40)])
    assert blocks[0].id == ""
    assert blocks[0].text == ""
