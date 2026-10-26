"""Перенос слов по слогам с дефисом (правило владельца стенда).

Было: не влезающее слово резалось по символам, и в балуне появлялось
«штаб-квартир» / «у». Стало: для кириллицы перенос по слогам с дефисом
(«квар-» / «ти-» / «ру»), на строке не меньше трёх знаков, одна буква не
остаётся нигде, а кегль уменьшается раньше, чем слово рвётся.

Латиница правил переноса не имеет: для неё остаётся прежний разрез по
символам без дефиса — он безопасен и не портит вид.
"""

from __future__ import annotations

import pytest
from translation_worker.models import Box, TextBlock
from translation_worker.providers.typeset import MIN_FONT_SIZE, layout_blocks

#: Мерило как в `test_typeset`: символ — 0.6 кегля.
MONO = 0.6


def mono(text: str, size: int) -> float:
    return MONO * size * len(text)


def block(text: str, box: Box | None = None) -> TextBlock:
    return TextBlock(
        id="p1-b1",
        box=box or Box(x=100, y=200, w=400, h=200),
        kind="BUBBLE",
        original="original",
        text=text,
        confidence=0.9,
    )


def lines_for(text: str, chars_per_line: float, *, height: int = 400) -> list[str]:
    """Разложить текст в рамку, где даже минимальный кегль не держит слово целиком.

    Ширина считается под `MIN_FONT_SIZE`, поэтому подбор кегля упирается в
    минимум и в работу вступает перенос — иначе тест про перенос проверял бы
    подобранный кегль, а не перенос.
    """
    width = int(chars_per_line * MONO * MIN_FONT_SIZE) + 2 * 4
    placed = layout_blocks([block(text, Box(x=0, y=0, w=width, h=height))], mono)
    return placed[0].lines


def letters(lines: list[str]) -> str:
    """Слово без знаков переноса — для проверки «ничего не потеряно»."""
    return "".join(lines).replace("-", "")


def test_cyrillic_word_is_split_by_syllables_with_hyphen() -> None:
    lines = lines_for("квартира", 5)

    assert lines == ["квар-", "тира"], lines
    assert letters(lines) == "квартира"


def test_a_single_letter_never_stays_on_a_line() -> None:
    """Случай со стенда: «штаб-квартир» / «у» больше не воспроизводится."""
    lines = lines_for("штаб-квартиру", 7)

    assert all(len(line.rstrip("-")) >= 3 for line in lines), lines
    assert "у" not in lines, lines
    assert letters(lines) == "штабквартиру", lines


def test_existing_hyphen_is_the_first_break_point() -> None:
    lines = lines_for("штаб-квартиру", 6)

    assert lines[0] == "штаб-", f"готовый дефис — естественная точка переноса: {lines}"
    assert letters(lines) == "штабквартиру"


def test_every_broken_line_ends_with_a_hyphen() -> None:
    lines = lines_for("переводчик", 4)

    assert len(lines) > 1, lines
    assert all(line.endswith("-") for line in lines[:-1]), lines
    assert not lines[-1].endswith("-"), "последняя строка дефиса не получает"


def test_no_line_starts_with_a_soft_or_hard_sign() -> None:
    lines = lines_for("объявление", 5)

    assert all(not line.startswith(("ь", "ъ", "й")) for line in lines), lines


def test_ukrainian_and_belarusian_letters_are_hyphenated() -> None:
    for word in ("переїзд", "літаки", "беларускі"):
        lines = lines_for(word, 5)
        assert all(len(line.rstrip("-")) >= 3 for line in lines), (word, lines)
        assert letters(lines) == word.replace("-", ""), (word, lines)


def test_latin_word_never_ends_with_a_lone_letter() -> None:
    """Латиница без дефисов, но одна буква в конце строки недопустима и здесь."""
    lines = lines_for("PETERPARKER", 5)

    assert all(len(line) >= 2 for line in lines), lines
    assert "".join(lines) == "PETERPARKER", lines


def test_overflow_is_logged_when_even_two_characters_do_not_fit(caplog) -> None:
    """Совсем узкая плашка: остаток вылезает, но это видно в логе."""
    with caplog.at_level("WARNING"):
        placed = layout_blocks([block("ВЛАСТЬ", Box(x=0, y=0, w=13, h=7))], mono)

    assert placed[0].font_size == MIN_FONT_SIZE, placed[0].font_size
    assert any("не влезает в рамку" in record.message for record in caplog.records), (
        f"нет предупреждения о вылезшем тексте: {[r.message for r in caplog.records]}"
    )


def test_latin_word_keeps_the_plain_character_split() -> None:
    """Латиница без правил переноса: режем по символам, дефис не выдумываем."""
    lines = lines_for("PETERPARKER", 4)

    assert "".join(lines) == "PETERPARKER", lines
    assert not any(line.endswith("-") for line in lines), lines


def test_font_shrinks_before_the_word_is_broken() -> None:
    """Сначала уменьшается кегль: слово целиком лучше переноса."""
    # при кегле 48 слово шире плашки, при 40 — ровно влезает: кегль и уступает
    placed = layout_blocks([block("квартира", Box(x=0, y=0, w=200, h=300))], mono)

    assert placed[0].lines == ["квартира"], placed[0].lines
    assert placed[0].font_size == 40, placed[0].font_size


def test_hyphenation_is_the_last_resort_on_a_narrow_plaque() -> None:
    """Узкая плашка: цельные слова не влезают — слово переносится по слогам."""
    size = MIN_FONT_SIZE
    width = int(7 * MONO * size) + 2 * 4
    placed = layout_blocks([block("переводчик", Box(x=0, y=0, w=width, h=90))], mono)

    lines = placed[0].lines
    assert placed[0].font_size >= MIN_FONT_SIZE, "кегль не уходит ниже минимума"
    assert len(lines) > 1, lines
    assert any(line.endswith("-") for line in lines[:-1]), lines
    assert letters(lines) == "переводчик", lines


@pytest.mark.parametrize(("chars", "min_letters"), [(4, 2), (5, 3), (6, 3)])
def test_text_is_never_lost_at_any_width(chars: int, min_letters: int) -> None:
    """На четырёх знаках в строке три буквы с дефисом не влезают — но не одна."""
    lines = lines_for("электрификация", chars)

    assert letters(lines) == "электрификация", (chars, lines)
    assert all(len(line.rstrip("-")) >= min_letters for line in lines), (chars, lines)


def test_two_signs_per_line_never_gives_a_lone_letter() -> None:
    """Совсем узкая плашка: слово идёт целиком, но куска из одной буквы нет."""
    lines = lines_for("квартира", 2)

    assert all(len(line.rstrip("-")) >= 2 for line in lines), lines
    assert letters(lines) == "квартира", lines


def test_existing_hyphen_never_starts_a_line() -> None:
    """Готовый дефис не должен оказаться отдельной строкой («б-» / «-»)."""
    lines = lines_for("штаб-квартиру", 2)

    assert "-" not in lines, lines
    assert all(not line.startswith("-") for line in lines), lines


def test_hyphenation_works_at_a_readable_font_size() -> None:
    """Перенос — не только на 8 px: кегль уступает, но остаётся читаемым."""
    # длинное слово в узкой плашке: цельным оно требует кегля ниже 60 % от старта
    placed = layout_blocks(
        [block("переворачивающийся", Box(x=0, y=0, w=150, h=200))], mono
    )

    assert placed[0].font_size > MIN_FONT_SIZE, placed[0].font_size
    assert any(line.endswith("-") for line in placed[0].lines[:-1]), placed[0].lines


def test_tail_is_not_widened_beyond_the_plaque() -> None:
    """Расширенный хвост перемеряется: перенос отменяется, если не влезает."""
    from translation_worker.providers.typeset import _move_letters_to_tail

    def wide(text: str, size: int) -> float:
        """Каждый знак — 12 px: три знака влезают в 33 px, четыре уже нет."""
        return 12.0 * len(text)

    pieces = ["ннн-", "нш"]
    _move_letters_to_tail(pieces, wide, 10, 33)

    assert pieces == ["ннн-", "нш"], pieces

    # а при бюджете пошире хвост выравнивается: три буквы уходят в конец
    roomy = ["ннн-", "нш"]
    _move_letters_to_tail(roomy, wide, 10, 37)

    assert roomy == ["нн-", "ннш"], roomy


def test_syllable_break_is_preferred_over_a_character_cut() -> None:
    """«пере-» лучше «пер-»: слоговая точка выбирается раньше буквенной."""
    from translation_worker.providers.typeset import _break_gaps

    gaps = _break_gaps("переводчик", 3)
    assert gaps, "слоговые точки обязаны находиться"
    assert 4 in gaps, f"«пере-» — законный слог: {gaps}"


def test_a_piece_without_a_vowel_is_not_a_syllable() -> None:
    """«взг-ляд» — не перенос: в куске нет гласной."""
    from translation_worker.providers.typeset import _break_gaps

    assert 3 not in _break_gaps("взглядом", 3)


def test_latin_word_with_a_cyrillic_letter_stays_latin() -> None:
    """Правило по большинству букв: «iPhone-переводчик» режется как латиница."""
    from translation_worker.providers.typeset import _is_cyrillic, _split_word

    assert _is_cyrillic("переводчик") is True
    assert _is_cyrillic("iPhone12-п") is False
    pieces = _split_word("iPhone12-п", mono, MIN_FONT_SIZE, 33)
    assert not any(piece.endswith("-") for piece in pieces), pieces
