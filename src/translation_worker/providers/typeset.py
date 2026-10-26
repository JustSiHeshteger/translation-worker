"""Вёрстка перевода в баблы арт-слоя (карточка 58 §2.2).

Инвариант карточки: текст влезает в `box` — перенос по словам, при
переполнении кегль уменьшается до `MIN_FONT_SIZE` (8 px), при необходимости слово
переносится по слогам с дефисом, плашка не выходит за границы изображения.
Единственное исключение — плашка, где даже два знака с дефисом не помещаются:
остаток остаётся одной строкой, вылезает за рамку и пишется в лог
предупреждением (иначе вылезший текст неотличим от нормальной вёрстки). Вёрстка
отделена от рисования: `layout_blocks` работает с функцией измерения текста,
поэтому перенос и подбор кегля проверяются без шрифтов и пикселей (в тестах —
детерминированная моноширинная мера).
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

#: Радиус скругления плашки, px.
PLAQUE_RADIUS = 10

#: Знак переноса.
HYPHEN = "-"

#: Минимум знаков по обе стороны переноса: одна буква на строке — брак вёрстки.
MIN_HYPHEN_PIECE = 3

#: До какой доли стартового кегля уступает шрифт, прежде чем слово переносится.
MIN_SHRINK_PERCENT = 60

#: Гласные кириллицы: по ним считаются слоги при переносе.
_CYRILLIC_VOWELS = frozenset("аеёиоуыэюяіїєґў")

#: Буквы кириллицы (ru/uk/be): только для них действуют слоговые правила.
_CYRILLIC_LETTERS = frozenset("абвгдеёжзийклмнопрстуфхцчшщъыьэюяіїєґў")

#: С этих букв строку начинать нельзя (русская типографика).
_NO_LINE_START = frozenset("ьъй")

#: Цвета плашки: белая заливка без обводки, чёрный текст. Обводки нет намеренно:
#: балун на странице уже обведён автором, и вторая линия вокруг перевода читается
#: как двойной блок.
PLAQUE_FILL = (255, 255, 255)
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

    Два прохода: сначала кегль уменьшается при цельных словах (уступает до
    `MIN_SHRINK_PERCENT` от стартового), и только если этого не хватило, слова
    переносятся — снова с самого крупного кегля. Ниже `min_font_size` не
    опускается (читаемость важнее полноты). Порядок блоков сохраняется.
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
        start_size = max(min_font_size, min(block.box.h // 3, block.box.w // 4, 48))
        # Сначала кегль: слово целиком читается лучше, чем перенесённое, поэтому
        # шрифт уменьшается, пока слова ещё цельные. Уступаем не бесконечно —
        # ниже `shrink_floor` текст уже нечитаем, и тогда честнее перенести
        # слово по слогам, чем отдать читателю 8 px.
        shrink_floor = max(min_font_size, start_size * MIN_SHRINK_PERCENT // 100)
        size, lines = _fit_words(
            text, measure, start_size, shrink_floor, max_width, max_height, allow_break=False
        )
        if not _fits(lines, measure, size, max_width, max_height):
            # Перенос разрешён: ищем с самого крупного кегля, иначе перенесённый
            # текст окажется мельче, чем позволяет плашка. Цельные слова на этих
            # кеглях уже проверены и не влезли, так что порядок «кегль, потом
            # перенос» соблюдён.
            size, lines = _fit_words(
                text, measure, start_size, min_font_size, max_width, max_height,
                allow_break=True,
            )
        if not _fits(lines, measure, size, max_width, max_height):
            # перенос не помог: плашка уже минимального кегля (13×7 px на полях).
            # Пишем в лог — иначе вылезший текст неотличим от нормальной вёрстки.
            _LOG.warning(
                "блок %s не влезает в рамку даже на %d px: %r",
                block.id,
                size,
                lines,
            )
        placed.append(
            PlacedText(block_id=block.id, box=block.box, lines=lines, font_size=size)
        )
    return placed


def _fit_words(
    text: str,
    measure: MeasureText,
    start_size: int,
    floor_size: int,
    max_width: float,
    max_height: float,
    *,
    allow_break: bool,
) -> tuple[int, list[str]]:
    """Наибольший кегль от `start_size` до `floor_size`, при котором текст помещается."""
    size = start_size
    lines = _wrap(text, measure, size, max_width, allow_break=allow_break)
    while size > floor_size and not _fits(lines, measure, size, max_width, max_height):
        size -= 1
        lines = _wrap(text, measure, size, max_width, allow_break=allow_break)
    return size, lines


def _wrap(
    text: str,
    measure: MeasureText,
    size: int,
    max_width: float,
    *,
    allow_break: bool = True,
) -> list[str]:
    """Перенос по словам: строка не шире `max_width`, текст не теряется.

    Слово, которое не влезает в строку целиком (длинный ник, URL, «ыыы…»),
    режется: иначе оно вылезло бы за плашку и за пределы бокса, хотя перенос
    «по словам» уже ничего не может сделать. `allow_break=False` запрещает
    резать слова — так подбор кегля успевает уменьшить шрифт раньше.
    """
    lines: list[str] = []
    current = ""
    for word in text.split():
        for index, piece in enumerate(_split_word(word, measure, size, max_width, allow_break)):
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


def _split_word(
    word: str, measure: MeasureText, size: int, max_width: float, allow_break: bool = True
) -> list[str]:
    """Нарезать слово, чтобы каждый кусок влезал в строку.

    Кириллица режется по слогам с дефисом («квар-» / «ти-» / «ру»), латиница —
    по символам без дефиса: слоговых правил для неё нет, а выдуманный перенос
    выглядел бы опечаткой. Если не влезает ни один слог — крайний случай,
    разрез по символам: текст важнее вида.
    """
    if measure(word, size) <= max_width or not allow_break:
        return [word]
    if _is_cyrillic(word):
        return _hyphenate(word, measure, size, max_width)
    return _split_by_chars(word, measure, size, max_width)


def _is_cyrillic(word: str) -> bool:
    """Слово написано кириллицей — для него действуют слоговые правила.

    Правило по большинству букв, а не по первой встречной: «iPhone-переводчик»
    остаётся латиницей, «переводчик» — кириллицей.
    """
    letters = [char.lower() for char in word if char.isalpha()]
    if not letters:
        return False
    cyrillic = sum(1 for char in letters if char in _CYRILLIC_LETTERS)
    return cyrillic * 2 > len(letters)


def _cluster_length(text: str) -> int:
    """Сколько согласных идёт подряд с начала `text`."""
    length = 0
    for char in text:
        if char.lower() in _CYRILLIC_VOWELS or not char.isalpha():
            break
        length += 1
    return length


def _break_gaps(word: str, min_piece: int) -> list[int]:
    """Позиции, где слово переносится по слогам (индекс — граница кусков).

    Правила русской переносицы в упрощённом виде: готовый дефис — точка
    переноса; после гласной перенос допустим, только если следующий слог
    начинается одной согласной («ти|ра», но не «ква|ртира»); внутри группы
    согласных перенос разрешён («квар|ти», «солн|це»); перед гласной согласную
    не отрывают («кварт|ира» нельзя); в куске обязана быть гласная («взг|ляд»
    не годится). Строка не начинается с «ь/ъ/й», и по обе стороны переноса
    остаётся не меньше `min_piece` знаков.
    """
    gaps: list[int] = []
    for index in range(1, len(word)):
        left, right = word[:index], word[index:]
        if len(left.rstrip(HYPHEN)) < min_piece or len(right) < min_piece:
            continue
        if right[0].lower() in _NO_LINE_START:
            continue
        if left.endswith(HYPHEN):
            gaps.append(index)
            continue
        if not (left[-1].isalpha() and right[0].isalpha()):
            continue
        # слог без гласной не слог: «взг-ляд» читается как обрыв
        if not any(char.lower() in _CYRILLIC_VOWELS for char in left):
            continue
        if left[-1].lower() in _CYRILLIC_VOWELS:
            single_consonant = _cluster_length(right) == 1
            if right[0].lower() in _CYRILLIC_VOWELS or single_consonant:
                gaps.append(index)
        elif right[0].lower() not in _CYRILLIC_VOWELS:
            gaps.append(index)
    return gaps


def _hyphenate(word: str, measure: MeasureText, size: int, max_width: float) -> list[str]:
    """Разрезать слово по слогам: все куски, кроме последнего, кончаются дефисом.

    Кусок из одной буквы не появляется: если даже два знака с дефисом не влезают
    (плашка шириной пару сантиметров), остаток идёт одной строкой — читатель
    увидит целое слово, вылезшее за плашку, вместо «…квартир» / «у».
    """
    pieces: list[str] = []
    rest = word
    while measure(rest, size) > max_width:
        cut = _longest_piece(rest, measure, size, max_width)
        if cut is None:
            break
        piece = rest[:cut]
        rest = rest[cut:]
        pieces.append(piece if piece.endswith(HYPHEN) else f"{piece}{HYPHEN}")
    if rest:
        pieces.append(rest)
    _move_letters_to_tail(pieces, measure, size, max_width)
    return pieces


def _letters(piece: str) -> int:
    """Сколько в куске настоящих знаков (дефис переноса не считается)."""
    return len(piece.rstrip(HYPHEN))


def _move_letters_to_tail(
    pieces: list[str], measure: MeasureText, size: int, max_width: float
) -> None:
    """Не оставлять в конце одну-две буквы: «…квартир» / «у» — брак вёрстки.

    Из предыдущего куска буквы переносятся в хвост (общая длина слова не
    меняется), пока хвост не наберёт `MIN_HYPHEN_PIECE` знаков или предыдущий
    кусок не опустится до двух. Расширенный хвост перемеряется: если он вылезает
    за плашку, перенос отменяется — лучше короткий хвост, чем текст за рамкой.
    """
    while len(pieces) > 1 and _letters(pieces[-1]) < MIN_HYPHEN_PIECE:
        if _letters(pieces[-2]) <= 2:
            return
        head = pieces[-2].rstrip(HYPHEN)
        tail = f"{head[-1]}{pieces[-1]}"
        if measure(tail, size) > max_width:
            return
        pieces[-1] = tail
        pieces[-2] = f"{head[:-1]}{HYPHEN}"


def _longest_piece(
    rest: str, measure: MeasureText, size: int, max_width: float
) -> int | None:
    """Самая длинная точка переноса, которая влезает в строку вместе с дефисом.

    Порядок — от лучшего к худшему: слог с куском от трёх знаков, слог с двумя,
    затем разрез по символам с теми же порогами. Слоговый перенос всегда
    предпочтительнее буквенного: «пере-во-дчик» лучше «пер-ево-дчик».
    """
    for min_piece in (MIN_HYPHEN_PIECE, 2):
        # от длинного куска к короткому: строку заполняем по максимуму
        for gap in reversed(_break_gaps(rest, min_piece)):
            piece = rest[:gap].rstrip(HYPHEN)
            if measure(f"{piece}{HYPHEN}", size) <= max_width:
                return gap
    for min_piece in (MIN_HYPHEN_PIECE, 2):
        cut = _char_cut(rest, measure, size, max_width, min_piece)
        if cut is not None:
            return cut
    return None


def _char_cut(
    rest: str, measure: MeasureText, size: int, max_width: float, min_piece: int
) -> int | None:
    """Сколько знаков влезает с дефисом, оставив после переноса `min_piece` знаков."""
    lowest = max(1, min_piece)
    highest = len(rest) - max(1, min_piece)
    cut: int | None = None
    for index in range(lowest, highest + 1):
        if rest[index].lower() in _NO_LINE_START or rest[index] == HYPHEN:
            # строка не начинается с «ь/ъ/й» и с готового дефиса
            continue
        if measure(f"{rest[:index]}{HYPHEN}", size) <= max_width:
            cut = index
    return cut


def _split_by_chars(word: str, measure: MeasureText, size: int, max_width: float) -> list[str]:
    """Разрезать слово по символам (латиница и всё, что без правил переноса).

    Дефисы не добавляются: в латинице выдуманный перенос читается как опечатка.
    Одна буква в конце строки недопустима и здесь — хвост выравнивается за счёт
    соседнего куска, если тот не тоньше двух знаков.
    """
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
    while len(pieces) > 1 and len(pieces[-1]) < 2 and len(pieces[-2]) > 2:
        tail = f"{pieces[-2][-1]}{pieces[-1]}"
        if measure(tail, size) > max_width:
            break
        pieces[-1] = tail
        pieces[-2] = pieces[-2][:-1]
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
    """Рисование белых плашек и текста поверх заинпанченной страницы (WEBP)."""

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
    "PADDING",
    "PLAQUE_FILL",
    "TEXT_COLOR",
    "MeasureText",
    "PlacedText",
    "Typesetter",
    "layout_blocks",
    "pillow_measure",
]
