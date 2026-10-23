"""Группировка строк OCR в блоки-баблы (карточка 55 §2.12).

Правило фиксировано карточкой и тестами `tests/test_grouping.py`: две строки
принадлежат одному баблу, если их вертикальные интервалы пересекаются или почти
касаются (зазор не больше пятой части высоты строки — строки одной реплики идут
вплотную, с межстрочным шагом) И горизонтальный зазор между ними не больше
`0.6 * высоты строки`. Внутри блока строки идут сверху вниз, `original`
склеивается через пробел, `box` — объединение рамок, `confidence` — минимум по
строкам. `kind` в фазе 1 всегда `BUBBLE` (SFX отдельно не выделяются, D-008);
`id` и `text` пустые — их ставит пайплайн (`p{page}-b{i}`) и переводчик.
"""

from __future__ import annotations

from translation_worker.models import Box, TextBlock, TextLine

_VERTICAL_GAP_RATIO = 0.2
_HORIZONTAL_GAP_RATIO = 0.6


def group_lines(lines: list[TextLine], *, min_confidence: float = 0.5) -> list[TextBlock]:
    """Собрать строки в блоки (шум с низким confidence отбрасывается)."""
    candidates = [
        line for line in lines if line.confidence >= min_confidence and line.text.strip()
    ]
    if not candidates:
        return []

    clusters = _cluster(candidates)
    blocks: list[TextBlock] = []
    for members in clusters:
        ordered = sorted(members, key=lambda line: (line.box.y, line.box.x))
        box = ordered[0].box
        for line in ordered[1:]:
            box = box.union(line.box)
        blocks.append(
            TextBlock(
                id="",
                box=box,
                kind="BUBBLE",
                original=" ".join(line.text.strip() for line in ordered),
                text="",
                confidence=min(line.confidence for line in ordered),
            )
        )
    blocks.sort(key=lambda block: (block.box.y, block.box.x))
    return blocks


def _cluster(lines: list[TextLine]) -> list[list[TextLine]]:
    """Транзитивное замыкание «в одном бабле» (union-find по парам строк)."""
    parents = list(range(len(lines)))

    def find(index: int) -> int:
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    for left in range(len(lines)):
        for right in range(left + 1, len(lines)):
            if _same_bubble(lines[left], lines[right]):
                parents[find(right)] = find(left)

    grouped: dict[int, list[TextLine]] = {}
    for index, line in enumerate(lines):
        grouped.setdefault(find(index), []).append(line)
    return list(grouped.values())


def _same_bubble(left: TextLine, right: TextLine) -> bool:
    """Одна реплика: строки рядом по вертикали и без большого горизонтального зазора."""
    height = min(left.box.h, right.box.h)
    if height <= 0:
        return False
    if _vertical_gap(left.box, right.box) > _VERTICAL_GAP_RATIO * height:
        return False
    return _horizontal_gap(left.box, right.box) <= _HORIZONTAL_GAP_RATIO * height


def _vertical_gap(left: Box, right: Box) -> float:
    """Зазор между вертикальными интервалами (0, если интервалы пересекаются)."""
    return max(0, max(left.y, right.y) - min(left.bottom, right.bottom))


def _horizontal_gap(left: Box, right: Box) -> float:
    """Зазор между горизонтальными интервалами (0, если интервалы пересекаются)."""
    return max(0, max(left.x, right.x) - min(left.right, right.right))
