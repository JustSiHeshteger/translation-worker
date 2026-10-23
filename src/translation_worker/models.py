"""Модели данных конвейера перевода (карточка 55 §2.3, §2.Т).

`Box`/`TextLine`/`TextBlock` — геометрия и текст одного бабла; `Layer` — слой
страницы ровно в формате карты цепочки (§ «Хранилище»), единственный артефакт,
который читает фронт; `PageOutcome` — сводка по готовой странице.

`layer.to_json()` побайтово детерминирован: никаких временных меток внутри
слоя, ключи пишутся в стабильном порядке (сортировку делает хранилище).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Box:
    """Рамка блока в пикселях оригинала страницы."""

    x: int
    y: int
    w: int
    h: int

    @property
    def right(self) -> int:
        return self.x + self.w

    @property
    def bottom(self) -> int:
        return self.y + self.h

    @property
    def area(self) -> int:
        return self.w * self.h

    def union(self, other: Box) -> Box:
        """Наименьшая рамка, содержащая обе исходные."""
        left = min(self.x, other.x)
        top = min(self.y, other.y)
        return Box(
            x=left,
            y=top,
            w=max(self.right, other.right) - left,
            h=max(self.bottom, other.bottom) - top,
        )

    def to_json(self) -> dict[str, int]:
        return {"x": self.x, "y": self.y, "w": self.w, "h": self.h}

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> Box:
        return cls(x=int(data["x"]), y=int(data["y"]), w=int(data["w"]), h=int(data["h"]))


@dataclass
class TextLine:
    """Строка, распознанная OCR-провайдером."""

    text: str
    box: Box
    confidence: float


@dataclass
class TextBlock:
    """Блок текста (в фазе 1 — бабл) внутри слоя страницы."""

    id: str
    box: Box
    kind: str = "BUBBLE"
    original: str = ""
    text: str = ""
    confidence: float = 0.0
    font_size: int | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "box": self.box.to_json(),
            "kind": self.kind,
            "original": self.original,
            "text": self.text,
            "confidence": self.confidence,
            "fontSize": self.font_size,
        }

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> TextBlock:
        return cls(
            id=str(data.get("id") or ""),
            box=Box.from_json(data["box"]),
            kind=str(data.get("kind") or "BUBBLE"),
            original=str(data.get("original") or ""),
            text=str(data.get("text") or ""),
            confidence=float(data.get("confidence") or 0.0),
            font_size=int(data["fontSize"]) if data.get("fontSize") is not None else None,
        )


@dataclass
class Layer:
    """Слой текста страницы (`layer.json`)."""

    version: int = 1
    page: int = 0
    language: str = ""
    source_language: str | None = None
    width: int = 0
    height: int = 0
    provider: str = ""
    blocks: list[TextBlock] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "page": self.page,
            "language": self.language,
            "sourceLanguage": self.source_language,
            "width": self.width,
            "height": self.height,
            "provider": self.provider,
            "blocks": [block.to_json() for block in self.blocks],
        }


@dataclass
class PageOutcome:
    """Итог обработки одной страницы (материал для событий и логов)."""

    number: int
    layer_key: str
    image_key: str | None = None
    blocks: int = 0
    warnings: list[str] = field(default_factory=list)
