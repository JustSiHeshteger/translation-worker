"""Dev-провайдеры для docker-compose (карточка 55 §2.8).

Включаются настройками `TRANSLATION_OCR_PROVIDER=fake` и
`TRANSLATION_TRANSLATE_PROVIDER=STUB`: сервис проходит весь конвейер без GPU,
моделей и ключей, что и нужно для компоуза и e2e. Тестовые фейки живут в
`tests/conftest.py` (зона облака) и от этих классов не зависят.
"""

from __future__ import annotations

from dataclasses import dataclass

from translation_worker.models import Box, TextLine
from translation_worker.providers.stub import PREFIX, StubTranslator


@dataclass
class FakeOcr:
    """Детерминированный OCR: `lines_per_page` строк `line {i}` в сетке страницы."""

    lines_per_page: int = 3
    confidence: float = 0.95

    async def recognize(self, image: bytes) -> list[TextLine]:
        return [
            TextLine(
                text=f"line {index}",
                box=Box(x=120, y=220 + index * 110, w=420, h=70),
                confidence=self.confidence,
            )
            for index in range(self.lines_per_page)
        ]


@dataclass
class FakeTranslator(StubTranslator):
    """Dev-переводчик: тот же детерминированный STUB с префиксом `переведено: `."""

    prefix: str = PREFIX
