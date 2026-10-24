"""STUB-провайдеры: детерминированный перевод и пустой рендер (карточка 55 §2.8).

`StubTranslator` выбирается настройкой `TRANSLATION_TRANSLATE_PROVIDER=STUB`
(dev/e2e): перевод детерминирован и не требует ни сети, ни ключей — по нему
проверяются сквозные сценарии компоуза. Глоссарий из задания уважается: термин
из него переводится как задано, остальное получает префикс.
"""

from __future__ import annotations

from dataclasses import dataclass

PREFIX = "переведено: "


@dataclass
class StubTranslator:
    """Детерминированный «перевод»: префикс + оригинал (или термин глоссария)."""

    prefix: str = PREFIX

    async def translate(
        self,
        text: str,
        *,
        source: str | None = None,
        target: str = "ru",
        glossary: dict[str, str] | None = None,
    ) -> str:
        term = (glossary or {}).get(text.strip())
        if term:
            return term
        return f"{self.prefix}{text}"
