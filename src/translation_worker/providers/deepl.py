"""Перевод через DeepL API (карточка 55 §2.11).

Реплики страницы уходят одним запросом (`text` — массив), `target_lang` — в
верхнем регистре, как требует API. Ошибки провайдера → `TRANSLATION_FAILED`.
Глоссарий DeepL требует отдельного glossary-ресурса — он появится в фазе 3,
поэтому здесь термин подставляется только через LLM-провайдер.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from translation_worker.providers.base import TranslationError

DEEPL_FREE_URL = "https://api-free.deepl.com"


@dataclass
class DeepLTranslator:
    """DeepL: пакетный перевод страницы + одиночный `translate`."""

    api_key: str
    base_url: str = DEEPL_FREE_URL
    timeout: float = 60.0

    async def translate(
        self,
        text: str,
        *,
        source: str | None = None,
        target: str = "ru",
        glossary: dict[str, str] | None = None,
    ) -> str:
        translated = await self.translate_page(
            [text], source=source, target=target, glossary=glossary
        )
        return translated[0]

    async def translate_page(
        self,
        texts: list[str],
        *,
        source: str | None = None,
        target: str = "ru",
        glossary: dict[str, str] | None = None,
    ) -> list[str]:
        if not texts:
            return []
        body: dict[str, Any] = {
            "text": list(texts),
            "target_lang": target.upper(),
        }
        if source:
            body["source_lang"] = source.upper()
        headers = {"Authorization": f"DeepL-Auth-Key {self.api_key}"}
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.base_url.rstrip('/')}/v2/translate", json=body, headers=headers
                )
        except httpx.HTTPError as exc:
            raise TranslationError(f"DeepL недоступен: {exc}") from exc
        if not 200 <= response.status_code < 300:
            raise TranslationError(f"DeepL ответил HTTP {response.status_code}")
        return _parse_translations(response.json(), expected=len(texts))


def _parse_translations(payload: Any, *, expected: int) -> list[str]:
    items = payload.get("translations") if isinstance(payload, dict) else None
    if not isinstance(items, list) or len(items) != expected:
        raise TranslationError("DeepL вернул неожиданное число переводов")
    translations: list[str] = []
    for item in items:
        if not isinstance(item, dict) or item.get("text") is None:
            raise TranslationError("DeepL вернул перевод без текста")
        translations.append(str(item["text"]))
    return translations
