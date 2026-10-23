"""Перевод через OpenAI-совместимый `chat/completions` (карточка 55 §2.9).

Один запрос на страницу: все блоки уходят в одном промпте (модель видит контекст
диалога), ответ — JSON `{"blocks": [{"id": "...", "text": "..."}]}`. Если
провайдер не поддерживает `response_format=json_object`, тот же запрос
повторяется без него, а JSON вырезается из ответа. Несовпадение набора id →
один repair-запрос, затем `TRANSLATION_FAILED`.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

import httpx

from translation_worker.providers.base import TranslationError

_LOG = logging.getLogger(__name__)

_SYSTEM_PROMPT = (
    "Ты переводишь реплики комикса с языка «{source}» на язык «{target}». "
    "Стиль: короткие живые реплики, без пояснений, сносок и транслитерации. "
    "Глоссарий (переводи термины дословно): {glossary}. "
    "На каждый входной id верни ровно один перевод. "
    'Ответ — только JSON: {{"blocks": [{{"id": "0", "text": "..."}}]}}.'
)


class _UnsupportedFormatError(TranslationError):
    """Провайдер не принимает `response_format` — повторим запрос без него."""


@dataclass
class LlmTranslator:
    """LLM-переводчик: пакетный перевод страницы + одиночный `translate`."""

    base_url: str
    model: str
    api_key: str = ""
    timeout: float = 60.0
    retries: int = 1

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
        """Перевести набор реплик страницы одним запросом (id = индекс блока)."""
        if not texts:
            return []
        system = _SYSTEM_PROMPT.format(
            source=source or "оригинала",
            target=target,
            glossary=_glossary_text(glossary),
        )
        user = json.dumps(
            {"blocks": [{"id": str(index), "text": text} for index, text in enumerate(texts)]},
            ensure_ascii=False,
        )
        expected = {str(index) for index in range(len(texts))}
        mapped = _parse_blocks(await self._chat(system, user))
        if set(mapped) != expected:
            _LOG.warning("LLM вернул не тот набор id (%s) — repair-запрос", sorted(mapped))
            repair = (
                f"{system} В ответе были id {sorted(mapped)}; нужны ровно id "
                f"{', '.join(sorted(expected))}."
            )
            mapped = _parse_blocks(await self._chat(repair, user))
        if set(mapped) != expected:
            missing = ", ".join(sorted(expected - set(mapped))) or "нет"
            raise TranslationError(f"LLM не перевёл блоки с id: {missing}")
        return [mapped[str(index)] for index in range(len(texts))]

    async def _chat(self, system: str, user: str) -> str:
        url = f"{self.base_url.rstrip('/')}/chat/completions"
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        body: dict[str, Any] = {
            "model": self.model,
            "temperature": 0,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        }
        try:
            return await self._post(url, headers, body)
        except _UnsupportedFormatError:
            _LOG.info("провайдер без response_format: повторяем без structured output")
            body.pop("response_format", None)
            return await self._post(url, headers, body)

    async def _post(self, url: str, headers: dict[str, str], body: dict[str, Any]) -> str:
        last_error: TranslationError | None = None
        for _attempt in range(self.retries + 1):
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.post(url, json=body, headers=headers)
            except httpx.HTTPError as exc:
                last_error = TranslationError(f"LLM недоступен: {exc}")
                continue
            if response.status_code in (400, 422) and "response_format" in body:
                raise _UnsupportedFormatError("response_format не поддерживается")
            if not 200 <= response.status_code < 300:
                last_error = TranslationError(f"LLM ответил HTTP {response.status_code}")
                continue
            try:
                return _answer_text(response.json())
            except ValueError as exc:
                last_error = TranslationError(f"LLM: некорректный ответ: {exc}")
        raise last_error or TranslationError("LLM: пустой ответ")


def _answer_text(payload: Any) -> str:
    try:
        return str(payload["choices"][0]["message"]["content"])
    except (KeyError, IndexError, TypeError) as exc:
        raise ValueError(f"нет choices[0].message.content: {exc}") from exc


def _parse_blocks(answer: str) -> dict[str, str]:
    document = _first_json_object(answer)
    if not isinstance(document, dict):
        return {}
    raw_blocks = document.get("blocks")
    if not isinstance(raw_blocks, list):
        return {}
    mapped: dict[str, str] = {}
    for item in raw_blocks:
        if not isinstance(item, dict):
            continue
        identifier = item.get("id")
        text = item.get("text")
        if identifier is None or text is None:
            continue
        mapped[str(identifier)] = " ".join(str(text).split())
    return mapped


def _first_json_object(answer: str) -> Any:
    text = answer.strip()
    try:
        return json.loads(text)
    except ValueError:
        pass
    start = text.find("{")
    if start < 0:
        return None
    try:
        document, _ = json.JSONDecoder().raw_decode(text[start:])
    except ValueError:
        return None
    return document


def _glossary_text(glossary: dict[str, str] | None) -> str:
    if not glossary:
        return "нет"
    return "; ".join(f"{term} → {translation}" for term, translation in glossary.items())
