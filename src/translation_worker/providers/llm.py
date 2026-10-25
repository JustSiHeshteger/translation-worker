"""Перевод через OpenAI-совместимый `chat/completions` (карточка 55 §2.9).

Страница уходит в модель пачками по `_BLOCKS_PER_REQUEST` реплик: плотная
страница комикса — это десятки строк, и один запрос на всю страницу тянется
минуты. Ответ — JSON `{"blocks": [{"id": "...", "text": "..."}]}`, у каждого
запроса есть потолок `max_tokens`. Если провайдер не поддерживает
`response_format=json_object`, тот же запрос повторяется без него, а JSON
вырезается из ответа. Неполный набор id → один repair-запрос; пустой ответ или
не помогший repair → та же пачка половинами: сбой одной пачки не губит страницу
и книгу.
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

#: Сколько реплик уходит в модель за один запрос. Плотная страница комикса даёт
#: десятки строк (диалоги, подписи, звуки), и один запрос на всю страницу — это
#: минуты генерации: локальная модель на CPU выдала 4457 токенов за 203 с, что
#: выходит за таймаут задания. Пачка ограничивает время ответа, а сбой одного
#: запроса не переводит заново всю страницу.
_BLOCKS_PER_REQUEST = 16

#: Потолок ответа модели: с запасом на реплику плюс служебная рамка JSON.
#: Без него генерация упирается в таймаут, а не в собственный предел.
#: Мера — по замеру: плотная страница отдаёт ~63 токена на реплику, а обрыв
#: стрима на потолке в режиме `json_object` возвращает пустой ответ. Запас
#: двойной: больше — и промпт с потолком перестают влезать в контекст модели.
_TOKENS_PER_BLOCK = 128
_TOKENS_FRAME = 384


class _UnsupportedFormatError(TranslationError):
    """Провайдер не принимает `response_format` — повторим запрос без него."""


class _ContentFailure(TranslationError):
    """Модель ответила пусто или не тем набором id — это лечится дроблением.

    Транспортные и постоянные ошибки (`LLM недоступен`, HTTP 5xx) сюда не
    попадают: делить пачку на них бессмысленно и дорого — каждый полузапрос
    снова ждёт полный таймаут.
    """


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
        """Перевести реплики страницы пачками по `_BLOCKS_PER_REQUEST` штук.

        Порядок результата совпадает с порядком входа: id внутри запроса —
        индекс реплики в пачке, а пачки склеиваются по порядку. Номер первой
        реплики пачки передаётся как `offset`: иначе ошибка из второй пачки
        укажет на первую строку страницы вместо настоящей.
        """
        translated: list[str] = []
        for start in range(0, len(texts), _BLOCKS_PER_REQUEST):
            chunk = texts[start:start + _BLOCKS_PER_REQUEST]
            translated.extend(
                await self._translate_batch(
                    chunk,
                    source=source,
                    target=target,
                    glossary=glossary,
                    offset=start,
                )
            )
        return translated

    async def _translate_batch(
        self,
        texts: list[str],
        *,
        source: str | None,
        target: str,
        glossary: dict[str, str] | None,
        offset: int = 0,
    ) -> list[str]:
        """Перевести пачку; на пустом/битом ответе — та же пачка половинами.

        Локальная модель в режиме `json_object` на длинном ответе обрывает стрим
        и возвращает пустоту: один такой ответ не должен губить страницу, а тем
        более всю книгу. Дробление сходится за log2(размера) шагов, а вырожденный
        случай (одна реплика) остаётся честной ошибкой. Транспортные ошибки и
        HTTP-отказы провайдера не дробятся: половины снова ждали бы полный
        таймаут, и задание сгорало бы по общему бюджету времени.
        """
        if not texts:
            return []
        try:
            return await self._translate_chunk(
                texts, source=source, target=target, glossary=glossary, offset=offset
            )
        except _ContentFailure:
            if len(texts) == 1:
                raise
            middle = len(texts) // 2
            _LOG.warning(
                "LLM не перевёл пачку из %d реплик (с %d) — повторяю половинами",
                len(texts),
                offset,
            )
            head = await self._translate_batch(
                texts[:middle],
                source=source,
                target=target,
                glossary=glossary,
                offset=offset,
            )
            tail = await self._translate_batch(
                texts[middle:],
                source=source,
                target=target,
                glossary=glossary,
                offset=offset + middle,
            )
            return head + tail

    async def _translate_chunk(
        self,
        texts: list[str],
        *,
        source: str | None,
        target: str,
        glossary: dict[str, str] | None,
        offset: int = 0,
    ) -> list[str]:
        """Один запрос на пачку (id = индекс блока внутри пачки).

        `offset` — номер первой реплики в пределах страницы: он попадает в
        сообщение об ошибке, иначе оператор видит локальный id и не может
        найти строку на странице из семидесяти.
        """
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
        mapped = _parse_blocks(await self._chat(system, user, blocks=len(texts)))
        # Пустой ответ — обрыв стрима на потолке: repair-запрос вернёт то же самое,
        # а дробить надо сразу, пока не истрачен бюджет времени.
        if not mapped:
            raise _ContentFailure(
                f"LLM вернул пустой ответ на реплики {_span(offset, len(texts))}"
            )
        mapped = _without_surplus(mapped, expected)
        if set(mapped) != expected:
            _LOG.warning("LLM вернул не тот набор id (%s) — repair-запрос", sorted(mapped))
            repair = (
                f"{system} В ответе были id {sorted(mapped)}; нужны ровно id "
                f"{', '.join(sorted(expected))}."
            )
            mapped = _without_surplus(
                _parse_blocks(await self._chat(repair, user, blocks=len(texts))), expected
            )
        missing = sorted(expected - set(mapped))
        if missing:
            raise _ContentFailure(
                f"LLM не перевёл реплики {_span(offset, len(texts))} "
                f"(без перевода: {', '.join(missing)})"
            )
        return [mapped[str(index)] for index in range(len(texts))]

    async def _chat(self, system: str, user: str, *, blocks: int) -> str:
        url = f"{self.base_url.rstrip('/')}/chat/completions"
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        body: dict[str, Any] = {
            "model": self.model,
            "temperature": 0,
            "response_format": {"type": "json_object"},
            # потолок ответа: без него локальная модель генерирует до таймаута
            "max_tokens": _TOKENS_FRAME + _TOKENS_PER_BLOCK * blocks,
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
                # 400 приходит и от переполнения контекста: отличаем по телу
                # ответа, иначе молча теряем structured output на целом прогоне
                if _rejects_response_format(response.text):
                    raise _UnsupportedFormatError("response_format не поддерживается")
            if not 200 <= response.status_code < 300:
                last_error = TranslationError(f"LLM ответил HTTP {response.status_code}")
                continue
            try:
                return _answer_text(response.json())
            except ValueError as exc:
                last_error = TranslationError(f"LLM: некорректный ответ: {exc}")
        raise last_error or TranslationError("LLM: пустой ответ")


def _span(offset: int, count: int) -> str:
    """Диапазон номеров реплик страницы для сообщений об ошибке."""
    first = offset + 1
    last = offset + count
    return f"{first}" if first == last else f"{first}–{last}"


#: Признаки отказа именно из-за `response_format` в теле 400/422.
_FORMAT_REJECTIONS = ("not supported", "unsupported", "unknown field", "invalid")

#: Признаки того, что 400 вызван размером запроса, а не structured output:
#: такие провайдеры часто эхом возвращают тело запроса, и там есть
#: `response_format` — повтор без него ничего не изменит.
_CONTEXT_REJECTIONS = ("context", "num_ctx", "too long", "length", "token")


def _rejects_response_format(body: str) -> bool:
    """Отказ провайдера относится к `response_format`, а не к размеру промпта."""
    text = body.lower()
    if "response_format" not in text:
        return False
    if any(marker in text for marker in _CONTEXT_REJECTIONS):
        return False
    return any(marker in text for marker in _FORMAT_REJECTIONS)


def _without_surplus(mapped: dict[str, str], expected: set[str]) -> dict[str, str]:
    """Отбросить id, которых не просили: лишний id — не повод переводить заново."""
    surplus = set(mapped) - expected
    if surplus:
        _LOG.info("LLM вернул лишние id (%s) — отбрасываю", sorted(surplus))
        return {key: value for key, value in mapped.items() if key in expected}
    return mapped


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
