"""LLM-переводчик: страница уходит в модель пачками, а не одним запросом.

Плотная страница комикса даёт десятки строк (озвучка, подписи, леттеринг), и
один запрос на всю страницу растягивается на минуты: локальная модель на CPU
выдаёт ~4500 токенов за 200 с, что выходит за таймаут задания. Пачка
ограничивает и время ответа, и цену сбоя — не переводится заново вся страница.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest

from translation_worker.providers.base import TranslationError
from translation_worker.providers.llm import (
    _BLOCKS_PER_REQUEST,
    _TOKENS_FRAME,
    _TOKENS_PER_BLOCK,
    _UnsupportedFormatError,
    LlmTranslator,
)


def _ceiling(blocks: int) -> int:
    """Ожидаемый потолок ответа: рамка плюс запас на каждую реплику."""
    return _TOKENS_FRAME + _TOKENS_PER_BLOCK * blocks


class _Recorder:
    """Подменяет HTTP-слой: запоминает тела запросов и отдаёт перевод по id."""

    def __init__(self) -> None:
        self.bodies: list[dict[str, Any]] = []

    async def __call__(self, url: str, headers: dict, body: dict) -> str:
        self.bodies.append(body)
        user = json.loads(body["messages"][-1]["content"])
        # эхо входного текста: id внутри пачки локальный, а текст — глобальный,
        # поэтому склейка пачек проверяется по порядку реплик
        blocks = [{"id": item["id"], "text": f"[{item['text']}]"} for item in user["blocks"]]
        return json.dumps({"blocks": blocks}, ensure_ascii=False)


def test_dense_page_is_translated_in_batches() -> None:
    """32 реплики воркер отправляет двумя пачками, порядок сохраняется."""
    recorded = _Recorder()
    translator = LlmTranslator(base_url="http://llm/v1", model="m")
    translator._post = recorded  # type: ignore[method-assign]

    texts = [f"line {index}" for index in range(_BLOCKS_PER_REQUEST * 2)]
    translated = asyncio.run(translator.translate_page(texts, source="en", target="ru"))

    assert translated == [f"[{text}]" for text in texts]
    assert len(recorded.bodies) == 2
    for body in recorded.bodies:
        sent = json.loads(body["messages"][-1]["content"])["blocks"]
        assert len(sent) <= _BLOCKS_PER_REQUEST
        # потолок растёт вместе с пачкой: без него модель генерирует до таймаута
        assert body["max_tokens"] == _ceiling(len(sent))


def test_short_page_still_takes_one_request() -> None:
    """Обычная страница не должна дробиться: лишние запросы — лишние минуты."""
    recorded = _Recorder()
    translator = LlmTranslator(base_url="http://llm/v1", model="m")
    translator._post = recorded  # type: ignore[method-assign]

    translated = asyncio.run(translator.translate_page(["a", "b", "c"], source="en", target="ru"))

    assert translated == ["[a]", "[b]", "[c]"]
    assert len(recorded.bodies) == 1
    assert recorded.bodies[0]["max_tokens"] == _ceiling(3)


def test_single_line_translate_uses_one_request() -> None:
    """Одиночный `translate` идёт тем же путём: одна пачка, один потолок."""
    recorded = _Recorder()
    translator = LlmTranslator(base_url="http://llm/v1", model="m")
    translator._post = recorded  # type: ignore[method-assign]

    translated = asyncio.run(translator.translate("hello", source="en", target="ru"))

    assert translated == "[hello]"
    assert len(recorded.bodies) == 1
    assert recorded.bodies[0]["max_tokens"] == _ceiling(1)


def test_glossary_reaches_the_system_prompt() -> None:
    """Глоссарий — часть системного промпта, а не только первого запроса."""
    recorded = _Recorder()
    translator = LlmTranslator(base_url="http://llm/v1", model="m")
    translator._post = recorded  # type: ignore[method-assign]

    asyncio.run(translator.translate_page(["x"], source="en", target="ru",
                                          glossary={"Wolverine": "Росомаха"}))

    system = recorded.bodies[0]["messages"][0]["content"]
    assert "Росомаха" in system and "Wolverine" in system


class _WrongIdsOnce:
    """Первый ответ теряет реплику — тогда и только тогда нужен repair-запрос."""

    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self, url: str, headers: dict, body: dict) -> str:
        self.calls += 1
        sent = json.loads(body["messages"][-1]["content"])["blocks"]
        if self.calls == 1:
            sent = sent[:-1]
        return json.dumps(
            {"blocks": [{"id": item["id"], "text": f"[{item['text']}]"} for item in sent]},
            ensure_ascii=False,
        )


def test_incomplete_answer_triggers_exactly_one_repair() -> None:
    """Неполный набор id лечится repair-запросом, а не дроблением страницы."""
    recorded = _WrongIdsOnce()
    translator = LlmTranslator(base_url="http://llm/v1", model="m")
    translator._post = recorded  # type: ignore[method-assign]

    translated = asyncio.run(translator.translate_page(["a", "b"], source="en", target="ru"))

    assert translated == ["[a]", "[b]"]
    assert recorded.calls == 2


class _RejectsStructuredOutput:
    """Провайдер без `response_format`: первый запрос — отказ, второй без него."""

    def __init__(self) -> None:
        self.bodies: list[dict[str, Any]] = []

    async def __call__(self, url: str, headers: dict, body: dict) -> str:
        self.bodies.append(body)
        if "response_format" in body:
            raise _UnsupportedFormatError("response_format не поддерживается")
        sent = json.loads(body["messages"][-1]["content"])["blocks"]
        return json.dumps(
            {"blocks": [{"id": item["id"], "text": f"[{item['text']}]"} for item in sent]},
            ensure_ascii=False,
        )


def test_unsupported_response_format_retries_without_it_and_keeps_ceiling() -> None:
    """Повтор без structured output сохраняет потолок ответа."""
    recorded = _RejectsStructuredOutput()
    translator = LlmTranslator(base_url="http://llm/v1", model="m")
    translator._post = recorded  # type: ignore[method-assign]

    translated = asyncio.run(translator.translate_page(["a", "b"], source="en", target="ru"))

    assert translated == ["[a]", "[b]"]
    assert len(recorded.bodies) == 2
    assert "response_format" not in recorded.bodies[1]
    assert recorded.bodies[1]["max_tokens"] == _ceiling(2)


class _FlakyRecorder:
    """Модель отвечает пустым JSON на большие пачки и нормально — на мелкие.

    Именно так ведёт себя локальная модель на `response_format=json_object`:
    на длинном ответе стрим обрывается по потолку токенов, и разобрать нечего.
    """

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.sizes: list[int] = []

    async def __call__(self, url: str, headers: dict, body: dict) -> str:
        sent = json.loads(body["messages"][-1]["content"])["blocks"]
        self.sizes.append(len(sent))
        if len(sent) > self.limit:
            return json.dumps({"blocks": []})
        return json.dumps(
            {"blocks": [{"id": item["id"], "text": f"[{item['text']}]"} for item in sent]},
            ensure_ascii=False,
        )


def test_empty_answer_falls_back_to_halved_batches() -> None:
    """Пустой ответ на большой пачке — не приговор странице: режем пачку пополам."""
    recorded = _FlakyRecorder(limit=4)
    translator = LlmTranslator(base_url="http://llm/v1", model="m")
    translator._post = recorded  # type: ignore[method-assign]

    texts = [f"line {index}" for index in range(8)]
    translated = asyncio.run(translator.translate_page(texts, source="en", target="ru"))

    assert translated == [f"[{text}]" for text in texts]
    # на пустой ответ дробим сразу, без repair-запроса: бюджет времени дороже
    assert recorded.sizes == [8, 4, 4]


def test_single_block_failure_still_fails_the_page() -> None:
    """Одна реплика и пустой ответ — честная ошибка, а не бесконечное дробление."""
    recorded = _FlakyRecorder(limit=0)
    translator = LlmTranslator(base_url="http://llm/v1", model="m")
    translator._post = recorded  # type: ignore[method-assign]

    with pytest.raises(TranslationError):
        asyncio.run(translator.translate_page(["only line"], source="en", target="ru"))
    assert recorded.sizes == [1]


class _AlwaysDown:
    """Транспортная ошибка: провайдер недоступен, ответа не будет вообще."""

    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self, url: str, headers: dict, body: dict) -> str:
        self.calls += 1
        raise TranslationError("LLM недоступен: ConnectError")


def test_transport_failure_is_not_halved() -> None:
    """Недоступный провайдер не дробится: половины снова ждали бы полный таймаут."""
    recorded = _AlwaysDown()
    translator = LlmTranslator(base_url="http://llm/v1", model="m")
    translator._post = recorded  # type: ignore[method-assign]

    texts = [f"line {index}" for index in range(_BLOCKS_PER_REQUEST)]
    with pytest.raises(TranslationError):
        asyncio.run(translator.translate_page(texts, source="en", target="ru"))
    assert recorded.calls == 1


class _FailsOnOneLine:
    """Модель отвечает пусто на пачку, где встречается конкретная реплика страницы."""

    def __init__(self, needle: str) -> None:
        self.needle = needle
        self.sizes: list[int] = []

    async def __call__(self, url: str, headers: dict, body: dict) -> str:
        sent = json.loads(body["messages"][-1]["content"])["blocks"]
        self.sizes.append(len(sent))
        if any(item["text"] == self.needle for item in sent):
            return json.dumps({"blocks": []})
        return json.dumps(
            {"blocks": [{"id": item["id"], "text": f"[{item['text']}]"} for item in sent]},
            ensure_ascii=False,
        )


def test_error_names_the_page_line_from_a_later_batch() -> None:
    """Номер реплики в ошибке — страничный, а не локальный внутри пачки.

    Без проброса `offset` из `translate_page` вторая пачка страницы из сорока
    реплик сообщала бы про реплику 4 вместо реплики 20, и оператор искал бы
    строку не там.
    """
    recorded = _FailsOnOneLine(needle="line 19")
    translator = LlmTranslator(base_url="http://llm/v1", model="m")
    translator._post = recorded  # type: ignore[method-assign]

    texts = [f"line {index}" for index in range(_BLOCKS_PER_REQUEST * 2 + 8)]
    with pytest.raises(TranslationError) as caught:
        asyncio.run(translator.translate_page(texts, source="en", target="ru"))

    assert "20" in str(caught.value)


class _AddsSurplusId:
    """Модель возвращает все попрошенные id плюс один лишний."""

    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self, url: str, headers: dict, body: dict) -> str:
        self.calls += 1
        sent = json.loads(body["messages"][-1]["content"])["blocks"]
        blocks = [{"id": item["id"], "text": f"[{item['text']}]"} for item in sent]
        blocks.append({"id": "99", "text": "лишнее"})
        return json.dumps({"blocks": blocks}, ensure_ascii=False)


def test_surplus_id_is_dropped_without_repair_or_halving() -> None:
    """Лишний id отбрасывается: страница переведена, лишних запросов нет."""
    recorded = _AddsSurplusId()
    translator = LlmTranslator(base_url="http://llm/v1", model="m")
    translator._post = recorded  # type: ignore[method-assign]

    translated = asyncio.run(translator.translate_page(["a", "b"], source="en", target="ru"))

    assert translated == ["[a]", "[b]"]
    assert recorded.calls == 1


def test_ceiling_fits_the_model_context() -> None:
    """Потолок самой большой пачки остаётся в пределах контекста модели.

    Промпт плотной страницы — около полутора тысяч токенов, поэтому потолок
    ответа обязан быть заметно меньше окна: иначе провайдер ответит 400 на
    размер запроса, а не на содержимое.
    """
    assert _TOKENS_FRAME + _TOKENS_PER_BLOCK * _BLOCKS_PER_REQUEST <= 3072


def _patch_transport(monkeypatch, handler) -> None:
    """Подменить httpx-клиент мок-транспортом: проверяется настоящий `_post`."""
    transport = httpx.MockTransport(handler)
    real_client = httpx.AsyncClient

    def factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        kwargs["transport"] = transport
        return real_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", factory)


def _blocks_answer(payload: dict[str, Any]) -> httpx.Response:
    sent = json.loads(payload["messages"][-1]["content"])["blocks"]
    blocks = [{"id": item["id"], "text": f"[{item['text']}]"} for item in sent]
    return httpx.Response(
        200, json={"choices": [{"message": {"content": json.dumps({"blocks": blocks})}}]}
    )


def test_format_rejection_body_triggers_retry_without_response_format(monkeypatch) -> None:
    """Тело 400 про `response_format` — реальный отказ формата: повтор без него."""
    seen: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        seen.append(body)
        if "response_format" in body:
            return httpx.Response(400, json={"error": "response_format is not supported"})
        return _blocks_answer(body)

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(base_url="http://llm.test/v1", model="m")

    translated = asyncio.run(translator.translate_page(["a"], source="en", target="ru"))

    assert translated == ["[a]"]
    assert len(seen) == 2
    assert "response_format" in seen[0]
    assert "response_format" not in seen[1]
    assert seen[1]["max_tokens"] == _ceiling(1)


def test_context_overflow_is_not_mistaken_for_format_rejection(monkeypatch) -> None:
    """400 про размер промпта — не повод терять structured output на весь прогон."""
    seen: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        seen.append(body)
        # провайдеры часто эхом возвращают тело запроса вместе с ошибкой
        return httpx.Response(
            400,
            json={
                "error": "the request exceeds the context length",
                "request": {"response_format": body.get("response_format")},
            },
        )

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(base_url="http://llm.test/v1", model="m")

    with pytest.raises(TranslationError):
        asyncio.run(translator.translate_page(["a"], source="en", target="ru"))
    # ретрай по настройке (retries=1) — два запроса, и ни одного без формата
    assert len(seen) == 2
    assert all("response_format" in body for body in seen)