"""Повторы запроса к LLM: сколько их, с какой паузой и что видно в ошибке.

Стенд поймал HTTP 500 от ollama посреди книги: два запроса подряд и
`TRANSLATION_FAILED` на странице, хотя следующий такой же запрос проходит.
Значит отказ временный, и политика повторов — часть работоспособности: число
попыток настраивается, между ними есть пауза, а в тексте ошибки видно тело
ответа провайдера, иначе причина 500 неизвестна.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest

from translation_worker.config import Settings
from translation_worker.providers import build_translator
from translation_worker.providers.base import TranslationError
from translation_worker.providers.llm import LlmTranslator


def _patch_transport(monkeypatch, handler) -> None:
    transport = httpx.MockTransport(handler)
    real_client = httpx.AsyncClient

    def factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        kwargs["transport"] = transport
        return real_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", factory)


def _answer(payload: dict[str, Any]) -> httpx.Response:
    sent = json.loads(payload["messages"][-1]["content"])["blocks"]
    blocks = [{"id": item["id"], "text": item["text"]} for item in sent]
    return httpx.Response(
        200, json={"message": {"content": json.dumps({"blocks": blocks})}}
    )


def test_temporary_error_is_retried_until_success(monkeypatch) -> None:
    """Первый ответ 500 — повтор: временный отказ провайдера не валит страницу."""
    calls: list[int] = []
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        if len(calls) < 3:
            return httpx.Response(500, json={"error": "model loading"})
        return _answer(json.loads(request.content))

    _patch_transport(monkeypatch, handler)

    async def no_wait(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", no_wait)
    translator = LlmTranslator(
        base_url="http://llm.test:11434/v1", model="m", api_mode="ollama", retries=2
    )

    translated = asyncio.run(translator.translate_page(["a"], source="en", target="ru"))

    assert translated == ["a"]
    assert len(calls) == 3, "две неудачи и одна успешная попытка"
    assert sleeps, "между попытками нужна пауза, иначе провайдер не успевает ожить"


def test_error_message_carries_provider_body(monkeypatch) -> None:
    """В ошибке видно тело ответа: иначе причина 500 остаётся загадкой."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "model loading"})

    _patch_transport(monkeypatch, handler)

    async def no_wait(seconds: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", no_wait)
    translator = LlmTranslator(
        base_url="http://llm.test:11434/v1", model="m", api_mode="ollama", retries=0
    )

    with pytest.raises(TranslationError) as failure:
        asyncio.run(translator.translate_page(["a"], source="en", target="ru"))

    message = str(failure.value)
    assert "HTTP 500" in message
    assert "model loading" in message, f"тело ответа потерялось: {message}"


def test_retries_are_configured_by_settings() -> None:
    """Число попыток задаётся настройкой, а не живёт константой в провайдере."""
    settings = Settings(
        translate_provider="LLM",
        llm_base_url="http://llm.test/v1",
        llm_model="m",
        llm_retries=3,
    )

    translator = build_translator(settings)

    assert getattr(translator, "retries", None) == 3


def test_successful_answer_is_not_delayed(monkeypatch) -> None:
    """Успешный ответ не ждёт паузы: повтор нужен только после отказа."""
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        return _answer(json.loads(request.content))

    _patch_transport(monkeypatch, handler)

    async def no_wait(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", no_wait)
    translator = LlmTranslator(
        base_url="http://llm.test:11434/v1", model="m", api_mode="ollama", retries=2
    )

    asyncio.run(translator.translate_page(["a"], source="en", target="ru"))

    assert sleeps == [], "пауза перед первым запросом не нужна"


def test_last_attempt_does_not_promise_a_retry(monkeypatch, caplog) -> None:
    """Последняя попытка не пишет «повтор»: повтора уже не будет."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "boom"})

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(base_url="http://llm.test/v1", model="m", retries=1)

    with caplog.at_level("WARNING"), pytest.raises(TranslationError):
        asyncio.run(translator.translate_page(["a"], source="en", target="ru"))

    retries = [record.message for record in caplog.records if "повтор" in record.message]
    assert len(retries) == 1, f"ожидалась одна запись о повторе, было {retries}"
    assert "1/1" in retries[0], f"нумерация повтора неверна: {retries[0]}"

