"""Режим API модели: OpenAI-совместимый слой (`openai`) и родной ollama (`ollama`).

`huihui_ai/qwen3.5-abliterated:9b` сначала «думает»: в OpenAI-слое размышления
уходят в поле `reasoning` и съедают весь потолок ответа, а `content` приходит
пустым. На родном эндпоинте `think=false` отключает размышления и модель отдаёт
чистый JSON. Поэтому у провайдера два режима, и проверяется каждый: адрес
запроса, тело (потолок ответа, флаг `think`, `format=json`) и разбор ответа.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest

from translation_worker.config import Settings
from translation_worker.providers.base import TranslationError
from translation_worker.providers.llm import LlmTranslator


def _patch_transport(monkeypatch, handler) -> None:
    """Подменить httpx-клиент мок-транспортом: проверяется настоящий `_chat`."""
    transport = httpx.MockTransport(handler)
    real_client = httpx.AsyncClient

    def factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        kwargs["transport"] = transport
        return real_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", factory)


def _native_answer(payload: dict[str, Any]) -> httpx.Response:
    """Ответ родного эндпоинта: JSON лежит в `message.content`."""
    sent = json.loads(payload["messages"][-1]["content"])["blocks"]
    blocks = [{"id": item["id"], "text": f"[{item['text']}]"} for item in sent]
    return httpx.Response(
        200,
        json={
            "message": {"role": "assistant", "content": json.dumps({"blocks": blocks})},
            "done": True,
            "eval_count": 42,
        },
    )


def _openai_answer(payload: dict[str, Any]) -> httpx.Response:
    sent = json.loads(payload["messages"][-1]["content"])["blocks"]
    blocks = [{"id": item["id"], "text": f"[{item['text']}]"} for item in sent]
    return httpx.Response(
        200, json={"choices": [{"message": {"content": json.dumps({"blocks": blocks})}}]}
    )


def test_ollama_mode_posts_to_native_endpoint(monkeypatch) -> None:
    """`api_mode=ollama`: родной `/api/chat`, `think=false`, `format=json`."""
    seen: list[tuple[str, dict[str, Any]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        seen.append((str(request.url), body))
        return _native_answer(body)

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(
        base_url="http://llm.test:11434/v1", model="m", api_mode="ollama"
    )

    translated = asyncio.run(translator.translate_page(["a", "b"], source="en", target="ru"))

    assert translated == ["[a]", "[b]"]
    url, body = seen[0]
    assert url == "http://llm.test:11434/api/chat"
    assert body["think"] is False, "без think=false модель тратит бюджет на размышления"
    assert body["format"] == "json"
    assert body["stream"] is False
    assert body["options"]["num_predict"] > 0, "потолок ответа обязан быть задан"
    assert "response_format" not in body and "max_tokens" not in body


def test_openai_mode_keeps_the_layer_contract(monkeypatch) -> None:
    """По умолчанию всё как раньше: OpenAI-слой, `response_format`, `max_tokens`."""
    seen: list[tuple[str, dict[str, Any]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        seen.append((str(request.url), body))
        return _openai_answer(body)

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(base_url="http://llm.test/v1", model="m")

    translated = asyncio.run(translator.translate_page(["a"], source="en", target="ru"))

    assert translated == ["[a]"]
    url, body = seen[0]
    assert url == "http://llm.test/v1/chat/completions"
    assert body["response_format"] == {"type": "json_object"}
    assert body["max_tokens"] > 0
    assert "think" not in body


def test_native_url_is_derived_from_base_without_v1(monkeypatch) -> None:
    """Адрес без `/v1` тоже даёт родной эндпоинт, а не битый путь."""
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        seen.append(str(request.url))
        return _native_answer(body)

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(base_url="http://llm.test:11434", model="m", api_mode="ollama")

    asyncio.run(translator.translate_page(["a"], source="en", target="ru"))

    assert seen == ["http://llm.test:11434/api/chat"]


def test_native_empty_answer_is_halved_not_fatal(monkeypatch) -> None:
    """Пустой `content` в родном режиме — та же пачка половинами, а не отказ."""
    sizes: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        sent = json.loads(body["messages"][-1]["content"])["blocks"]
        sizes.append(len(sent))
        if len(sent) > 1:
            return httpx.Response(200, json={"message": {"content": ""}})
        return _native_answer(body)

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(base_url="http://llm.test:11434/v1", model="m", api_mode="ollama")

    translated = asyncio.run(translator.translate_page(["a", "b"], source="en", target="ru"))

    assert translated == ["[a]", "[b]"]
    assert sizes == [2, 1, 1], "пачка из двух должна развалиться на две одиночные"


def test_native_http_error_is_transport_failure(monkeypatch) -> None:
    """HTTP 500 в родном режиме — транспортная ошибка без дробления пачки."""
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(500, json={"error": "boom"})

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(
        base_url="http://llm.test:11434/v1", model="m", api_mode="ollama", retries=0
    )

    with pytest.raises(TranslationError) as failure:
        asyncio.run(translator.translate_page(["a", "b"], source="en", target="ru"))

    assert "HTTP 500" in str(failure.value)
    assert len(calls) == 1, "транспортный отказ не дробится на полузапросы"


def test_config_rejects_unknown_api_mode() -> None:
    """Опечатка в режиме API не должна молча уводить перевод в другой протокол."""
    with pytest.raises(ValueError) as failure:
        Settings(
            translate_provider="LLM",
            llm_base_url="http://llm.test/v1",
            llm_model="m",
            llm_api_mode="grpc",
        )
    message = str(failure.value)
    assert "llm_api_mode" in message, f"ошибка не называет настройку: {message}"
    assert "ollama" in message, f"ошибка не перечисляет допустимые режимы: {message}"
