"""Пустой перевод реплики: это промах, а не «переведено в пустоту».

Замер на странице из 68 реплик показал, что модель возвращает часть id с пустым
текстом. Если принять такую пару за перевод, вёрстка молча выбросит реплику, и
читатель увидит страницу с пропавшим балуном. Поэтому пустой текст считается
отсутствующим id: сначала repair-запрос, затем дробление пачки.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest

from translation_worker.providers.base import TranslationError
from translation_worker.providers.llm import LlmTranslator


def _patch_transport(monkeypatch, handler) -> None:
    transport = httpx.MockTransport(handler)
    real_client = httpx.AsyncClient

    def factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        kwargs["transport"] = transport
        return real_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", factory)


def _blocks(texts: list[str]) -> dict[str, Any]:
    return {"blocks": [{"id": str(index), "text": text} for index, text in enumerate(texts)]}


def test_blank_translation_triggers_repair(monkeypatch) -> None:
    """Пустой текст одной реплики — повод попросить перевод заново, а не терять её."""
    requests: list[list[str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        sent = json.loads(payload["messages"][-1]["content"])["blocks"]
        requests.append([item["id"] for item in sent])
        if len(requests) == 1:
            # вторая реплика «переведена» пустой строкой
            body = {"blocks": [{"id": "0", "text": "[a]"}, {"id": "1", "text": "  "}]}
        else:
            body = _blocks([f"[{item['text']}]" for item in sent])
        return httpx.Response(
            200, json={"choices": [{"message": {"content": json.dumps(body)}}]}
        )

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(base_url="http://llm.test/v1", model="m")

    translated = asyncio.run(translator.translate_page(["a", "b"], source="en", target="ru"))

    assert translated == ["[a]", "[b]"], "пустой перевод обязан быть заменён настоящим"
    assert len(requests) == 2, "нужен ровно один repair-запрос"


def test_persistent_blank_translation_is_halved(monkeypatch) -> None:
    """Если после repair пустышка осталась — пачка дробится половинами."""
    sizes: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        sent = json.loads(payload["messages"][-1]["content"])["blocks"]
        sizes.append(len(sent))
        if len(sent) > 1:
            # одна реплика пустая всегда: repair не помогает
            body = {
                "blocks": [
                    {"id": item["id"], "text": "" if item["id"] == "1" else f"[{item['text']}]"}
                    for item in sent
                ]
            }
        else:
            body = _blocks([f"[{item['text']}]" for item in sent])
        return httpx.Response(
            200, json={"choices": [{"message": {"content": json.dumps(body)}}]}
        )

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(base_url="http://llm.test/v1", model="m")

    translated = asyncio.run(translator.translate_page(["a", "b"], source="en", target="ru"))

    assert translated == ["[a]", "[b]"]
    assert sizes[0] == 2 and 1 in sizes[1:], f"ожидалось дробление, было {sizes}"


def test_all_blank_answer_is_a_content_failure(monkeypatch) -> None:
    """Ответ целиком из пустых строк — пустой ответ: дробим сразу, без repair."""
    requests: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        sent = json.loads(payload["messages"][-1]["content"])["blocks"]
        requests.append(len(sent))
        if len(sent) > 1:
            body = {"blocks": [{"id": item["id"], "text": ""} for item in sent]}
        else:
            body = _blocks([f"[{item['text']}]" for item in sent])
        return httpx.Response(
            200, json={"choices": [{"message": {"content": json.dumps(body)}}]}
        )

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(base_url="http://llm.test/v1", model="m")

    translated = asyncio.run(translator.translate_page(["a", "b"], source="en", target="ru"))

    assert translated == ["[a]", "[b]"]
    assert requests == [2, 1, 1], "пустой ответ дробится без repair-запроса"


def test_retry_is_logged(monkeypatch, caplog) -> None:
    """Повтор обязан быть виден в логе: иначе не отличить флаки от чистой работы."""
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        if len(calls) == 1:
            return httpx.Response(500, json={"error": "model loading"})
        return httpx.Response(
            200, json={"choices": [{"message": {"content": json.dumps(_blocks(["[a]"]))}}]}
        )

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(base_url="http://llm.test/v1", model="m", retries=2)

    with caplog.at_level("WARNING"):
        asyncio.run(translator.translate_page(["a"], source="en", target="ru"))

    assert any("повтор" in record.message for record in caplog.records), (
        f"в логе нет строки о повторе: {[r.message for r in caplog.records]}"
    )


def test_translation_error_type_for_native_http_failure(monkeypatch) -> None:
    """Родной режим падает именно `TranslationError`, а не чем попало."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"error": "unavailable"})

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(
        base_url="http://llm.test:11434/v1", model="m", api_mode="ollama", retries=0
    )

    with pytest.raises(TranslationError):
        asyncio.run(translator.translate_page(["a"], source="en", target="ru"))
