"""Ollama может прервать генерацию: «token repeat limit reached» → HTTP 500.

Модель зацикливается на повторе токенов, и сервер не отдаёт ответ вообще. Это не
транспортная ошибка и не повод ждать тот же таймаут трижды: деградация лечится
дроблением пачки (на короткой пачке цикл не заводится), а одиночная реплика
остаётся оригиналом с предупреждением — иначе одна страница роняет книгу.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest

from translation_worker.providers.base import TranslationError
from translation_worker.providers.llm import _GenerationAborted, LlmTranslator

REPEAT_BODY = {"error": "prediction aborted, token repeat limit reached"}


def _patch_transport(monkeypatch, handler) -> None:
    transport = httpx.MockTransport(handler)
    real_client = httpx.AsyncClient

    def factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        kwargs["transport"] = transport
        return real_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", factory)


def _chat(request: httpx.Request) -> list[dict[str, str]]:
    return json.loads(json.loads(request.content)["messages"][-1]["content"])["blocks"]


def _answer(blocks: list[dict[str, str]]) -> httpx.Response:
    return httpx.Response(
        200, json={"choices": [{"message": {"content": json.dumps({"blocks": blocks})}}]}
    )


def test_repeat_abort_is_not_retried_as_transport_error(monkeypatch) -> None:
    """Лимит повторов — не транспортный сбой: тот же запрос не повторяем."""
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(len(_chat(request)))
        return httpx.Response(500, json=REPEAT_BODY)

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(base_url="http://llm.test/v1", model="m", retries=2)

    translated = asyncio.run(
        translator.translate_page(["first line", "second line"], source="en", target="ru")
    )

    # по одному запросу на пачку: 2 реплики, затем по одной — без повторов
    assert calls == [2, 1, 1], f"повторы на лимит повторов бессмысленны, было {calls}"
    assert translated == ["first line", "second line"], "оригиналы остаются на месте"


def test_repeat_abort_is_halved_until_it_passes(monkeypatch) -> None:
    """Дробление лечит цикл: на короткой пачке модель отвечает нормально."""
    sizes: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent = _chat(request)
        sizes.append(len(sent))
        if len(sent) > 1:
            return httpx.Response(500, json=REPEAT_BODY)
        return _answer(
            [{"id": item["id"], "text": f"ПЕРЕВОД: {item['text'][:15]}"} for item in sent]
        )

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(base_url="http://llm.test/v1", model="m")

    translated = asyncio.run(
        translator.translate_page(["первая строка", "вторая строка"], source="en", target="ru")
    )

    assert all("ПЕРЕВОД" in text for text in translated), translated
    assert sizes[0] == 2 and sizes[1:] == [1, 1], f"ожидалось дробление пополам, было {sizes}"


def test_single_block_repeat_abort_keeps_original(monkeypatch, caplog) -> None:
    """Одна реплика зацикливает модель — оставляем оригинал, страница живёт."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json=REPEAT_BODY)

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(base_url="http://llm.test/v1", model="m")

    with caplog.at_level("WARNING"):
        translated = asyncio.run(
            translator.translate_page(["WE KNOW WHAT HAS TO BE DONE."], source="en", target="ru")
        )

    assert translated == ["WE KNOW WHAT HAS TO BE DONE."]
    assert any("зациклился" in record.message for record in caplog.records), (
        f"нет предупреждения о зацикливании: {[r.message for r in caplog.records]}"
    )


def test_repeat_abort_on_the_native_ollama_endpoint(monkeypatch) -> None:
    """Тот же отказ на родном `/api/chat` — боевой путь локальной модели."""
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path != "/api/chat":
            raise AssertionError(f"ожидался родной эндпоинт, а не {request.url.path}")
        calls.append(len(_chat(request)))
        return httpx.Response(500, json=REPEAT_BODY)

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(
        base_url="http://llm.test:11434/v1", model="m", api_mode="ollama", retries=2
    )

    translated = asyncio.run(
        translator.translate_page(["first line", "second line"], source="en", target="ru")
    )

    assert calls == [2, 1, 1], f"на родном пути ожидалось дробление без повторов, было {calls}"
    assert translated == ["first line", "second line"]


def test_plain_http_failure_still_fails_the_page(monkeypatch) -> None:
    """Обычный 500 без признака лимита повторов остаётся честной ошибкой."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "internal"})

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(base_url="http://llm.test/v1", model="m", retries=0)

    with pytest.raises(TranslationError) as failure:
        asyncio.run(
            translator.translate_page(["WE KNOW WHAT HAS TO BE DONE."], source="en", target="ru")
        )

    assert not isinstance(failure.value, _GenerationAborted)
