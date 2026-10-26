"""Промпт перевода: короткие слова вместо длинных синонимов.

Владелец попросил, чтобы перевод предпочитал короткие слова: если у слова или
оборота есть короткий синоним, подходящий по смыслу, в плашку идёт он.
Проверяется именно то, что уходит в модель: правило обязано быть в системном
сообщении каждого запроса, иначе локальная модель его не увидит.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx

from translation_worker.providers.llm import LlmTranslator


def _patch_transport(monkeypatch, handler) -> None:
    """Подменить httpx-клиент мок-транспортом: проверяется настоящий `_post`."""
    transport = httpx.MockTransport(handler)
    real_client = httpx.AsyncClient

    def factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        kwargs["transport"] = transport
        return real_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", factory)


def _answer(payload: dict[str, Any]) -> httpx.Response:
    """Ответ модели: по строке на каждый присланный блок."""
    sent = json.loads(payload["messages"][-1]["content"])["blocks"]
    blocks = [{"id": item["id"], "text": item["text"]} for item in sent]
    return httpx.Response(
        200, json={"choices": [{"message": {"content": json.dumps({"blocks": blocks})}}]}
    )


def _system_of(request: httpx.Request) -> str:
    """Системное сообщение запроса — то, где живут правила стиля."""
    body = json.loads(request.content)
    systems = [message for message in body["messages"] if message["role"] == "system"]
    assert len(systems) == 1, "запрос обязан нести ровно одно системное сообщение"
    return systems[0]["content"].lower()


def test_page_prompt_asks_for_shorter_synonyms(monkeypatch) -> None:
    """Перевод страницы требует короткий синоним, когда он подходит по смыслу."""
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(_system_of(request))
        return _answer(json.loads(request.content))

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(base_url="http://llm.test/v1", model="m")
    asyncio.run(translator.translate_page(["Длинная реплика"], source="en", target="ru"))

    assert seen, "запрос к модели не ушёл"
    prompt = seen[0]
    assert "синоним" in prompt, f"нет правила про синонимы: {prompt[:200]}"
    assert "коротк" in prompt, "правило обязано называть признак — короткое слово"
    # смысл важнее длины: иначе модель начнёт обрезать реплики
    assert "смысл" in prompt, "правило обязано запрещать потерю смысла ради длины"


def test_single_translation_uses_the_same_prompt(monkeypatch) -> None:
    """Одиночный `translate` не живёт отдельным промптом: правило одно на всех."""
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(_system_of(request))
        return _answer(json.loads(request.content))

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(base_url="http://llm.test/v1", model="m")
    asyncio.run(translator.translate("Длинная реплика", source="en", target="ru"))

    assert seen, "запрос к модели не ушёл"
    assert "синоним" in seen[0], "у одиночного перевода другой промпт — правило потерялось"


def test_glossary_still_wins_over_brevity(monkeypatch) -> None:
    """Глоссарий и имена важнее краткости: термин переводится дословно."""
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(_system_of(request))
        return _answer(json.loads(request.content))

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(base_url="http://llm.test/v1", model="m", )
    asyncio.run(
        translator.translate_page(
            ["Reed Richards"], source="en", target="ru", glossary={"Reed Richards": "Рид Ричардс"}
        )
    )

    prompt = seen[0]
    assert "глоссари" in prompt and "дословно" in prompt, f"пропал глоссарий: {prompt[:200]}"
    assert "рид ричардс" in prompt, "термин глоссария обязан доехать до модели"


def test_repair_request_keeps_the_style_rule(monkeypatch) -> None:
    """Repair-запрос дописывает системное сообщение, а не заменяет его."""
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        seen.append(_system_of(request))
        sent = json.loads(payload["messages"][-1]["content"])["blocks"]
        if len(seen) == 1:
            # неполный ответ: не хватает одного id, начинается repair
            blocks = [{"id": sent[0]["id"], "text": sent[0]["text"]}]
        else:
            blocks = [{"id": item["id"], "text": item["text"]} for item in sent]
        return httpx.Response(
            200, json={"choices": [{"message": {"content": json.dumps({"blocks": blocks})}}]}
        )

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(base_url="http://llm.test/v1", model="m")

    asyncio.run(translator.translate_page(["a", "b"], source="en", target="ru"))

    assert len(seen) == 2, "должен быть ровно один repair-запрос"
    assert "синоним" in seen[1], "в repair-запросе правило про короткие слова потерялось"
    assert "нужны ровно id" in seen[1], "repair-запрос обязан просить недостающие id"

