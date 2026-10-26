"""Модель иногда возвращает исходный текст вместо перевода — это не перевод.

Стенд: страница 3 книги X-Men 039 пришла в русскую раздачу целиком английской —
модель перевела первые пятнадцать реплик, а остальные вернула дословно. Такие
ответы проходили все проверки (id на месте, текст непустой), поэтому страница
выглядела переведённой, но читатель видел английский. Эхо источника обязано
уходить в repair и дробление, как пустой ответ, а одиночная реплика-эхо не
должна губить книгу: её оставляем как есть с предупреждением.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx

from translation_worker.providers.llm import LlmTranslator, _untranslated

SHORT_NAME = "X-MEN"
LONG_ECHO = "AND THAT'S WHY WE'RE Z*E*R*O BECAUSE WE'VE LOST"


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


def _translated(blocks: list[dict[str, str]]) -> list[dict[str, str]]:
    return [{"id": item["id"], "text": f"ПЕРЕВОД: {item['text'][:20]}"} for item in blocks]


def test_untranslated_recognises_echo() -> None:
    assert _untranslated(LONG_ECHO, LONG_ECHO, "ru") is True
    assert _untranslated("WE KNOW WHAT HAS TO BE DONE.", "МЫ ЗНАЕМ, ЧТО ДЕЛАТЬ.", "ru") is False
    # короткие имена и цифры переводить нечего — не эхо
    assert _untranslated(SHORT_NAME, SHORT_NAME, "ru") is False
    assert _untranslated("180", "180", "ru") is False
    # текст без букв целевого письма, но и без латиницы — не эхо
    assert _untranslated("...", "...", "ru") is False


def test_echoed_blocks_go_to_repair(monkeypatch) -> None:
    """Эхо в ответе — повод попросить перевод заново, а не отдать англ. текст."""
    requests: list[list[str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent = _chat(request)
        requests.append([item["id"] for item in sent])
        if len(requests) == 1:
            # последние две реплики вернулись исходным текстом
            blocks = _translated(sent[:2]) + [
                {"id": item["id"], "text": item["text"]} for item in sent[2:]
            ]
        else:
            blocks = _translated(sent)
        return _answer(blocks)

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(base_url="http://llm.test/v1", model="m")
    sources = [
        "I'VE CALLED THIS EMERGENCY MEETING BECAUSE WE HAVE AN",
        "THIS IS WHAT WEVE HAD TO SMILE WHEN A MUTANT",
        LONG_ECHO,
        "WE KNOW WHAT HAS TO BE DONE.",
    ]

    translated = asyncio.run(translator.translate_page(sources, source="en", target="ru"))

    assert len(requests) == 2, "эхо обязано вызвать ровно один repair-запрос"
    assert all("ПЕРЕВОД" in text for text in translated), translated


def test_persistent_echo_is_halved(monkeypatch) -> None:
    """Если эхо повторяется, пачка дробится: на коротких пачках модель переводит."""
    sizes: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent = _chat(request)
        sizes.append(len(sent))
        if len(sent) > 2:
            # модель ленится на длинной пачке и возвращает исходник
            return _answer([{"id": item["id"], "text": item["text"]} for item in sent])
        return _answer(_translated(sent))

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(base_url="http://llm.test/v1", model="m")
    sources = [
        "I'VE CALLED THIS EMERGENCY MEETING BECAUSE WE HAVE AN",
        "THIS IS WHAT WEVE HAD TO SMILE WHEN A MUTANT",
        LONG_ECHO,
        "WE KNOW WHAT HAS TO BE DONE.",
    ]

    translated = asyncio.run(translator.translate_page(sources, source="en", target="ru"))

    assert all("ПЕРЕВОД" in text for text in translated), translated
    assert 2 in sizes, f"ожидалось дробление до коротких пачек, было {sizes}"


def test_single_echoed_block_keeps_original_text(monkeypatch, caplog) -> None:
    """Одна реплика, которую модель упорно возвращает как есть, не валит книгу."""
    def handler(request: httpx.Request) -> httpx.Response:
        sent = _chat(request)
        return _answer([{"id": item["id"], "text": item["text"]} for item in sent])

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(base_url="http://llm.test/v1", model="m")

    with caplog.at_level("WARNING"):
        translated = asyncio.run(
            translator.translate_page([LONG_ECHO], source="en", target="ru")
        )

    assert translated == [LONG_ECHO], "оригинал остаётся, страница не падает"
    assert any("оригинал" in record.message for record in caplog.records), (
        f"нет предупреждения об оставленном оригинале: {[r.message for r in caplog.records]}"
    )
    # сводка по странице: по ней видно, что страница ушла недопереведённой
    assert any("не полностью" in record.message for record in caplog.records), (
        f"нет сводки по странице: {[r.message for r in caplog.records]}"
    )


def test_repair_request_forbids_the_source_text(monkeypatch) -> None:
    """Repair просит перевести, а не повторить исходник: иначе эхо вернётся снова."""
    systems: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        systems.append(json.loads(request.content)["messages"][0]["content"])
        sent = _chat(request)
        if len(systems) == 1:
            blocks = [{"id": item["id"], "text": item["text"]} for item in sent]
        else:
            blocks = _translated(sent)
        return _answer(blocks)

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(base_url="http://llm.test/v1", model="m")

    asyncio.run(translator.translate_page([LONG_ECHO], source="en", target="ru"))

    assert len(systems) == 2, "ожидался один repair-запрос"
    assert "исходный текст" in systems[1], f"repair не запрещает исходник: {systems[1][-160:]}"


def test_short_name_is_not_treated_as_echo(monkeypatch) -> None:
    """Короткое имя в верхнем регистре не вызывает лишних запросов."""
    requests: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent = _chat(request)
        requests.append(len(sent))
        blocks = [
            {"id": item["id"], "text": item["text"] if item["text"] == SHORT_NAME else "ПЕРЕВОД"}
            for item in sent
        ]
        return _answer(blocks)

    _patch_transport(monkeypatch, handler)
    translator = LlmTranslator(base_url="http://llm.test/v1", model="m")

    translated = asyncio.run(
        translator.translate_page([SHORT_NAME, "SOMETHING LONG ENOUGH"], source="en", target="ru")
    )

    assert requests == [2], "имя целиком в верхнем регистре не повод для repair"
    assert translated[0] == SHORT_NAME


def test_translated_text_is_never_treated_as_echo() -> None:
    assert _untranslated("Hello there, friend", "Привет, друг", "ru") is False
