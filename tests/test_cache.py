"""Кэш переводов по SHA-256 страницы (карточка 55 §2.14).

Ключ кэша = язык + провайдер + хеш содержимого страницы. Попадание кэша
пропускает OCR и перевод; в кэше лежат блоки БЕЗ привязки к странице
(id/page подставляются при сборке layer.json), поэтому layer.json побайтово
стабилен между прогонами.
"""

from __future__ import annotations

import hashlib
import json

from translation_worker import events
from translation_worker.cache import TranslationCache
from translation_worker.models import Box, TextBlock

BOOK = "018f0000-0000-7000-8000-000000000001"
PUBLIC_BASE = "http://minio.test/books-platform"


def blocks() -> list[TextBlock]:
    return [
        TextBlock(
            id="p1-b1",
            box=Box(x=10, y=20, w=100, h=40),
            kind="BUBBLE",
            original="You're late, cat.",
            text="Ты опоздал, кот.",
            confidence=0.9,
            font_size=22,
        )
    ]


def test_save_and_load_roundtrip(settings, storage):
    cache = TranslationCache(storage, settings)
    digest = hashlib.sha256(b"page-bytes").hexdigest()

    key = cache.key(language="ru", provider="LLM", sha256=digest)
    cache.save(key, blocks())
    loaded = cache.load(key)

    assert loaded is not None
    assert len(loaded) == 1
    assert loaded[0].text == "Ты опоздал, кот."
    assert loaded[0].box == Box(x=10, y=20, w=100, h=40)
    assert loaded[0].font_size == 22


def test_cache_keeps_original_and_translated_text(settings, storage):
    cache = TranslationCache(storage, settings)
    digest = hashlib.sha256(b"x").hexdigest()
    key = cache.key(language="ru", provider="LLM", sha256=digest)
    cache.save(key, blocks())

    payload = json.loads(storage.objects[key].decode("utf-8"))
    assert payload["blocks"][0]["original"] == "You're late, cat."
    assert payload["blocks"][0]["text"] == "Ты опоздал, кот."


def test_missing_entry_returns_none(settings, storage):
    cache = TranslationCache(storage, settings)
    assert cache.load(cache.key(language="ru", provider="LLM", sha256="deadbeef")) is None


def test_cache_key_depends_on_language_provider_and_content(settings, storage):
    cache = TranslationCache(storage, settings)
    base = cache.key(language="ru", provider="LLM", sha256="abc")

    assert cache.key(language="de", provider="LLM", sha256="abc") != base
    assert cache.key(language="ru", provider="DEEPL", sha256="abc") != base
    assert cache.key(language="ru", provider="LLM", sha256="def") != base
    assert cache.key(language="ru", provider="LLM", sha256="abc") == base


def test_corrupted_cache_entry_is_a_miss_not_a_crash(settings, storage):
    cache = TranslationCache(storage, settings)
    key = cache.key(language="ru", provider="LLM", sha256="abc")
    storage.put_bytes(key, b"{not json", "application/json")

    assert cache.load(key) is None


def test_second_run_hits_cache_and_skips_translator(
    settings, storage, page_png, request_payload, fake_ocr, fake_translator
):
    """Тот же контент + язык + провайдер → кэш-хит: переводчик не вызывается."""
    from translation_worker.pipeline import Pipeline

    prepared = settings.model_copy(update={"public_base": PUBLIC_BASE})
    page_url = f"{PUBLIC_BASE}/processed/up-1/page/001/original.jpg"
    payload = request_payload(pages=[{"number": 1, "url": page_url}])
    request = events.parse_requested(payload, path=_schema_path())

    storage.put_bytes("processed/up-1/page/001/original.jpg", page_png(600, 900))

    fake_ocr.default_lines = [_line()]
    cache = TranslationCache(storage, prepared)
    pipeline = Pipeline(prepared, storage, fake_ocr, fake_translator, cache)

    first = _run(pipeline, request)
    calls_after_first = fake_translator.calls
    first_bytes = storage.objects[_layer_key(1)]

    second = _run(pipeline, request)

    assert calls_after_first == 1
    # кэш-хит: ни OCR, ни перевод не повторяются, layer.json идентичен
    assert fake_translator.calls == calls_after_first
    assert storage.objects[_layer_key(1)] == first_bytes
    assert second.pages == first.pages


def _run(pipeline, request):
    import asyncio

    return asyncio.run(pipeline.process(request))


def _line():
    from translation_worker.models import TextLine

    return TextLine(text="cat", box=Box(x=10, y=10, w=60, h=20), confidence=0.9)


def _schema_path():
    from pathlib import Path

    return (
        Path(__file__).resolve().parents[2]
        / "books-platform-openapi"
        / "asyncapi"
        / "comic-translation-events.yaml"
    )


def _layer_key(page: int) -> str:
    return f"translated/{BOOK}/ru/page/{page:03d}/layer.json"
