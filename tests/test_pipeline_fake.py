"""Пайплайн на fake-провайдерах (карточка 55 §2.15, §5).

Проверяется весь путь страницы: S3-страница → OCR → группировка → перевод →
вёрстка → `layer.json` в детерминированном ключе, прогресс не чаще интервала
и только вперёд, устойчивость к сбоям (страница падает, книга — нет), таймаут
задания и точный формат слоя из карты цепочки.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from translation_worker import events
from translation_worker.cache import TranslationCache
from translation_worker.models import Box, TextLine
from translation_worker.pipeline import Pipeline

BOOK = "018f0000-0000-7000-8000-000000000001"
PUBLIC_BASE = "http://minio.test/books-platform"
SCHEMA = (
    Path(__file__).resolve().parents[2]
    / "books-platform-openapi"
    / "asyncapi"
    / "comic-translation-events.yaml"
)


def lines(count: int = 2) -> list[TextLine]:
    return [
        TextLine(
            text=f"line {index}",
            box=Box(x=100, y=200 + index * 50, w=200, h=40),
            confidence=0.9,
        )
        for index in range(count)
    ]


@pytest.fixture
def prepared(settings):
    return settings.model_copy(update={"public_base": PUBLIC_BASE})


@pytest.fixture
def request_pages(request_payload):
    def _make(numbers: tuple[int, ...] = (1, 2)):
        return request_payload(
            pages=[
                {
                    "number": number,
                    "url": f"{PUBLIC_BASE}/processed/up-1/page/{number:03d}/original.jpg",
                }
                for number in numbers
            ]
        )

    return _make


def load_pages(storage, page_png, numbers: tuple[int, ...] = (1, 2)) -> None:
    for number in numbers:
        storage.put_bytes(f"processed/up-1/page/{number:03d}/original.jpg", page_png(1200, 1800))


def execute(config, storage, ocr, translator, payload, on_progress=None):
    """Собрать пайплайн и прогнать один запрос до конца."""
    pipeline = Pipeline(config, storage, ocr, translator, TranslationCache(config, storage))
    request = events.parse_requested(payload, path=SCHEMA)
    return asyncio.run(pipeline.process(request, on_progress=on_progress))


# ── Счастливый путь ────────────────────────────────────────────


def test_every_page_produces_layer_json_with_original_coordinates(
    prepared, storage, page_png, fake_ocr, fake_translator, request_pages
):
    load_pages(storage, page_png)
    fake_ocr.pages = [lines(2), lines(1)]
    payload = request_pages()

    result = execute(prepared, storage, fake_ocr, fake_translator, payload)

    assert isinstance(result, events.ComicTranslationCompleted)
    assert result.pages_done == 2
    assert result.pages_total == 2
    assert result.provider == "LLM"
    assert result.warnings == []
    assert [page.number for page in result.pages] == [1, 2]
    assert [page.layer_key for page in result.pages] == [
        f"translated/{BOOK}/ru/page/001/layer.json",
        f"translated/{BOOK}/ru/page/002/layer.json",
    ]
    assert result.layer_base_key == f"translated/{BOOK}/ru/page"
    # фаза 1: арт-слоя нет
    assert all(page.image_key is None for page in result.pages)
    assert [page.blocks for page in result.pages] == [2, 1]


def test_layer_json_matches_chain_map_format(
    prepared, storage, page_png, fake_ocr, fake_translator, request_pages
):
    load_pages(storage, page_png, numbers=(1,))
    fake_ocr.pages = [lines(1)]

    execute(prepared, storage, fake_ocr, fake_translator, request_pages(numbers=(1,)))

    layer = storage.json_at(f"translated/{BOOK}/ru/page/001/layer.json")
    assert set(layer) == {
        "version",
        "page",
        "language",
        "sourceLanguage",
        "width",
        "height",
        "provider",
        "blocks",
    }
    assert layer["version"] == 1
    assert layer["page"] == 1
    assert layer["language"] == "ru"
    assert layer["sourceLanguage"] == "en"
    assert layer["width"] == 1200
    assert layer["height"] == 1800
    assert layer["provider"] == "LLM"

    block = layer["blocks"][0]
    assert set(block) == {"id", "box", "kind", "original", "text", "confidence", "fontSize"}
    assert block["id"] == "p1-b1"
    assert block["kind"] == "BUBBLE"
    assert block["box"] == {"x": 100, "y": 200, "w": 200, "h": 40}
    assert block["original"] == "line 0"
    assert block["text"] == "переведено: line 0"
    assert block["fontSize"] >= 10


def test_layer_json_is_byte_stable_between_runs(
    prepared, storage, page_png, fake_ocr, fake_translator, request_pages
):
    load_pages(storage, page_png)
    fake_ocr.pages = [lines(2), lines(2)]
    payload = request_pages()

    execute(prepared, storage, fake_ocr, fake_translator, payload)
    first = storage.objects[f"translated/{BOOK}/ru/page/001/layer.json"]

    execute(prepared, storage, fake_ocr, fake_translator, payload)
    assert storage.objects[f"translated/{BOOK}/ru/page/001/layer.json"] == first


# ── Страница без текста: книга не падает ───────────────────────


def test_page_without_detected_text_yields_warning_and_zero_blocks(
    prepared, storage, page_png, fake_ocr, fake_translator, request_pages
):
    load_pages(storage, page_png)
    fake_ocr.pages = [lines(1), []]

    result = execute(prepared, storage, fake_ocr, fake_translator, request_pages())

    assert isinstance(result, events.ComicTranslationCompleted)
    assert result.pages[1].blocks == 0
    assert "page 2: no text" in result.warnings
    assert "page 2: no text" in result.pages[1].warnings
    # слой для страницы всё равно записан (пустой blocks) — читалка не 404
    layer = storage.json_at(f"translated/{BOOK}/ru/page/002/layer.json")
    assert layer["blocks"] == []


# ── Ошибки провайдеров ─────────────────────────────────────────


def test_ocr_failure_fails_book_with_retryable_code(
    prepared, storage, page_png, fake_ocr, fake_translator, request_pages
):
    load_pages(storage, page_png)
    fake_ocr.error = RuntimeError("ocr exploded")

    result = execute(prepared, storage, fake_ocr, fake_translator, request_pages())

    assert isinstance(result, events.ComicTranslationFailed)
    assert result.error_code == "OCR_FAILED"
    assert result.retryable is True
    assert result.pages_done == 0
    assert result.failed_page == 1


def test_translation_failure_keeps_completed_pages_count(
    prepared, storage, page_png, fake_ocr, fake_translator, request_pages
):
    load_pages(storage, page_png)
    fake_ocr.pages = [lines(1), lines(1)]
    fake_translator.error = RuntimeError("llm exploded")

    result = execute(prepared, storage, fake_ocr, fake_translator, request_pages())

    assert isinstance(result, events.ComicTranslationFailed)
    assert result.error_code == "TRANSLATION_FAILED"
    assert result.retryable is True
    assert result.failed_page == 1


def test_timeout_produces_retryable_timeout_failure(
    prepared, storage, page_png, fake_ocr, fake_translator, request_pages
):
    load_pages(storage, page_png)
    fake_ocr.pages = [lines(1)]
    fake_translator.delay = 0.05
    tight = prepared.model_copy(update={"timeout": 0.001})

    result = execute(tight, storage, fake_ocr, fake_translator, request_pages())

    assert isinstance(result, events.ComicTranslationFailed)
    assert result.error_code == "TIMEOUT"
    assert result.retryable is True


def test_missing_page_object_fails_with_page_fetch_error(
    prepared, storage, page_png, fake_ocr, fake_translator, request_pages
):
    # страница 2 отсутствует в хранилище
    load_pages(storage, page_png, numbers=(1,))
    fake_ocr.pages = [lines(1), lines(1)]

    result = execute(prepared, storage, fake_ocr, fake_translator, request_pages())

    assert isinstance(result, events.ComicTranslationFailed)
    assert result.error_code == "PAGE_FETCH_FAILED"
    assert result.retryable is True


# ── Прогресс ───────────────────────────────────────────────────


def test_progress_is_monotonic_and_ends_at_one_hundred(
    prepared, storage, page_png, fake_ocr, fake_translator, request_pages
):
    numbers = (1, 2, 3)
    load_pages(storage, page_png, numbers=numbers)
    fake_ocr.pages = [lines(1), lines(1), lines(1)]
    collected: list[events.ComicTranslationProgress] = []

    async def on_progress(progress: events.ComicTranslationProgress) -> None:
        collected.append(progress)

    result = execute(
        prepared, storage, fake_ocr, fake_translator, request_pages(numbers=numbers), on_progress
    )

    assert isinstance(result, events.ComicTranslationCompleted)
    assert collected, "прогресс должен публиковаться"
    percents = [item.percent for item in collected]
    assert percents == sorted(percents)
    assert collected[-1].percent == 100
    assert collected[-1].pages_done == 3
    assert all(item.pages_total == 3 for item in collected)
    assert all(
        item.stage in {"DOWNLOAD", "OCR", "TRANSLATE", "LAYOUT"} for item in collected
    )


def test_progress_is_throttled_by_interval(
    prepared, storage, page_png, fake_ocr, fake_translator, request_pages
):
    numbers = (1, 2, 3)
    load_pages(storage, page_png, numbers=numbers)
    fake_ocr.pages = [lines(1), lines(1), lines(1)]
    throttled = prepared.model_copy(update={"progress_interval": 3600.0})
    collected: list[events.ComicTranslationProgress] = []

    async def on_progress(progress: events.ComicTranslationProgress) -> None:
        collected.append(progress)

    execute(
        throttled, storage, fake_ocr, fake_translator, request_pages(numbers=numbers), on_progress
    )

    # интервал не истёк — не больше одного события на этап
    assert len(collected) <= 2


# ── Идемпотентность артефактов ─────────────────────────────────


def test_repeated_processing_rewrites_identical_artifacts(
    prepared, storage, page_png, fake_ocr, fake_translator, request_pages
):
    load_pages(storage, page_png)
    fake_ocr.pages = [lines(2), lines(2)]
    payload = request_pages()
    layer_path = f"translated/{BOOK}/ru/page/001/layer.json"

    first = execute(prepared, storage, fake_ocr, fake_translator, payload)
    first_layer = storage.objects[layer_path]

    second = execute(prepared, storage, fake_ocr, fake_translator, payload)

    assert isinstance(first, events.ComicTranslationCompleted)
    assert isinstance(second, events.ComicTranslationCompleted)
    assert storage.objects[layer_path] == first_layer
    assert json.loads(first_layer)["blocks"][0]["id"] == "p1-b1"
