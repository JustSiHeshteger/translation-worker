"""CloudEvents-конверт и схемы событий (карточка 54 §4, карточка 55 §2.5).

Проверяется: обязательные ce-* заголовки, источники (book-service для
заданий, translation-worker для результатов), subject = bookId, форматы
`ce-type` для всех пяти сообщений, валидация payload по asyncapi-схеме и
поведение pydantic-моделей (from/to глоссария, необязательные поля).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from translation_worker import cloudevents, events
from translation_worker.cloudevents import build_event

WORKSPACE_SCHEMA = (
    Path(__file__).resolve().parents[2]
    / "books-platform-openapi"
    / "asyncapi"
    / "comic-translation-events.yaml"
)

BOOK = "018f0000-0000-7000-8000-000000000001"


# ── ce-type / ce-source ────────────────────────────────────────


def test_event_types_match_contract():
    assert cloudevents.TYPE_REQUESTED == "com.books.platform.comic-translation.requested.v1"
    assert cloudevents.TYPE_PROGRESS == "com.books.platform.comic-translation.progress.v1"
    assert cloudevents.TYPE_COMPLETED == "com.books.platform.comic-translation.completed.v1"
    assert cloudevents.TYPE_FAILED == "com.books.platform.comic-translation.failed.v1"
    assert cloudevents.TYPE_DLQ == "com.books.platform.comic-translation.dlq.v1"


def test_requested_event_is_sourced_from_book_service():
    event = build_event(
        cloudevents.TYPE_REQUESTED,
        source=cloudevents.SOURCE_BOOK_SERVICE,
        subject=BOOK,
        payload={"bookId": BOOK},
        event_id="evt-1",
        time=datetime(2026, 10, 12, 9, 0, tzinfo=UTC),
    )
    assert event.headers["ce-specversion"] == "1.0"
    assert event.headers["ce-type"] == cloudevents.TYPE_REQUESTED
    assert event.headers["ce-source"] == "/book-service"
    assert event.headers["ce-subject"] == BOOK
    assert event.headers["ce-id"] == "evt-1"
    assert event.headers["ce-time"].startswith("2026-10-12T09:00:00")
    assert json.loads(event.value) == {"bookId": BOOK}


def test_result_events_are_sourced_from_worker():
    for ce_type in (
        cloudevents.TYPE_PROGRESS,
        cloudevents.TYPE_COMPLETED,
        cloudevents.TYPE_FAILED,
        cloudevents.TYPE_DLQ,
    ):
        event = build_event(ce_type, source=cloudevents.SOURCE_WORKER, subject=BOOK, payload={})
        assert event.headers["ce-source"] == "/translation-worker"


def test_event_id_is_generated_when_not_provided():
    first = build_event(
        cloudevents.TYPE_PROGRESS, source=cloudevents.SOURCE_WORKER, subject=BOOK, payload={}
    )
    second = build_event(
        cloudevents.TYPE_PROGRESS, source=cloudevents.SOURCE_WORKER, subject=BOOK, payload={}
    )
    assert first.headers["ce-id"]
    assert first.headers["ce-id"] != second.headers["ce-id"]


def test_dataschema_header_is_optional():
    without = build_event(
        cloudevents.TYPE_PROGRESS, source=cloudevents.SOURCE_WORKER, subject=BOOK, payload={}
    )
    assert "ce-dataschema" not in without.headers
    with_schema = build_event(
        cloudevents.TYPE_PROGRESS,
        source=cloudevents.SOURCE_WORKER,
        subject=BOOK,
        payload={},
        dataschema="https://schemas.test/comic-translation-events.yaml",
    )
    assert with_schema.headers["ce-dataschema"].endswith(".yaml")


def test_headers_are_readable_back_from_envelope():
    event = build_event(
        cloudevents.TYPE_COMPLETED,
        source=cloudevents.SOURCE_WORKER,
        subject=BOOK,
        payload={"x": 1},
        event_id="evt-9",
    )
    assert cloudevents.event_id(event.headers) == "evt-9"
    assert cloudevents.event_type(event.headers) == cloudevents.TYPE_COMPLETED


# ── Валидация payload по схеме контракта ───────────────────────


def test_schema_is_loaded_from_contract_file():
    assert WORKSPACE_SCHEMA.exists(), f"нет контракта событий: {WORKSPACE_SCHEMA}"
    schema = events.load_component_schema("ComicTranslationRequestedPayload", WORKSPACE_SCHEMA)
    assert schema["required"]


def test_valid_requested_payload_passes_validation(payload):
    events.validate_payload(payload, "ComicTranslationRequestedPayload", WORKSPACE_SCHEMA)
    request = events.parse_requested(payload, path=WORKSPACE_SCHEMA)
    assert str(request.book_id) == payload["bookId"]
    assert request.language == "ru"
    assert request.provider == "LLM"
    assert [page.number for page in request.pages] == [1, 2]
    assert request.retry_count == 0


def test_missing_required_field_fails_validation(payload):
    payload.pop("bookId")
    with pytest.raises(events.SchemaValidationError):
        events.validate_payload(payload, "ComicTranslationRequestedPayload", WORKSPACE_SCHEMA)


def test_invalid_language_pattern_fails_validation(payload):
    payload["language"] = "RU-ru"
    with pytest.raises(events.SchemaValidationError):
        events.validate_payload(payload, "ComicTranslationRequestedPayload", WORKSPACE_SCHEMA)


def test_unknown_provider_fails_validation(payload):
    payload["provider"] = "GOOGLE"
    with pytest.raises(events.SchemaValidationError):
        events.validate_payload(payload, "ComicTranslationRequestedPayload", WORKSPACE_SCHEMA)


def test_glossary_uses_from_to_aliases(payload):
    payload["glossary"] = [{"from": "cat", "to": "кот"}]
    request = events.parse_requested(payload, path=WORKSPACE_SCHEMA)
    assert request.glossary[0].from_ == "cat"
    assert request.glossary[0].to == "кот"
    assert request.glossary[0].model_dump(by_alias=True) == {"from": "cat", "to": "кот"}


# ── Модели результатов ─────────────────────────────────────────


def test_completed_page_payload_has_nullable_image_key():
    page = events.ComicTranslationPage(
        number=1, layer_key="translated/x/ru/page/001/layer.json", blocks=3
    )
    dumped = page.to_payload()
    assert dumped["number"] == 1
    assert dumped["blocks"] == 3
    # фаза 1: арт-слоя нет, но поле контракта присутствует и равно null
    assert "imageKey" in dumped and dumped["imageKey"] is None
    assert dumped["warnings"] == []


def test_completed_payload_keeps_provider_and_warnings():
    completed = events.ComicTranslationCompleted(
        book_id=BOOK,
        language="ru",
        completed_at=datetime(2026, 10, 12, 9, 30, tzinfo=UTC),
        pages_done=2,
        pages_total=2,
        layer_base_key=f"translated/{BOOK}/ru/page",
        pages=[],
        provider="LLM",
        duration_ms=1234,
        warnings=["page 2: low confidence"],
    )
    dumped = completed.to_payload()
    assert dumped["provider"] == "LLM"
    assert dumped["durationMs"] == 1234
    assert dumped["pagesDone"] == 2
    assert dumped["warnings"] == ["page 2: low confidence"]


def test_failed_payload_marks_non_retryable_errors():
    failed = events.ComicTranslationFailed(
        book_id=BOOK,
        language="ru",
        failed_at=datetime(2026, 10, 12, 9, 30, tzinfo=UTC),
        error_code="NOT_A_COMIC",
        retryable=False,
        pages_done=0,
    )
    dumped = failed.to_payload()
    assert dumped["errorCode"] == "NOT_A_COMIC"
    assert dumped["retryable"] is False
    assert "failedPage" not in dumped or dumped["failedPage"] is None


def test_dlq_payload_allows_missing_event_id_for_poison():
    dlq = events.ComicTranslationDlq(
        book_id=BOOK,
        language="ru",
        reason="SCHEMA_VALIDATION_FAILED",
        error_code="INTERNAL_ERROR",
        retry_count=0,
        pages_done=0,
    )
    dumped = dlq.to_payload()
    assert dumped["reason"] == "SCHEMA_VALIDATION_FAILED"
    assert "eventId" not in dumped or dumped["eventId"] is None


@pytest.fixture
def payload(request_payload):
    return request_payload()
