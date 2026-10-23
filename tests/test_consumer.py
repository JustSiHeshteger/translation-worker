"""Kafka-консьюмер заданий перевода (карточка 55 §2.17, §5).

Проверяется: happy-path публикует Completed в results, ретраибельный сбой —
Failed + переопубликованный Requested с retryCount+1, исчерпанные ретраи и
неретраибельные коды — DLQ, poison-payload (не JSON) — тихий skip с коммитом,
дубликат ce-id — пропуск без публикаций, payload вне схемы —
Failed(INTERNAL_ERROR) + DLQ SCHEMA_VALIDATION_FAILED.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from translation_worker import cloudevents, events
from translation_worker.kafka_consumer import IncomingRecord, TranslationConsumer

BOOK = "018f0000-0000-7000-8000-000000000001"
SCHEMA = (
    Path(__file__).resolve().parents[2]
    / "books-platform-openapi"
    / "asyncapi"
    / "comic-translation-events.yaml"
)


class FakeDedup:
    """Дедуп-двойник с трёхместным резервом (§2.16, `SET NX`)."""

    def __init__(self, seen_ids: set[str] | None = None, busy_ids: set[str] | None = None) -> None:
        self.seen_ids = seen_ids or set()
        self.busy_ids = busy_ids or set()
        self.marked: list[str] = []
        self.released: list[str] = []
        self.inflight: set[str] = set()

    async def acquire(self, event_id: str) -> str:
        from translation_worker.kafka_consumer import (
            ACQUIRE_ACQUIRED,
            ACQUIRE_BUSY,
            ACQUIRE_DUPLICATE,
        )

        if event_id in self.seen_ids:
            return ACQUIRE_DUPLICATE
        if event_id in self.busy_ids or event_id in self.inflight:
            return ACQUIRE_BUSY
        self.inflight.add(event_id)
        return ACQUIRE_ACQUIRED

    async def complete(self, event_id: str) -> None:
        self.inflight.discard(event_id)
        self.marked.append(event_id)

    async def release(self, event_id: str) -> None:
        self.inflight.discard(event_id)
        self.released.append(event_id)


class StubPipeline:
    """Пайплайн-заглушка: возвращает заранее заданный результат или падает."""

    def __init__(self, result) -> None:
        self.result = result
        self.requests = []

    async def process(self, request, on_progress=None):
        self.requests.append(request)
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def completed(provider: str = "LLM") -> events.ComicTranslationCompleted:
    return events.ComicTranslationCompleted(
        book_id=BOOK,
        language="ru",
        completed_at=datetime(2026, 10, 12, 9, 30, tzinfo=UTC),
        pages_done=2,
        pages_total=2,
        layer_base_key=f"translated/{BOOK}/ru/page",
        pages=[
            events.ComicTranslationPage(
                number=1, layer_key=f"translated/{BOOK}/ru/page/001/layer.json", blocks=2
            )
        ],
        provider=provider,
        duration_ms=500,
        warnings=[],
    )


def failed(code: str, retryable: bool) -> events.ComicTranslationFailed:
    return events.ComicTranslationFailed(
        book_id=BOOK,
        language="ru",
        failed_at=datetime(2026, 10, 12, 9, 30, tzinfo=UTC),
        error_code=code,
        retryable=retryable,
        pages_done=1,
        failed_page=2,
    )


@pytest.fixture
def consumer_factory(settings, fake_ocr, fake_translator, publisher):
    def _make(result, *, dedup=None, request_payload=None):
        pipeline = StubPipeline(result)
        consumer = TranslationConsumer(
            settings,
            pipeline,
            publisher,
            dedup=dedup or FakeDedup(),
            schema_path=SCHEMA,
        )
        return consumer, pipeline

    return _make


def record(
    payload: dict | str, *, event_id: str = "evt-1", ce_type: str | None = None
) -> IncomingRecord:
    body = payload if isinstance(payload, str) else json.dumps(payload)
    return IncomingRecord(
        key=BOOK,
        value=body,
        headers={
            "ce-id": event_id,
            "ce-type": ce_type or cloudevents.TYPE_REQUESTED,
            "ce-source": cloudevents.SOURCE_BOOK_SERVICE,
            "ce-subject": BOOK,
        },
    )


# ── Happy path ─────────────────────────────────────────────────


async def test_completed_is_published_to_results(consumer_factory, publisher, request_payload):
    consumer, pipeline = consumer_factory(completed())

    outcome = await consumer.handle_record(record(request_payload()))

    assert outcome.action == "completed"
    assert len(pipeline.requests) == 1
    assert publisher.types() == [cloudevents.TYPE_COMPLETED]
    topic, event = publisher.published[0]
    assert topic == "comic-translations.results"
    assert event.headers["ce-source"] == "/translation-worker"
    assert event.headers["ce-subject"] == BOOK
    assert json.loads(event.value)["provider"] == "LLM"


# ── Ретраи ────────────────────────────────────────────────────


async def test_retryable_failure_republishes_request_with_incremented_retry(
    consumer_factory, publisher, request_payload
):
    consumer, _ = consumer_factory(failed("OCR_FAILED", retryable=True))
    payload = request_payload(retry_count=0)

    outcome = await consumer.handle_record(record(payload))

    assert outcome.action == "failed"
    assert publisher.types() == [cloudevents.TYPE_FAILED, cloudevents.TYPE_REQUESTED]

    failed_payload = publisher.payloads(cloudevents.TYPE_FAILED)[0]
    assert failed_payload["errorCode"] == "OCR_FAILED"

    requests_topic, republished = publisher.published[1]
    assert requests_topic == "comic-translations.requests"
    assert json.loads(republished.value)["retryCount"] == 1
    assert json.loads(republished.value)["bookId"] == BOOK
    # переопубликовал воркер — источник честно он
    assert republished.headers["ce-source"] == "/translation-worker"


async def test_exhausted_retries_go_to_dlq(consumer_factory, publisher, request_payload, settings):
    consumer, _ = consumer_factory(failed("OCR_FAILED", retryable=True))
    payload = request_payload(retry_count=settings.max_retries)

    outcome = await consumer.handle_record(record(payload))

    assert outcome.action == "dlq"
    assert cloudevents.TYPE_REQUESTED not in publisher.types()
    dlq_topic, dlq = publisher.published[-1]
    assert dlq_topic == "comic-translations.dlq"
    assert dlq.headers["ce-type"] == cloudevents.TYPE_DLQ
    body = json.loads(dlq.value)
    assert body["reason"] == "MAX_RETRIES_EXCEEDED"
    assert body["errorCode"] == "OCR_FAILED"
    assert body["eventId"] == "evt-1"


async def test_non_retryable_failure_goes_to_dlq_without_republish(
    consumer_factory, publisher, request_payload
):
    consumer, _ = consumer_factory(failed("NOT_A_COMIC", retryable=False))

    outcome = await consumer.handle_record(record(request_payload()))

    assert outcome.action == "dlq"
    assert cloudevents.TYPE_REQUESTED not in publisher.types()
    assert publisher.payloads(cloudevents.TYPE_FAILED)[0]["errorCode"] == "NOT_A_COMIC"
    assert publisher.payloads(cloudevents.TYPE_DLQ)[0]["reason"] == "NON_RETRYABLE_ERROR"


# ── Poison / дубликаты / чужие типы ───────────────────────────


async def test_poison_payload_is_skipped_and_committed(consumer_factory, publisher):
    consumer, pipeline = consumer_factory(completed())

    outcome = await consumer.handle_record(record("{definitely not json"))

    assert outcome.action == "poison"
    assert pipeline.requests == []
    assert publisher.published == []


async def test_duplicate_event_id_is_skipped(consumer_factory, publisher, request_payload):
    consumer, pipeline = consumer_factory(completed(), dedup=FakeDedup({"evt-7"}))

    outcome = await consumer.handle_record(record(request_payload(), event_id="evt-7"))

    assert outcome.action == "duplicate"
    assert pipeline.requests == []
    assert publisher.published == []


async def test_foreign_event_type_is_skipped(consumer_factory, publisher, request_payload):
    consumer, pipeline = consumer_factory(completed())

    outcome = await consumer.handle_record(
        record(request_payload(), ce_type=cloudevents.TYPE_COMPLETED)
    )

    assert outcome.action == "skipped"
    assert pipeline.requests == []
    assert publisher.published == []


async def test_tombstone_is_skipped(consumer_factory, publisher):
    consumer, pipeline = consumer_factory(completed())

    outcome = await consumer.handle_record(
        IncomingRecord(key=BOOK, value=None, headers={"ce-type": cloudevents.TYPE_REQUESTED})
    )

    assert outcome.action == "skipped"
    assert publisher.published == []


# ── Схема ─────────────────────────────────────────────────────


async def test_payload_outside_schema_fails_with_internal_error_and_dlq(
    consumer_factory, publisher, request_payload
):
    consumer, pipeline = consumer_factory(completed())
    broken = request_payload()
    broken["language"] = "RUSSIAN"

    outcome = await consumer.handle_record(record(broken))

    assert outcome.action == "dlq"
    assert pipeline.requests == []
    assert publisher.payloads(cloudevents.TYPE_FAILED)[0]["errorCode"] == "INTERNAL_ERROR"
    assert publisher.payloads(cloudevents.TYPE_DLQ)[0]["reason"] == "SCHEMA_VALIDATION_FAILED"


async def test_successful_processing_marks_event_as_processed(consumer_factory, request_payload):
    dedup = FakeDedup()
    consumer, _ = consumer_factory(completed(), dedup=dedup)

    await consumer.handle_record(record(request_payload(), event_id="evt-42"))

    assert dedup.marked == ["evt-42"]


async def test_pipeline_crash_becomes_failure_event_not_exception(
    consumer_factory, publisher, request_payload
):
    consumer, _ = consumer_factory(RuntimeError("boom"))

    outcome = await consumer.handle_record(record(request_payload()))

    # непойманное исключение пайплайна не должно уронить консюмер без события
    assert outcome.action in {"dlq", "failed"}
    assert publisher.types()[0] == cloudevents.TYPE_FAILED

# ── Резерв дедупа и отмена (known issues ревью) ────────────────


class _FailingPublisher:
    """Публикация исхода падает: исход не доставлен, резерв должен быть снят."""

    def __init__(self) -> None:
        self.published: list[str] = []

    async def publish(self, topic: str, event) -> None:
        raise ConnectionError("kafka is down")


class _CancellingPipeline:
    """SIGTERM посреди задания: пайплайн отменён."""

    async def process(self, request, on_progress=None):
        raise asyncio.CancelledError


async def test_reservation_released_when_outcome_publish_fails(
    settings, request_payload
):
    dedup = FakeDedup()
    consumer = TranslationConsumer(
        settings,
        StubPipeline(completed()),
        _FailingPublisher(),
        dedup=dedup,
        schema_path=SCHEMA,
    )

    with pytest.raises(ConnectionError):
        await consumer.handle_record(record(request_payload()))

    assert dedup.released == ["evt-1"], "резерв снимается, чтобы запись дошла повторно"
    assert dedup.marked == [], "отметка «готово» ставится только после публикации исхода"
    assert dedup.inflight == set()


async def test_busy_event_is_not_treated_as_duplicate(consumer_factory, publisher, request_payload):
    consumer, pipeline = consumer_factory(completed(), dedup=FakeDedup(busy_ids={"evt-1"}))

    outcome = await consumer.handle_record(record(request_payload()))

    assert outcome.action == "busy", "занятое чужой репликой нельзя коммитить как дубликат"
    assert outcome.published == []
    assert pipeline.requests == []
    assert publisher.published == []


async def test_processing_without_dedup_still_works(settings, request_payload, publisher):
    consumer = TranslationConsumer(
        settings, StubPipeline(completed()), publisher, dedup=None, schema_path=SCHEMA
    )

    outcome = await consumer.handle_record(record(request_payload()))

    assert outcome.action == "completed"


async def test_cancellation_releases_reservation(settings, request_payload):
    dedup = FakeDedup()
    consumer = TranslationConsumer(
        settings, _CancellingPipeline(), publisher=None, dedup=dedup, schema_path=SCHEMA
    )

    with pytest.raises(asyncio.CancelledError):
        await consumer.handle_record(record(request_payload()))

    assert dedup.released == ["evt-1"], "остановка пода не должна оставлять резерв"

class _SlowDedup(FakeDedup):
    """`release` уступает управление — так проверяется `asyncio.shield`."""

    async def release(self, event_id: str) -> None:
        for _ in range(3):
            await asyncio.sleep(0)
        await super().release(event_id)


class _BlockingPipeline:
    """Задание в работе: корутина ждёт отмены (SIGTERM посреди страницы)."""

    def __init__(self) -> None:
        self.started = asyncio.Event()

    async def process(self, request, on_progress=None):
        self.started.set()
        await asyncio.sleep(3600)


async def test_release_survives_double_cancellation(settings, request_payload):
    """Второй `cancel()` не должен обрывать снятие резерва (иначе фантомный busy)."""
    dedup = _SlowDedup()
    pipeline = _BlockingPipeline()
    consumer = TranslationConsumer(
        settings, pipeline, publisher=None, dedup=dedup, schema_path=SCHEMA
    )

    task = asyncio.create_task(consumer.handle_record(record(request_payload())))
    await asyncio.wait_for(pipeline.started.wait(), timeout=5)

    task.cancel()
    task.cancel()  # отмена приходит ещё раз, пока release уже в полёте
    with pytest.raises(asyncio.CancelledError):
        await task

    assert dedup.released == ["evt-1"], "резерв обязан быть снят даже при двойной отмене"