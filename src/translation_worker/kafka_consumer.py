"""Обработка Kafka-записей задания перевода (карточка 55 §2.16, §2.17).

Модуль намеренно не знает про aiokafka: он получает уже разобранную запись
(`IncomingRecord`) и возвращает исход (`HandleResult`), поэтому вся логика
ретраев/DLQ/дедупа проверяется без брокера. Сам цикл consume/commit живёт в
`__main__`: оффсет коммитится только после публикации исхода.

Гарантии (at-least-once):
- повторный `ce-id` → `duplicate` без публикаций (dedup в Redis, TTL 24h);
- не-JSON → WARN + `poison` (коммитим, публикаций нет — событие построить нельзя);
- чужой `ce-type` и tombstone → `skipped`;
- payload вне схемы → `Failed(INTERNAL_ERROR)` + DLQ `SCHEMA_VALIDATION_FAILED`;
- retryable-сбой и `retryCount < max_retries` → `Failed` + переопубликованное
  `Requested(retryCount+1)` тем же ключом `bookId`;
- лимит исчерпан → DLQ `MAX_RETRIES_EXCEEDED`; неисправимый код →
  DLQ `NON_RETRYABLE_ERROR` без переопубликации.
"""

from __future__ import annotations

import json
import logging
import uuid as _uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from translation_worker import cloudevents, events
from translation_worker.config import Settings

_LOG = logging.getLogger(__name__)

def _is_diagnosable(book_id: str, language: str) -> bool:
    """Можно ли связать отказ по схеме с конкретным заданием.

    `bookId` контракта — UUID; язык в диагностическом событии допустим любой
    непустой код (значение берётся из отвергнутого payload'а как есть).
    """
    if not book_id or not language or len(language) > 32:
        return False
    if not language.isprintable() or " " in language:
        return False
    try:
        _uuid.UUID(book_id)
    except ValueError:
        return False
    return True

ACTION_COMPLETED = "completed"
ACTION_FAILED = "failed"
ACTION_DLQ = "dlq"
ACTION_SKIPPED = "skipped"
ACTION_DUPLICATE = "duplicate"
ACTION_POISON = "poison"

REASON_MAX_RETRIES = "MAX_RETRIES_EXCEEDED"
REASON_NON_RETRYABLE = "NON_RETRYABLE_ERROR"
REASON_SCHEMA = "SCHEMA_VALIDATION_FAILED"

_INTERNAL_ERROR = "INTERNAL_ERROR"
_MESSAGE_LIMIT = 500


@dataclass
class IncomingRecord:
    """Запись Kafka: ключ партиционирования, тело и CE-заголовки."""

    key: str | None
    value: str | bytes | None
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class HandleResult:
    """Исход обработки записи и топики, в которые что-то опубликовано."""

    action: str
    published: list[str] = field(default_factory=list)


class Publisher(Protocol):
    """Публикация CloudEvent в топик."""

    async def publish(self, topic: str, event: cloudevents.CloudEvent) -> None: ...


class DedupStore(Protocol):
    """Идемпотентность по `ce-id` (Redis, TTL 24h)."""

    async def seen(self, event_id: str) -> bool: ...

    async def mark(self, event_id: str) -> None: ...


class PagePipeline(Protocol):
    """Пайплайн задания: возвращает событие исхода (Completed или Failed)."""

    async def process(
        self,
        request: events.ComicTranslationRequested,
        on_progress: Callable[[events.ComicTranslationProgress], Awaitable[None]] | None = None,
    ) -> events.ComicTranslationCompleted | events.ComicTranslationFailed: ...


class TranslationConsumer:
    """Логика консьюмера: валидация → пайплайн → события исхода и DLQ."""

    def __init__(
        self,
        settings: Settings,
        pipeline: PagePipeline,
        publisher: Publisher,
        *,
        dedup: DedupStore | None = None,
        schema_path: str | Path | None = None,
    ) -> None:
        self._settings = settings
        self._pipeline = pipeline
        self._publisher = publisher
        self._dedup = dedup
        self._schema_path = schema_path

    async def handle_record(self, record: IncomingRecord) -> HandleResult:
        """Обработать одну запись; исключения наружу не выпускаются."""
        headers = record.headers or {}
        if record.value is None:
            _LOG.info("tombstone: пропуск записи ключа %s", record.key)
            return HandleResult(ACTION_SKIPPED)
        ce_type = cloudevents.event_type(headers)
        if ce_type != cloudevents.TYPE_REQUESTED:
            _LOG.info("пропуск события чужого типа: %s", ce_type)
            return HandleResult(ACTION_SKIPPED)

        correlation_id = cloudevents.event_id(headers)
        payload = _parse_json(record.value)
        if payload is None:
            _LOG.warning("poison-payload (не JSON), ce-id=%s: пропуск", correlation_id)
            return HandleResult(ACTION_POISON)
        if await self._is_duplicate(correlation_id):
            _LOG.info("дубликат ce-id=%s: пропуск", correlation_id)
            return HandleResult(ACTION_DUPLICATE)

        try:
            request = events.parse_requested(payload, path=self._schema_path)
        except events.SchemaValidationError as exc:
            _LOG.warning("payload вне схемы (ce-id=%s): %s", correlation_id, exc)
            published = await self._reject_schema(payload, correlation_id, str(exc))
            return HandleResult(ACTION_DLQ, published)

        try:
            outcome = await self._pipeline.process(request, on_progress=self._on_progress)
        except Exception as exc:
            _LOG.exception("пайплайн упал на задании %s", request.request_id)
            failed = self._internal_failure(request, exc)
            action, published = await self._handle_failure(request, failed, correlation_id)
            return HandleResult(action, published)

        if isinstance(outcome, events.ComicTranslationCompleted):
            await self._publish(
                self._settings.results_topic,
                cloudevents.TYPE_COMPLETED,
                outcome.book_id,
                outcome.to_payload(),
            )
            await self._mark(correlation_id)
            return HandleResult(ACTION_COMPLETED, [self._settings.results_topic])

        action, published = await self._handle_failure(request, outcome, correlation_id)
        return HandleResult(action, published)

    # ── Исходы ────────────────────────────────────────────────────

    async def _handle_failure(
        self,
        request: events.ComicTranslationRequested,
        failed: events.ComicTranslationFailed,
        correlation_id: str | None,
    ) -> tuple[str, list[str]]:
        """Failed + (переопубликация задания | DLQ) — по retryable и лимиту попыток."""
        await self._publish(
            self._settings.results_topic,
            cloudevents.TYPE_FAILED,
            request.book_id,
            failed.to_payload(),
        )
        published = [self._settings.results_topic]
        if failed.retryable and request.retry_count < self._settings.max_retries:
            retried = request.model_copy(update={"retry_count": request.retry_count + 1})
            _LOG.info(
                "задание %s: повтор %d/%d",
                request.request_id,
                retried.retry_count,
                self._settings.max_retries,
            )
            await self._publish(
                self._settings.requests_topic,
                cloudevents.TYPE_REQUESTED,
                request.book_id,
                retried.to_payload(),
            )
            published.append(self._settings.requests_topic)
            await self._mark(correlation_id)
            return ACTION_FAILED, published

        reason = REASON_MAX_RETRIES if failed.retryable else REASON_NON_RETRYABLE
        await self._publish_dlq(
            book_id=request.book_id,
            language=request.language,
            reason=reason,
            error_code=failed.error_code,
            retry_count=request.retry_count,
            pages_done=failed.pages_done,
            correlation_id=correlation_id,
        )
        published.append(self._settings.dlq_topic)
        await self._mark(correlation_id)
        return ACTION_DLQ, published

    async def _reject_schema(
        self, payload: dict[str, Any], correlation_id: str | None, message: str
    ) -> list[str]:
        """Payload вне схемы: Failed(INTERNAL_ERROR) + DLQ SCHEMA_VALIDATION_FAILED.

        Диагностические события подчиняются тому же контракту: если из
        испорченного payload'а нельзя достать валидные `bookId`/`language`,
        публикация бессмысленна и неконтрактна — запись уходит в лог с `ce-id`
        (poison), консьюмеры её не увидят.
        """
        book_id = str(payload.get("bookId") or "")
        language = str(payload.get("language") or "")
        if not _is_diagnosable(book_id, language):
            _LOG.error(
                "payload вне схемы и не идентифицируем (ce-id=%s, bookId=%r, language=%r): "
                "Failed/DLQ не публикуются, причина: %s",
                correlation_id,
                book_id,
                language,
                _short(message),
            )
            await self._mark(correlation_id)
            return []
        failed = events.ComicTranslationFailed(
            book_id=book_id,
            language=language,
            failed_at=_now(),
            error_code=_INTERNAL_ERROR,
            retryable=False,
            pages_done=0,
            message=_short(message),
        )
        await self._publish(
            self._settings.results_topic,
            cloudevents.TYPE_FAILED,
            book_id,
            failed.to_payload(),
        )
        await self._publish_dlq(
            book_id=book_id,
            language=language,
            reason=REASON_SCHEMA,
            error_code=_INTERNAL_ERROR,
            retry_count=_retry_count(payload),
            pages_done=0,
            correlation_id=correlation_id,
        )
        await self._mark(correlation_id)
        return [self._settings.results_topic, self._settings.dlq_topic]

    def _internal_failure(
        self, request: events.ComicTranslationRequested, exc: Exception
    ) -> events.ComicTranslationFailed:
        """Непойманное исключение пайплайна превращается в retryable INTERNAL_ERROR."""
        return events.ComicTranslationFailed(
            book_id=request.book_id,
            language=request.language,
            failed_at=_now(),
            error_code=_INTERNAL_ERROR,
            retryable=True,
            pages_done=0,
            message=_short(exc),
        )

    # ── Публикация и дедуп ────────────────────────────────────────

    async def _on_progress(self, progress: events.ComicTranslationProgress) -> None:
        await self._publish(
            self._settings.results_topic,
            cloudevents.TYPE_PROGRESS,
            progress.book_id,
            progress.to_payload(),
        )

    async def _publish_dlq(
        self,
        *,
        book_id: str,
        language: str,
        reason: str,
        error_code: str | None,
        retry_count: int,
        pages_done: int,
        correlation_id: str | None,
    ) -> None:
        dlq = events.ComicTranslationDlq(
            book_id=book_id,
            language=language,
            reason=reason,
            error_code=error_code,
            retry_count=retry_count,
            pages_done=pages_done,
            event_id=correlation_id,
        )
        await self._publish(
            self._settings.dlq_topic,
            cloudevents.TYPE_DLQ,
            book_id,
            dlq.to_payload(),
        )

    async def _publish(self, topic: str, ce_type: str, subject: str, payload: Any) -> None:
        # ключ партиции = bookId (§4): события одной книги идут в одну партицию
        event = cloudevents.build_event(
            ce_type,
            source=cloudevents.SOURCE_WORKER,
            subject=subject,
            payload=payload,
            key=subject or None,
        )
        await self._publisher.publish(topic, event)

    async def _is_duplicate(self, correlation_id: str | None) -> bool:
        if not correlation_id or self._dedup is None:
            return False
        return await self._dedup.seen(correlation_id)

    async def _mark(self, correlation_id: str | None) -> None:
        if correlation_id and self._dedup is not None:
            await self._dedup.mark(correlation_id)


class RedisDedup:
    """Дедуп по `ce-id` в Redis: ключ `{prefix}{ce-id}`, TTL сутки (§2.16).

    Клиент передаётся снаружи (`redis.asyncio.Redis`), чтобы модуль логики
    консьюмера не тянул сетевые зависимости в тестах.
    """

    def __init__(
        self,
        client: Any,
        *,
        prefix: str = "translation-cache/dedup:",
        ttl_seconds: int = 86_400,
    ) -> None:
        self._client = client
        self._prefix = prefix
        self._ttl = ttl_seconds

    def key(self, event_id: str) -> str:
        return f"{self._prefix}{event_id}"

    async def seen(self, event_id: str) -> bool:
        return bool(await self._client.exists(self.key(event_id)))

    async def mark(self, event_id: str) -> None:
        await self._client.set(self.key(event_id), "1", ex=self._ttl)


def _parse_json(value: str | bytes) -> dict[str, Any] | None:
    try:
        payload = json.loads(value)
    except (TypeError, UnicodeDecodeError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def _retry_count(payload: dict[str, Any]) -> int:
    value = payload.get("retryCount")
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _now() -> datetime:
    return datetime.now(UTC)


def _short(message: Any) -> str:
    return " ".join(str(message).split())[:_MESSAGE_LIMIT]
