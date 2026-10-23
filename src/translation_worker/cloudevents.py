"""CloudEvents 1.0 в binary content mode (карточка 55 §2.5).

Payload — тело Kafka-сообщения (JSON), CE-атрибуты — record headers (`ce-*`),
как в upload-worker. Источник (`ce-source`) задаёт вызывающий: задания
публикует book-service (`/book-service`), результаты, DLQ и переопубликованное
задание ретрая — сам воркер (`/translation-worker`, §4 карточки).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

TYPE_REQUESTED = "com.books.platform.comic-translation.requested.v1"
TYPE_PROGRESS = "com.books.platform.comic-translation.progress.v1"
TYPE_COMPLETED = "com.books.platform.comic-translation.completed.v1"
TYPE_FAILED = "com.books.platform.comic-translation.failed.v1"
TYPE_DLQ = "com.books.platform.comic-translation.dlq.v1"

SOURCE_BOOK_SERVICE = "/book-service"
SOURCE_WORKER = "/translation-worker"

SPECVERSION = "1.0"


@dataclass(frozen=True)
class CloudEvent:
    """Готовое к отправке сообщение: CE-атрибуты в headers, JSON в value.

    `key` — ключ партиции Kafka (для событий конвейера всегда `bookId`):
    порядок progress/completed/failed одной книги обязан сохраняться, а
    повторно опубликованное задание — попадать на ту же партицию (§4 карточки).
    """

    headers: dict[str, str]
    value: bytes
    key: str | None = None


def build_event(
    ce_type: str,
    *,
    source: str,
    subject: str,
    payload: Any,
    dataschema: str | None = None,
    event_id: str | None = None,
    time: datetime | None = None,
    key: str | None = None,
    specversion: str = SPECVERSION,
) -> CloudEvent:
    """Собрать CloudEvent: обязательные `ce-*` заголовки + JSON-payload + ключ партиции."""
    headers = {
        "ce-specversion": specversion,
        "ce-type": ce_type,
        "ce-source": source,
        "ce-id": event_id or new_event_id(),
        "ce-time": format_time(time or datetime.now(UTC)),
        "ce-subject": subject,
    }
    if dataschema:
        headers["ce-dataschema"] = dataschema
    return CloudEvent(headers=headers, value=dumps_payload(payload), key=key)


def dumps_payload(payload: Any) -> bytes:
    """Детерминированная JSON-сериализация payload'а (UTF-8, без ASCII-escape)."""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")


def new_event_id() -> str:
    """Новый `ce-id` для идемпотентности (dedup в Redis, TTL 24h)."""
    return f"evt_{uuid4().hex}"


def format_time(moment: datetime) -> str:
    """UTC-время события в формате контракта: `2026-10-12T09:00:00.000Z`."""
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    moment = moment.astimezone(UTC)
    return f"{moment:%Y-%m-%dT%H:%M:%S}.{moment.microsecond // 1000:03d}Z"


def event_id(headers: dict[str, str]) -> str | None:
    """`ce-id` входящего сообщения (None, если заголовка нет)."""
    return headers.get("ce-id") if headers else None


def event_type(headers: dict[str, str]) -> str | None:
    """`ce-type` входящего сообщения (None, если заголовка нет)."""
    return headers.get("ce-type") if headers else None
