"""Payload'ы событий перевода и их валидация по контракту (карточка 55 §2.4).

Модели — дословное зеркало `components.schemas` из
`asyncapi/comic-translation-events.yaml` (карточка 54): snake_case в Python,
camelCase в payload'е (alias), null-поля присутствуют в `to_payload()`.
Входящее задание валидируется по схеме контракта (`jsonschema`) до разбора в
модель, поэтому «почти правильный» payload уходит в DLQ, а не в конвейер.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import jsonschema
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from translation_worker import _contract

SCHEMA_REQUESTED = "ComicTranslationRequestedPayload"
SCHEMA_PROGRESS = "ComicTranslationProgressPayload"
SCHEMA_COMPLETED = "ComicTranslationCompletedPayload"
SCHEMA_FAILED = "ComicTranslationFailedPayload"
SCHEMA_DLQ = "ComicTranslationDlqPayload"


class SchemaValidationError(ValueError):
    """Payload не соответствует схеме контракта событий."""


class EventPayload(BaseModel):
    """Общая база payload'ов: camelCase в контракте, snake_case в коде."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    def to_payload(self) -> dict[str, Any]:
        """Payload в формате контракта (camelCase, null-поля на месте)."""
        return self.model_dump(by_alias=True, mode="json")


# ── Задание на перевод ──────────────────────────────────────────────


class ComicPageSource(EventPayload):
    """Страница-изображение, которую нужно перевести."""

    number: int
    url: str


class GlossaryTerm(EventPayload):
    """Термин глоссария: `from` — в оригинале, `to` — устоявшийся перевод."""

    from_: str = Field(alias="from")
    to: str


class ComicTranslationRequested(EventPayload):
    """Задание перевода книги (публикует book-service)."""

    book_id: str = Field(alias="bookId")
    book_slug: str | None = Field(default=None, alias="bookSlug")
    language: str
    source_language: str | None = Field(default=None, alias="sourceLanguage")
    provider: str
    pages: list[ComicPageSource] = Field(default_factory=list)
    glossary: list[GlossaryTerm] = Field(default_factory=list)
    requested_by: str | None = Field(default=None, alias="requestedBy")
    request_id: str = Field(alias="requestId")
    retry_count: int = Field(default=0, alias="retryCount")
    created_at: datetime = Field(alias="createdAt")

    def glossary_map(self) -> dict[str, str]:
        """Глоссарий в виде словаря — в таком виде его ждёт переводчик."""
        return {term.from_: term.to for term in self.glossary}


# ── Прогресс, результат, ошибка, DLQ ────────────────────────────────


class ComicTranslationProgress(EventPayload):
    """Прогресс обработки задания (не чаще `progress_interval`)."""

    book_id: str = Field(alias="bookId")
    language: str
    stage: str
    pages_done: int = Field(default=0, alias="pagesDone")
    pages_total: int = Field(default=0, alias="pagesTotal")
    percent: int = 0
    message: str | None = None


class ComicTranslationPage(EventPayload):
    """Результат перевода одной страницы."""

    number: int
    layer_key: str = Field(alias="layerKey")
    image_key: str | None = Field(default=None, alias="imageKey")
    blocks: int = 0
    warnings: list[str] = Field(default_factory=list)


class ComicTranslationCompleted(EventPayload):
    """Перевод книги завершён: слои всех страниц в S3."""

    book_id: str = Field(alias="bookId")
    language: str
    completed_at: datetime = Field(alias="completedAt")
    pages_done: int = Field(default=0, alias="pagesDone")
    pages_total: int = Field(default=0, alias="pagesTotal")
    layer_base_key: str = Field(default="", alias="layerBaseKey")
    pages: list[ComicTranslationPage] = Field(default_factory=list)
    provider: str = ""
    duration_ms: int = Field(default=0, alias="durationMs")
    warnings: list[str] = Field(default_factory=list)


class ComicTranslationFailed(EventPayload):
    """Ошибка обработки задания."""

    book_id: str = Field(alias="bookId")
    language: str
    failed_at: datetime = Field(alias="failedAt")
    error_code: str = Field(alias="errorCode")
    retryable: bool = True
    pages_done: int = Field(default=0, alias="pagesDone")
    failed_page: int | None = Field(default=None, alias="failedPage")
    message: str | None = None


class ComicTranslationDlq(EventPayload):
    """Задание в dead letter queue: ретраи исчерпаны или ошибка неисправима."""

    book_id: str = Field(alias="bookId")
    language: str
    reason: str
    error_code: str | None = Field(default=None, alias="errorCode")
    retry_count: int = Field(default=0, alias="retryCount")
    pages_done: int = Field(default=0, alias="pagesDone")
    event_id: str | None = Field(default=None, alias="eventId")


# ── Работа со схемой контракта ──────────────────────────────────────


def load_component_schema(name: str, path: str | Path | None = None) -> dict[str, Any]:
    """Вернуть схему `components.schemas.<name>` контракта событий."""
    try:
        return _contract.component_schema(name, path)
    except _contract.ContractError as exc:
        raise SchemaValidationError(str(exc)) from exc


def validate_payload(payload: Any, schema_name: str, path: str | Path | None = None) -> None:
    """Проверить payload по схеме контракта; ошибка → SchemaValidationError."""
    schema = load_component_schema(schema_name, path)
    try:
        jsonschema.validate(instance=payload, schema=schema)
    except jsonschema.ValidationError as exc:
        location = "/".join(str(part) for part in exc.absolute_path) or "<root>"
        raise SchemaValidationError(f"{schema_name}: {location}: {exc.message}") from exc
    except jsonschema.SchemaError as exc:
        raise SchemaValidationError(f"{schema_name}: схема невалидна: {exc.message}") from exc


def parse_requested(
    payload: Any, *, path: str | Path | None = None
) -> ComicTranslationRequested:
    """Провалидировать payload задания по контракту и разобрать в модель."""
    validate_payload(payload, SCHEMA_REQUESTED, path)
    try:
        return ComicTranslationRequested.model_validate(payload)
    except ValidationError as exc:
        raise SchemaValidationError(f"{SCHEMA_REQUESTED}: {exc}") from exc
