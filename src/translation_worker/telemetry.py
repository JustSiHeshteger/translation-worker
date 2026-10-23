"""Тайминги стадий и итоговая строка задания (карточка 55 §2.18).

В логи попадают только метрики (стадии, длительности, число страниц, код
ошибки) — без содержимого страниц: текст комикса не должен утекать в логи.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime

_LOG = logging.getLogger(__name__)


@dataclass
class StageMeasure:
    """Замер одной стадии конвейера."""

    stage: str
    status: str
    started_at: datetime
    finished_at: datetime
    duration_ms: int
    error_code: str | None = None


@dataclass
class StageTiming:
    """Сборщик таймингов: `with timing.measure("OCR"): ...` и `summary()`."""

    records: list[StageMeasure] = field(default_factory=list)

    def reset(self) -> None:
        """Начать сбор таймингов заново: замеры принадлежат ОДНОМУ заданию.

        Консьюмер живёт процессом целиком, поэтому без сброса итоговая строка
        растёт линейно с числом обработанных страниц и смешивает тайминги
        прошлых заданий.
        """
        self.records.clear()

    @contextmanager
    def measure(self, stage: str) -> Iterator[StageMeasure]:
        """Замерить стадию; статус `failed` и код ошибки при исключении."""
        started_at = datetime.now(UTC)
        record = StageMeasure(
            stage=stage,
            status="ok",
            started_at=started_at,
            finished_at=started_at,
            duration_ms=0,
        )
        self.records.append(record)
        try:
            yield record
        except Exception as exc:
            record.status = "failed"
            record.error_code = str(getattr(exc, "code", "INTERNAL_ERROR"))
            raise
        finally:
            record.finished_at = datetime.now(UTC)
            record.duration_ms = int((record.finished_at - started_at).total_seconds() * 1000)
            _LOG.debug(
                "стадия %s: %s за %d ms",
                record.stage,
                record.status,
                record.duration_ms,
            )

    def summary(self) -> str:
        """Компактная строка таймингов для итогового лога."""
        if not self.records:
            return "нет"
        return ", ".join(
            f"{record.stage}={record.duration_ms}ms/{record.status}" for record in self.records
        )


def log_task_summary(
    *,
    book_id: str,
    language: str,
    provider: str,
    pages_done: int,
    pages_total: int,
    duration_ms: int,
    error_code: str | None = None,
    timings: StageTiming | None = None,
) -> None:
    """Итоговая INFO-строка задания: без текста страниц, только метрики."""
    _LOG.info(
        "перевод книги %s (%s, %s): страниц %d/%d, %d ms, ошибка: %s, стадии: %s",
        book_id,
        language,
        provider,
        pages_done,
        pages_total,
        duration_ms,
        error_code or "нет",
        timings.summary() if timings is not None else "нет",
    )
