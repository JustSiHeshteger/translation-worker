"""Цикл consume: сбой обработки не должен терять запись (ревью round 2, H7).

`at-least-once` конвейера держится на двух правилах: оффсет коммитится только
после публикации исхода, и упавшая запись не «перепрыгивается» коммитом
следующей записи той же партиции — иначе задание исчезает без DLQ.
"""

from __future__ import annotations

import asyncio

import pytest
from aiokafka.structs import ConsumerRecord, TopicPartition

from translation_worker import __main__ as entrypoint
from translation_worker.kafka_consumer import HandleResult, IncomingRecord

TOPIC = "comic-translations.requests"
BOOK_ID = b"018f0000-0000-7000-8000-000000000001"


class _FakeConsumer:
    """Консьюмер-двойник с семантикой `seek`: позиция откатывается к оффсету.

    Как и настоящий aiokafka, после `seek` следующий poll отдаёт запись с этого
    оффсета снова, поэтому «пропуск» упавшей записи здесь был бы виден.
    """

    def __init__(self, records: list[ConsumerRecord]) -> None:
        self._records = list(records)
        self.commits: list[dict[TopicPartition, int]] = []
        self.seeks: list[tuple[TopicPartition, int]] = []

    def __aiter__(self) -> _FakeConsumer:
        return self

    async def __anext__(self) -> ConsumerRecord:
        if not self._records:
            raise StopAsyncIteration
        return self._records.pop(0)

    async def commit(self, offsets: dict[TopicPartition, int]) -> None:
        self.commits.append(dict(offsets))

    async def seek(self, partition: TopicPartition, offset: int) -> None:
        self.seeks.append((partition, offset))
        # повторная доставка: запись с этого оффсета снова в голове очереди
        self._records.insert(0, _record(offset, partition.partition))


class _FlakyHandler:
    """Первые `failures` попыток падают, дальше — успех."""

    def __init__(self, failures: int = 1) -> None:
        self.failures = failures
        self.calls = 0

    async def handle_record(self, record: IncomingRecord) -> HandleResult:
        self.calls += 1
        if self.calls <= self.failures:
            raise RuntimeError("брокер недоступен")
        return HandleResult("completed", [TOPIC])


def _record(offset: int, partition: int = 0) -> ConsumerRecord:
    return ConsumerRecord(
        topic=TOPIC,
        partition=partition,
        offset=offset,
        timestamp=0,
        timestamp_type=0,
        key=BOOK_ID,
        value=b"{}",
        headers=None,
        checksum=None,
        serialized_key_size=-1,
        serialized_value_size=-1,
    )


async def _no_sleep(_seconds: float) -> None:
    """Бэкофф в тесте не нужен."""


def test_failed_record_is_retried_from_same_offset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(entrypoint.asyncio, "sleep", _no_sleep)
    consumer = _FakeConsumer([_record(0)])
    handler = _FlakyHandler(failures=1)

    asyncio.run(entrypoint._consume(consumer, handler))  # type: ignore[arg-type]

    partition = TopicPartition(TOPIC, 0)
    assert consumer.seeks == [(partition, 0)]
    # коммит ровно на одну запись вперёд: упавшая была обработана повторно,
    # а не перепрыгнута
    assert consumer.commits == [{partition: 1}]
    assert handler.calls == 2


def test_successful_record_is_committed_once() -> None:
    consumer = _FakeConsumer([_record(7)])
    handler = _FlakyHandler(failures=0)

    asyncio.run(entrypoint._consume(consumer, handler))  # type: ignore[arg-type]

    assert consumer.commits == [{TopicPartition(TOPIC, 0): 8}]
    assert consumer.seeks == []


def test_commit_failure_does_not_kill_the_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(entrypoint.asyncio, "sleep", _no_sleep)
    consumer = _FakeConsumer([_record(0, partition=1)])
    handler = _FlakyHandler(failures=0)

    async def _broken_commit(_offsets: dict[TopicPartition, int]) -> None:
        raise RuntimeError("CommitFailedError")

    consumer.commit = _broken_commit  # type: ignore[method-assign,assignment]

    asyncio.run(entrypoint._consume(consumer, handler))  # type: ignore[arg-type]

    assert handler.calls == 1  # цикл дошёл до конца, процесс не упал
