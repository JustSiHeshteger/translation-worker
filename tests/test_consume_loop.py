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
        self._seen = {record.offset: record for record in records}
        self.commits: list[dict[TopicPartition, int]] = []
        self.seeks: list[tuple[TopicPartition, int]] = []
        self.paused: list[TopicPartition] = []
        self.resumed: list[TopicPartition] = []

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
        # повторная доставка: запись с этого оффсета снова в голове очереди,
        # с исходными value/headers (иначе фейк проверял бы не тот payload)
        self._records.insert(0, self._seen[offset])

    async def pause(self, *partitions: TopicPartition) -> None:
        self.paused.extend(partitions)

    async def resume(self, *partitions: TopicPartition) -> None:
        self.resumed.extend(partitions)


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


@pytest.fixture(autouse=True)
def _fresh_retries() -> None:
    """Счётчик повторов — модульный: тесты не должны видеть чужие попытки."""
    entrypoint._retries.clear()
    entrypoint._busy_wait.clear()


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

async def test_kafka_flag_drops_when_loop_ends():
    """Готовность не должна врать после выхода цикла consume (LOW 7)."""
    state = entrypoint.DependencyState()
    consumer = _FakeConsumer([])

    await entrypoint._consume(consumer, _FlakyHandler(failures=0), state)  # type: ignore[arg-type]

    assert state.kafka is False

class _BusyHandler:
    """Событие всегда «в работе» у другой реплики."""

    async def handle_record(self, record: IncomingRecord) -> HandleResult:
        return HandleResult("busy")


def test_busy_record_pauses_partition_and_never_commits(monkeypatch: pytest.MonkeyPatch) -> None:
    """Занятое чужой репликой нельзя ни коммитить, ни обгонять соседней записью."""
    monkeypatch.setattr(entrypoint.asyncio, "sleep", _no_sleep)
    monkeypatch.setattr(entrypoint, "_BUSY_WAIT_CHUNK_SECONDS", 15.0)
    monkeypatch.setattr(entrypoint, "_busy_budget_override", 15.0)
    # следующая запись партиции готова: без паузы её коммит сдвинул бы позицию
    # за занятую запись, и задание владельца исчезло бы при его падении
    consumer = _FakeConsumer([_record(0), _record(1)])

    asyncio.run(entrypoint._consume(consumer, _BusyHandler()))  # type: ignore[arg-type]

    assert consumer.commits == [], "ни одна запись партиции не коммитится, пока занятость не снята"
    assert consumer.seeks, "позиция отматывается на занятую запись"
    assert consumer.paused, "партиция ставится на паузу, чтобы не обгонять занятую запись"
    assert consumer.resumed, "пауза снимается после ожидания"

def test_retry_history_is_cleared_after_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Счётчик повторов не должен расти на долгоживущем поде."""
    monkeypatch.setattr(entrypoint.asyncio, "sleep", _no_sleep)
    consumer = _FakeConsumer([_record(0)])
    handler = _FlakyHandler(failures=1)

    asyncio.run(entrypoint._consume(consumer, handler))  # type: ignore[arg-type]

    assert entrypoint._retries == {}