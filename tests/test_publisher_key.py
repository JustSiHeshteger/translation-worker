"""Ключ партиции в событиях конвейера (карточка 55 §4, ревью round 1).

Progress/completed/failed одной книги обязаны идти в одну партицию, иначе
book-service может увидеть `completed` раньше `progress`, а повторно
опубликованное задание уйдёт на чужую партицию и при нескольких репликах
книга обработается параллельно дважды.
"""

from __future__ import annotations

import asyncio

from translation_worker import cloudevents
from translation_worker.__main__ import KafkaPublisher

BOOK_ID = "018f0000-0000-7000-8000-000000000001"


class _RecordingProducer:
    """Продюсер-двойник: запоминает аргументы `send_and_wait`."""

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def send_and_wait(self, topic: str, **kwargs: object) -> None:
        self.calls.append({"topic": topic, **kwargs})


def test_publish_sends_book_id_as_partition_key() -> None:
    producer = _RecordingProducer()
    publisher = KafkaPublisher(producer)  # type: ignore[arg-type]
    event = cloudevents.build_event(
        cloudevents.TYPE_PROGRESS,
        source=cloudevents.SOURCE_WORKER,
        subject=BOOK_ID,
        payload={"bookId": BOOK_ID},
        key=BOOK_ID,
    )

    asyncio.run(publisher.publish("comic-translations.results", event))

    assert producer.calls[0]["key"] == BOOK_ID.encode("utf-8")


def test_publish_without_key_sends_none() -> None:
    producer = _RecordingProducer()
    publisher = KafkaPublisher(producer)  # type: ignore[arg-type]
    event = cloudevents.build_event(
        cloudevents.TYPE_DLQ,
        source=cloudevents.SOURCE_WORKER,
        subject="",
        payload={"bookId": ""},
    )

    asyncio.run(publisher.publish("comic-translations.dlq", event))

    assert producer.calls[0]["key"] is None


def test_cloud_event_carries_key() -> None:
    event = cloudevents.build_event(
        cloudevents.TYPE_COMPLETED,
        source=cloudevents.SOURCE_WORKER,
        subject=BOOK_ID,
        payload={},
        key=BOOK_ID,
    )

    assert event.key == BOOK_ID
    assert event.headers["ce-subject"] == BOOK_ID
