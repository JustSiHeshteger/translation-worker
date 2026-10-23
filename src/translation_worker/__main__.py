"""Точка входа: `python -m translation_worker` (карточка 55 §2.1).

Поднимает продюсер результатов, Kafka-консьюмер заданий, health-сервер для проб
и ждёт SIGTERM/SIGINT. Оффсет коммитится только после публикации исхода записи
(at-least-once); при остановке цикл consume отменяется, клиенты закрываются,
порт проб освобождается — под в k8s успевает погаситься до SIGKILL.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from pathlib import Path
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import ConsumerRecord, TopicPartition
from redis.asyncio import Redis, from_url

from translation_worker import _contract, cloudevents
from translation_worker.cache import TranslationCache
from translation_worker.config import Settings
from translation_worker.kafka_consumer import (
    IncomingRecord,
    RedisDedup,
    TranslationConsumer,
)
from translation_worker.pipeline import Pipeline
from translation_worker.providers import build_ocr, build_renderer, build_translator
from translation_worker.storage import Storage
from translation_worker.telemetry import StageTiming

_LOG = logging.getLogger(__name__)

_HEALTH_BODY = b'{"status":"UP"}'

# Предел повторов обработки одной записи перед осознанным пропуском:
# без него устойчивый сбой публикации/Redis заклинил бы партицию навсегда.
_MAX_RECORD_ATTEMPTS = 5

# Попытки по (партиция, оффсет) — состояние живёт вместе с циклом consume.
_retries: dict[tuple[TopicPartition, int], int] = {}


class KafkaPublisher:
    """Продюсер CloudEvents: атрибуты `ce-*` в record headers (binary mode)."""

    def __init__(self, producer: AIOKafkaProducer) -> None:
        self._producer = producer

    async def publish(self, topic: str, event: cloudevents.CloudEvent) -> None:
        headers = [(name, value.encode("utf-8")) for name, value in event.headers.items()]
        # ключ партиции (bookId) — порядок событий одной книги и повторные
        # задания обязаны попадать в одну партицию (§4 карточки 55)
        key = event.key.encode("utf-8") if event.key else None
        await self._producer.send_and_wait(topic, value=event.value, key=key, headers=headers)


async def _main() -> int:
    settings = Settings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    stop = asyncio.Event()
    _install_signal_handlers(stop)

    storage = Storage(settings)
    cache = TranslationCache(storage, settings)
    timings = StageTiming()
    pipeline = Pipeline(
        settings,
        storage,
        build_ocr(settings),
        build_translator(settings),
        cache,
        renderer=build_renderer(settings),
        timings=timings,
    )
    # таймауты обязательны: зависший Redis не должен стопорить цикл consume
    redis: Redis = from_url(settings.redis_url, socket_connect_timeout=5, socket_timeout=5)
    dedup = RedisDedup(
        redis,
        prefix=settings.dedup_prefix,
        ttl_seconds=settings.dedup_ttl_seconds,
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap,
        # результат задания не должен теряться на стороне брокера: offset
        # коммитится после публикации, поэтому acks=all + идемпотентность
        acks="all",
        enable_idempotence=True,
    )
    consumer = AIOKafkaConsumer(
        settings.requests_topic,
        bootstrap_servers=settings.kafka_bootstrap,
        group_id=settings.group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_interval_ms=settings.max_poll_interval_ms,
    )
    schema_path = _resolve_schema_path(settings.schema_path)
    # fail fast: без читаемого контракта любое задание уходило бы в
    # DLQ SCHEMA_VALIDATION_FAILED при зелёном /health
    try:
        _contract.load_document(schema_path)
        # наличие самой схемы задания: переименование в контракте иначе
        # снова увело бы все задания в DLQ при зелёном /health
        _contract.component_schema("ComicTranslationRequestedPayload", schema_path)
    except _contract.ContractError as exc:
        raise SystemExit(f"translation-worker: контракт {schema_path} не читается: {exc}") from exc
    handler = TranslationConsumer(
        settings,
        pipeline,
        KafkaPublisher(producer),
        dedup=dedup,
        schema_path=schema_path,
    )

    await producer.start()
    await consumer.start()
    health = await asyncio.start_server(_serve_health, host="0.0.0.0", port=settings.health_port)
    _LOG.info(
        "translation-worker запущен: топик %s, группа %s, провайдеры ocr=%s translate=%s,"
        " порт проб %d",
        settings.requests_topic,
        settings.group_id,
        settings.ocr_provider,
        settings.translate_provider,
        settings.health_port,
    )
    try:
        consume_task = asyncio.create_task(_consume(consumer, handler))
        stop_task = asyncio.create_task(stop.wait())
        done, _pending = await asyncio.wait(
            {consume_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if consume_task in done:
            consume_task.result()
            _LOG.error("цикл consume завершился сам — останавливаем сервис")
            stop_task.cancel()
            return 1
        _LOG.info("получен сигнал остановки: дожидаемся завершения текущей записи")
        consume_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await consume_task
        return 0
    finally:
        health.close()
        await health.wait_closed()
        await consumer.stop()
        await producer.stop()
        await redis.aclose()


async def _consume(
    consumer: AIOKafkaConsumer, handler: TranslationConsumer
) -> None:
    """Цикл consume: коммит оффсета — только после обработки и публикации исхода.

    Сбой публикации, дедупа или коммита не должен убивать процесс: платформа
    придерживается того же правила в `CommitEscape`/`AbstractEventConsumer`
    (таймаут + ограниченные повторы + WARN-продолжение). Запись, которую
    обработать не удалось, остаётся незакоммиченной и будет доставлена снова.
    """
    async for record in consumer:
        incoming = _to_incoming(record)
        try:
            result = await handler.handle_record(incoming)
        except Exception:
            # Оффсет не коммитим: следующая запись той же партиции
            # перепрыгнула бы упавшую. Отматываем позицию назад и пробуем
            # снова с бэкоффом; после исчерпания попыток запись пропускается
            # осознанно (партиция не должна встать навсегда).
            partition = TopicPartition(record.topic, record.partition)
            attempts = _retries.get((partition, record.offset), 0) + 1
            _LOG.exception(
                "запись %s/%s@%d: обработка упала (попытка %d/%d), оффсет не коммитится",
                record.topic,
                record.partition,
                record.offset,
                attempts,
                _MAX_RECORD_ATTEMPTS,
            )
            if attempts < _MAX_RECORD_ATTEMPTS:
                _retries[(partition, record.offset)] = attempts
                await consumer.seek(partition, record.offset)
                await asyncio.sleep(min(2**attempts, 30))
                continue
            _LOG.error(
                "запись %s/%s@%d: пропущена после %d попыток",
                record.topic,
                record.partition,
                record.offset,
                attempts,
            )
            _retries.pop((partition, record.offset), None)
        try:
            await consumer.commit(
                {TopicPartition(record.topic, record.partition): record.offset + 1}
            )
        except Exception:
            # сессия могла истёкнуть (ребаланс) — следующий poll вернёт запись снова
            _LOG.warning(
                "запись %s/%s@%d: коммит оффсета не удался, продолжаем цикл",
                record.topic,
                record.partition,
                record.offset,
                exc_info=True,
            )
            continue
        _LOG.debug(
            "запись %s/%s@%d: %s, топики: %s",
            record.topic,
            record.partition,
            record.offset,
            result.action,
            ", ".join(result.published) or "нет",
        )


def _resolve_schema_path(configured: str) -> str:
    """Путь к контракту событий: сначала настройка, затем монорепо.

    В образ файл кладёт Dockerfile (`schemas/...`), при локальном запуске из
    чекаута контракт лежит в `books-platform-openapi/asyncapi/`. Если не найден
    ни один вариант — падаем на старте: иначе каждое валидное задание уходит
    в DLQ SCHEMA_VALIDATION_FAILED при зелёном `/health`.
    """
    candidates = [
        Path(configured),
        Path(__file__).resolve().parents[3]
        / "books-platform-openapi"
        / "asyncapi"
        / "comic-translation-events.yaml",
    ]
    for candidate in candidates:
        if candidate.is_file():
            return str(candidate)
    raise SystemExit(
        "translation-worker: контракт событий не найден (ни "
        + str(candidates[0])
        + ", ни "
        + str(candidates[1])
        + "); укажите TRANSLATION_SCHEMA_PATH"
    )


def _to_incoming(record: ConsumerRecord) -> IncomingRecord:
    """Запись aiokafka → запись логики консьюмера (заголовки как строки)."""
    headers: dict[str, str] = {}
    for name, value in record.headers or ():
        headers[name] = (value or b"").decode("utf-8", errors="replace")
    key = record.key.decode("utf-8", errors="replace") if record.key else None
    return IncomingRecord(key=key, value=record.value, headers=headers)


async def _serve_health(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Минимальный HTTP-ответ пробе: GET /health → 200 {"status":"UP"}."""
    try:
        await reader.readuntil(b"\r\n\r\n")
    except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, OSError):
        pass
    head = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: application/json\r\n"
        b"Connection: close\r\n"
        b"Content-Length: " + str(len(_HEALTH_BODY)).encode("ascii") + b"\r\n\r\n"
    )
    with contextlib.suppress(OSError):
        writer.write(head + _HEALTH_BODY)
        await writer.drain()
    writer.close()
    with contextlib.suppress(OSError):
        await writer.wait_closed()


def _install_signal_handlers(stop: asyncio.Event) -> None:
    """SIGTERM/SIGINT → graceful shutdown (на Windows add_signal_handler нет)."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            signal.signal(sig, _stop_handler(stop))


def _stop_handler(stop: asyncio.Event) -> Any:
    def _handler(_signum: int, _frame: Any) -> None:
        stop.set()

    return _handler


def main() -> int:
    """Синхронная обёртка для `python -m translation_worker`."""
    return asyncio.run(_main())


if __name__ == "__main__":
    raise SystemExit(main())
