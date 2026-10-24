"""Точка входа: `python -m translation_worker` (карточка 55 §2.1).

Поднимает продюсер результатов, Kafka-консьюмер заданий, health-сервер для проб
и ждёт SIGTERM/SIGINT. Оффсет коммитится только после публикации исхода записи
(at-least-once); при остановке цикл consume отменяется, клиенты закрываются,
порт проб освобождается — под в k8s успевает погаситься до SIGKILL.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import signal
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import ConsumerRecord, TopicPartition
from redis.asyncio import Redis, from_url
from redis.exceptions import RedisError

from translation_worker import _contract, cloudevents
from translation_worker.cache import TranslationCache
from translation_worker.config import Settings
from translation_worker.kafka_consumer import (
    ACTION_BUSY,
    IncomingRecord,
    RedisDedup,
    TranslationConsumer,
)
from translation_worker.pipeline import Pipeline
from translation_worker.providers import build_ocr, build_renderer, build_translator
from translation_worker.storage import Storage
from translation_worker.telemetry import StageTiming

_LOG = logging.getLogger(__name__)

# Интервал проверки зависимостей для `/ready`.
_READY_REFRESH_SECONDS = 15.0

# Сколько ждать снятия резерва дедупа при остановке; меньше
# terminationGracePeriodSeconds в манифесте, чтобы под успел погаситься сам.
_SHUTDOWN_GRACE_SECONDS = 10.0

# Чанк ожидания чужого резерва: партиция на паузе, остальные продолжают течь.
_BUSY_WAIT_CHUNK_SECONDS = 15.0

# Бюджет ожидания чужого резерва: дольше TTL резерва ждать бессмысленно —
# после его истечения запись возьмёт кто угодно.
_busy_budget_override: float | None = None
_busy_wait: dict[tuple[TopicPartition, int], float] = {}


class _RecordBusy(Exception):
    """Запись занята другой репликой: оффсет коммитить нельзя."""


class _BusyUnresolved(Exception):
    """Занятость не разрешилась за бюджет ожидания: цикл останавливается."""


@dataclass
class DependencyState:
    """Состояние зависимостей для readiness-пробы `/ready`.

    Liveness `/health` зависимости намеренно игнорирует: рестарт живого воркера
    из-за недоступного Redis не помогает, а теряет уже сделанную работу.
    """

    # до первого подтверждения проба честно неготов: `_consume` поднимет
    # kafka, первый успешный PING — redis
    kafka: bool = False
    redis: bool = False

    def payload(self) -> dict[str, Any]:
        healthy = self.kafka and self.redis
        return {
            "status": "UP" if healthy else "DOWN",
            "dependencies": {
                "kafka": "ok" if self.kafka else "down",
                "redis": "ok" if self.redis else "down",
            },
        }

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
        inflight_ttl_seconds=settings.dedup_inflight_ttl_seconds,
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

    # Сбой старта не должен оставлять открытыми уже поднятые клиенты:
    # `finally` основного блока на этот момент ещё не активен.
    try:
        await producer.start()
        await consumer.start()
    except Exception:
        # `start()` упавшего клиента тоже надо остановить: aiokafka заводит
        # внутренние задачи ДО подключения, и без `stop()` asyncio пишет
        # «Unclosed AIOKafkaProducer» на штатном пути недоступного брокера
        for client in [consumer, producer]:
            with contextlib.suppress(Exception):
                await client.stop()
        with contextlib.suppress(Exception):
            await redis.aclose()
        raise
    dependencies = DependencyState()
    health: asyncio.AbstractServer | None = None
    tasks: list[asyncio.Task[Any]] = []
    consume_task: asyncio.Task[Any] | None = None
    consume_cancelled = False
    try:
        # сервер проб поднимается ВНУТРИ защищённого блока: занятый порт
        # (частый случай рестарта пода) иначе уводил бы из функции с
        # открытыми Kafka-клиентами и Redis
        health = await asyncio.start_server(
            lambda reader, writer: _serve_health(reader, writer, dependencies),
            host="0.0.0.0",
            port=settings.health_port,
        )
        _LOG.info(
            "translation-worker запущен: топик %s, группа %s, провайдеры ocr=%s translate=%s,"
            " порт проб %d",
            settings.requests_topic,
            settings.group_id,
            settings.ocr_provider,
            settings.translate_provider,
            settings.health_port,
        )
        consume_task = asyncio.create_task(_consume(consumer, handler, dependencies))
        ready_task = asyncio.create_task(_refresh_redis(redis, dependencies))
        stop_task = asyncio.create_task(stop.wait())
        tasks = [consume_task, ready_task, stop_task]
        done, _pending = await asyncio.wait(
            {consume_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
        )
        dependencies.kafka = False
        if consume_task in done:
            consume_task.result()
            _LOG.error("цикл consume завершился сам — останавливаем сервис")
            return 1
        # SIGTERM: задание не обрывается мгновенно — отмена доходит до
        # обработчика, который снимает резерв дедупа, и только потом клиенты
        # закрываются (release под shield обязан успеть)
        _LOG.info("получен сигнал остановки: прерываем текущую запись и снимаем резерв")
        consume_task.cancel()
        consume_cancelled = True
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await asyncio.wait({consume_task}, timeout=_SHUTDOWN_GRACE_SECONDS)
        return 0
    finally:
        for task in tasks:
            # уже отменённую задачу не трогаем: повторный CancelledError
            # оборвал бы shield-release резерва
            if task is consume_task and consume_cancelled:
                continue
            task.cancel()
        # отменённые задачи надо ДОЖДАТЬСЯ: иначе finally закроет Redis, пока
        # фоновый PING ещё в нём, и клиент упадёт на закрытом соединении
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if health is not None:
            health.close()
            with contextlib.suppress(Exception):
                await health.wait_closed()
        with contextlib.suppress(Exception):
            await consumer.stop()
        with contextlib.suppress(Exception):
            await producer.stop()
        with contextlib.suppress(Exception):
            await redis.aclose()


async def _consume(
    consumer: AIOKafkaConsumer,
    handler: TranslationConsumer,
    state: DependencyState | None = None,
) -> None:
    """Цикл consume: коммит оффсета — только после обработки и публикации исхода.

    Сбой публикации, дедупа или коммита не должен убивать процесс: платформа
    придерживается того же правила в `CommitEscape`/`AbstractEventConsumer`
    (таймаут + ограниченные повторы + WARN-продолжение). Запись, которую
    обработать не удалось, остаётся незакоммиченной и будет доставлена снова.
    """
    if state is not None:
        state.kafka = True
    try:
        await _consume_loop(consumer, handler)
    except _BusyUnresolved:
        # резерв чужой реплики жив дольше нашего бюджета: дальнейшая работа с
        # этой партией отравила бы оффсет, поэтому выходим и перечитываем
        # с последнего закоммиченного оффсета после рестарта
        _LOG.error("цикл consume остановлен: занятая запись не разрешилась")
    finally:
        if state is not None:
            state.kafka = False


async def _consume_loop(consumer: AIOKafkaConsumer, handler: TranslationConsumer) -> None:
    """Тело цикла consume (вынесено, чтобы флаг готовности сбрасывался в finally)."""
    async for record in consumer:
        incoming = _to_incoming(record)
        try:
            result = await handler.handle_record(incoming)
            if result.action == ACTION_BUSY:
                raise _RecordBusy
        except _RecordBusy:
            # Событие обрабатывает другая реплика. Оффсет коммитить НЕЛЬЗЯ:
            # коммит следующей записи той же партиции сдвинул бы позицию ЗА
            # занятую, и при падении владельца задание исчезло бы навсегда.
            # Поэтому партиция ставится на паузу и ждёт, пока резерв истечёт
            # (или владелец его снимет); ждём не дольше TTL резерва.
            partition = TopicPartition(record.topic, record.partition)
            waited = _busy_wait.get((partition, record.offset), 0.0)
            budget = _busy_budget()
            if waited >= budget:
                _LOG.error(
                    "запись %s/%s@%d: занята дольше %.0f с (бюджет): останавливаем цикл,"
                    " оффсет НЕ коммитим — задание вернётся после рестарта",
                    record.topic,
                    record.partition,
                    record.offset,
                    budget,
                )
                _busy_wait.pop((partition, record.offset), None)
                raise _BusyUnresolved from None
            pause_for = min(_BUSY_WAIT_CHUNK_SECONDS, budget - waited)
            _busy_wait[(partition, record.offset)] = waited + pause_for
            await consumer.pause(partition)
            try:
                await consumer.seek(partition, record.offset)
                _LOG.info(
                    "запись %s/%s@%d: занята другой репликой, пауза %.0f с (%.0f/%.0f)",
                    record.topic,
                    record.partition,
                    record.offset,
                    pause_for,
                    waited + pause_for,
                    budget,
                )
                await asyncio.sleep(pause_for)
            finally:
                await consumer.resume(partition)
            continue
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
        # успешная запись снимает свою историю повторов: иначе модульный
        # счётчик растёт на долгоживущем поде
        _retries.pop((TopicPartition(record.topic, record.partition), record.offset), None)
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


async def _serve_health(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    state: DependencyState,
) -> None:
    """Пробы сервиса: `/health` — liveness, `/ready` — готовность зависимостей."""
    try:
        # молчащий клиент не должен оставлять висящую корутину: проба
        # отваливается по timeoutSeconds, а корутина жила бы до закрытия
        request = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5)
    except (TimeoutError, asyncio.IncompleteReadError, asyncio.LimitOverrunError, OSError):
        request = b""
    path = _request_path(request)
    if path == "/ready":
        payload: dict[str, Any] = state.payload()
        code, reason = (200, "OK") if payload["status"] == "UP" else (503, "Service Unavailable")
    elif path == "/health":
        payload, code, reason = {"status": "UP"}, 200, "OK"
    else:
        payload, code, reason = {"status": "NOT_FOUND"}, 404, "Not Found"
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    head = (
        f"HTTP/1.1 {code} {reason}\r\n"
        "Content-Type: application/json\r\n"
        "Connection: close\r\n"
        f"Content-Length: {len(body)}\r\n\r\n"
    ).encode("ascii")
    with contextlib.suppress(OSError):
        writer.write(head + body)
        await writer.drain()
    writer.close()
    with contextlib.suppress(OSError):
        await writer.wait_closed()


async def _refresh_redis(
    redis: Redis,
    state: DependencyState,
    interval: float = _READY_REFRESH_SECONDS,
) -> None:
    """Держать readiness в актуальном состоянии: PING раз в N секунд.

    Проба не должна ходить в сеть на каждый запрос (k8s опрашивает часто),
    а `/ready` обязан замечать пропажу Redis в пределах пары интервалов.
    """
    while True:
        try:
            await asyncio.wait_for(redis.ping(), timeout=5)
            state.redis = True
        except (TimeoutError, OSError, RedisError):
            # недоступность Redis — «не готов»; программные ошибки (AttributeError)
            # не глушим, иначе поломка кода выглядела бы как падение зависимости
            state.redis = False
        await asyncio.sleep(interval)


def _busy_budget() -> float:
    """Сколько секунд ждать разрешения чужого резерва (TTL резерва)."""
    if _busy_budget_override is not None:
        return _busy_budget_override
    try:
        from translation_worker.config import Settings

        return float(Settings().dedup_inflight_ttl_seconds)
    except Exception:  # noqa: BLE001 - конфиг недоступен: безопасный минимум
        return 300.0


def _request_path(request: bytes) -> str:
    """Путь из первой строки HTTP-запроса (`GET /ready HTTP/1.1`)."""
    line = request.split(b"\r\n", 1)[0]
    parts = line.split(b" ")
    if len(parts) < 2:
        return ""
    return parts[1].decode("utf-8", errors="replace").split("?", 1)[0]


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
