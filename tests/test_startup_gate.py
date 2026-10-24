"""Старт с гейтом по готовности: под жив, /ready врать не должен.

Правило платформы (карточка 55 §2.1, долг «readiness не гейтит потребление»):
недоступный брокер — это «не готов», а не «процесс умер». Пока Kafka нет,
`/health` обязан отвечать 200 (иначе k8s начнёт убивать под), `/ready` — 503
(трафик не идёт), а подключение повторяется с backoff. Флаг `kafka` в
`/ready` поднимается ТОЛЬКО после ответа брокера, а не при входе в цикл.
"""

from __future__ import annotations

import asyncio

import pytest

import translation_worker.__main__ as entrypoint
from translation_worker.kafka_consumer import HandleResult, IncomingRecord


class _Client:
    """Kafka-клиент одной попытки подключения."""

    def __init__(self, *, fail: bool) -> None:
        self.fail = fail
        self.starts = 0
        self.stops = 0

    async def start(self) -> None:
        self.starts += 1
        if self.fail:
            raise OSError("broker is not reachable")

    async def stop(self) -> None:
        self.stops += 1


class _Factory:
    """Фабрика клиентов: попытка = пара (продюсер, консюмер) с общим исходом."""

    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.attempt = 0
        self.created: list[_Client] = []

    def __call__(self) -> _Client:
        # первым в попытке создаётся продюсер — по нему и считаем попытку
        if self.created and len(self.created) % 2 == 0:
            self.attempt += 1
        client = _Client(fail=self.attempt < self.failures)
        self.created.append(client)
        return client


def test_startup_retries_until_broker_answers() -> None:
    factory = _Factory(failures=2)

    result = asyncio.run(
        entrypoint._connect_clients(
            factory, factory, stop=asyncio.Event(), retry_delay=0.0
        )
    )

    assert result is not None, "воркер обязан дождаться брокера, а не упасть"
    assert factory.attempt == 2, "две неудачные попытки, успех на третьей"
    assert len(factory.created) == 6, "на каждую попытку — свои клиенты"
    stopped = [client for client in factory.created if client.stops]
    assert len(stopped) == 4, "упавшие клиенты закрываются, иначе текут соединения"
    producer, consumer = result
    assert (producer.starts, consumer.starts) == (1, 1), "успешные клиенты стартуют один раз"
    assert not producer.fail and not consumer.fail


def test_startup_gives_up_only_on_stop_signal() -> None:
    stop = asyncio.Event()
    factory = _Factory(failures=99)
    original = factory.__call__

    def factory_with_stop() -> _Client:
        client = original()
        if len(factory.created) >= 2:
            stop.set()
        return client

    result = asyncio.run(
        entrypoint._connect_clients(
            factory_with_stop, factory_with_stop, stop=stop, retry_delay=0.0
        )
    )

    assert result is None, "без сигнала остановки подключение не прекращается"


def test_startup_does_not_leak_clients_when_stopped_midway() -> None:
    stop = asyncio.Event()
    stop.set()
    factory = _Factory(failures=1)

    result = asyncio.run(
        entrypoint._connect_clients(factory, factory, stop=stop, retry_delay=0.0)
    )

    assert result is None
    assert factory.created == [], "после сигнала остановки новые клиенты не создаются"


class _Admin:
    """Админ-клиент брокера: метаданные то отвечают, то падают.

    Набор методов намеренно узкий — ровно публичный API aiokafka, которым
    пользуется зонд. Ошибка прошлой реализации была именно в выдуманном
    методе (`list_topics` есть только у админ-клиента, у консьюмера его нет).
    """

    def __init__(self, results: list[object], default: object | None = None) -> None:
        self.results = results
        self.default = default
        self.calls = 0
        self.starts = 0
        self.closes = 0

    async def start(self) -> None:
        self.starts += 1

    async def close(self) -> None:
        self.closes += 1

    async def describe_cluster(self) -> dict[str, object]:
        self.calls += 1
        if self.results:
            result = self.results.pop(0)
        elif self.default is not None:
            result = self.default
        else:
            result = {"cluster_id": "stand"}
        if isinstance(result, BaseException):
            raise result
        return {"cluster": result}


def test_broker_flag_turns_green_only_after_broker_reply() -> None:
    state = entrypoint.DependencyState()
    admin = _Admin([OSError("broker down")])

    async def scenario() -> None:
        task = asyncio.create_task(
            entrypoint._watch_broker(lambda: admin, state, interval=0.0)  # type: ignore[arg-type]
        )
        await asyncio.sleep(0)
        assert state.kafka is False, "флаг не поднимается до ответа брокера"
        await asyncio.sleep(0.05)
        assert state.kafka is True, "брокер ответил — готовы потреблять"
        assert admin.closes == 1, "упавшее соединение закрывается перед повтором"
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())


def test_broker_flag_drops_when_broker_disappears() -> None:
    state = entrypoint.DependencyState(kafka=True, redis=True)
    admin = _Admin([], default=OSError("connection reset"))

    async def scenario() -> None:
        task = asyncio.create_task(
            entrypoint._watch_broker(lambda: admin, state, interval=0.0)  # type: ignore[arg-type]
        )
        await asyncio.sleep(0.05)
        assert state.kafka is False, "пропавший брокер снимает готовность"
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())


def test_broker_probe_uses_a_real_admin_client() -> None:
    """Зонд обязан опираться на существующий API: `list_topics` у консьюмера нет."""
    from aiokafka import AIOKafkaConsumer
    from aiokafka.admin import AIOKafkaAdminClient

    async def check() -> None:
        # клиент создаётся И закрывается в одном loop: aiokafka берёт текущий
        # loop в конструкторе, закрытие из другого loop повисло бы
        admin = entrypoint.make_admin(entrypoint.Settings(_env_file=None))  # type: ignore[call-arg]
        try:
            assert isinstance(admin, AIOKafkaAdminClient)
            assert hasattr(admin, "describe_cluster"), "зонд опирается на этот метод"
        finally:
            await admin.close()

    asyncio.run(check())
    assert not hasattr(AIOKafkaConsumer, "list_topics"), "у консьюмера такого метода и нет"


def test_consume_loop_does_not_claim_ready_before_first_poll() -> None:
    """Известный долг: `kafka=True` ставился при входе в цикл, до первого poll."""
    state = entrypoint.DependencyState()
    seen: list[bool] = []

    class _Handler:
        async def handle_record(self, record: IncomingRecord) -> HandleResult:
            del record
            seen.append(state.kafka)
            return HandleResult("committed")

    consumer = _OneRecordConsumer()

    asyncio.run(entrypoint._consume(consumer, _Handler(), state))  # type: ignore[arg-type]

    assert seen == [False], "готовность подтверждает опрос брокера, а не вход в цикл"
    assert state.kafka is False, "после выхода цикла готовность снята"


class _OneRecordConsumer:
    """Минимальный consumer с одной записью — для проверки флага готовности."""

    def __init__(self) -> None:
        self.commits: list[dict] = []

    async def __aiter__(self):
        from aiokafka import ConsumerRecord
        from aiokafka.structs import TopicPartition

        record = ConsumerRecord(
            topic="books.comics.translation.requested.v1",
            partition=0,
            offset=0,
            timestamp=0,
            timestamp_type=0,
            key=b"book-1",
            value=b"{}",
            headers=None,
            checksum=None,
            serialized_key_size=-1,
            serialized_value_size=-1,
        )
        del TopicPartition
        yield record

    async def commit(self, offsets: dict) -> None:
        self.commits.append(offsets)


def test_health_server_answers_while_broker_is_down() -> None:
    """Под без Kafka обязан быть жив и честно «не готов»."""
    state = entrypoint.DependencyState(kafka=False, redis=False)

    async def scenario() -> tuple[int, int]:
        server = await entrypoint._start_health_server(state, port=0)
        try:
            port = server.sockets[0].getsockname()[1]
            health = await _http_get(port, "/health")
            ready = await _http_get(port, "/ready")
            return health, ready
        finally:
            server.close()
            await server.wait_closed()

    health, ready = asyncio.run(scenario())
    assert health == 200, "liveness не зависит от зависимостей"
    assert ready == 503, "без брокера и Redis — не готов"


async def _http_get(port: int, path: str) -> int:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write(f"GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode("ascii"))
        await writer.drain()
        head = await reader.readline()
        return int(head.split(b" ")[1])
    finally:
        writer.close()
        await writer.wait_closed()


def test_ready_payload_names_the_missing_dependency() -> None:
    payload = entrypoint.DependencyState(kafka=False, redis=True).payload()

    assert payload["status"] == "DOWN"
    assert payload["dependencies"] == {"kafka": "down", "redis": "ok"}, (
        "по /ready надо понимать, чего именно ждём"
    )

def test_backoff_doubles_and_stops_at_the_ceiling() -> None:
    """Пауза между попытками растёт вдвое и упирается в потолок.

    Раньше это жило только в комментарии: тесты гоняли `retry_delay=0.0`, при
    котором любая формула даёт ноль.
    """
    delays: list[float] = []
    delay = 1.0
    for _ in range(8):
        delay = entrypoint._next_delay(delay, retry_delay=1.0, max_delay=30.0)
        delays.append(delay)

    assert delays == [2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0, 30.0]


def test_stop_interrupts_a_hanging_connect_attempt() -> None:
    """SIGTERM во время попытки не ждёт клиентский таймаут aiokafka.

    Клиенты, которые «висят» в `start()`, держат grace-бюджет пода: без
    прерывания резерв дедупа не снимается, а k8s присылает SIGKILL.
    """

    class _Hanging(_Client):
        def __init__(self) -> None:
            super().__init__(fail=False)
            self.entered = asyncio.Event()
            self.cancelled = False

        async def start(self) -> None:
            self.entered.set()
            try:
                await asyncio.Event().wait()  # висим, как реальный таймаут aiokafka
            except asyncio.CancelledError:
                # отмена попытки — единственный признак настоящего прерывания:
                # без неё задача осталась бы висеть, а функция вернулась бы
                # «случайно» и тест этого не заметил
                self.cancelled = True
                raise

    hanging = _Hanging()
    partner = _Client(fail=False)

    async def scenario() -> tuple[object, float, bool]:
        stop = asyncio.Event()
        task = asyncio.create_task(
            entrypoint._connect_clients(
                lambda: hanging, lambda: partner, stop=stop, retry_delay=0.0
            )
        )
        await asyncio.wait_for(hanging.entered.wait(), timeout=1.0)
        started = asyncio.get_running_loop().time()
        stop.set()
        result = await asyncio.wait_for(task, timeout=1.0)
        return result, asyncio.get_running_loop().time() - started, hanging.cancelled

    result, elapsed, cancelled = asyncio.run(scenario())

    assert result is None, "по сигналу остановки подключение не отдаёт клиентов"
    assert cancelled is True, "текущая попытка обязана быть отменена, а не доиграна"
    assert hanging.stops == 1, "клиенты прерванной попытки обязаны быть закрыты"
    assert partner.stops == 1, "и второй клиент пары тоже"
    assert elapsed < 1.0, "ожидание остановки не должно длиться таймаут aiokafka"


def test_broker_probe_survives_an_unexpected_error() -> None:
    """Зонд обязан жить после неожидаемой ошибки и снимать готовность.

    Если зонд умирает молча, `/ready` застывает в последнем состоянии — и в
    самом плохом случае продолжает врать «готов» при мёртвом брокере.
    """

    class _Exploding:
        async def start(self) -> None:
            raise ValueError("несовместимая версия клиента")

        async def close(self) -> None:
            return None

    state = entrypoint.DependencyState(kafka=True)
    calls = 0

    def factory() -> object:
        nonlocal calls
        calls += 1
        return _Exploding()

    async def scenario() -> None:
        task = asyncio.create_task(
            entrypoint._watch_broker(factory, state, interval=0.0)  # type: ignore[arg-type]
        )
        await asyncio.sleep(0.05)
        assert task.done() is False, "зонд не имеет права завершаться"
        assert state.kafka is False, "неожидаемая ошибка зонда снимает готовность"
        assert calls > 1, "зонд продолжает опрос после ошибки"
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())


def test_redis_probe_survives_an_unexpected_error() -> None:
    """Та же инварианта для Redis-зонда: `/ready` не застывает в «готове»."""

    class _Exploding:
        async def ping(self) -> None:
            raise ValueError("клиент redis несовместим")

    state = entrypoint.DependencyState(redis=True)

    async def scenario() -> None:
        task = asyncio.create_task(
            entrypoint._refresh_redis(_Exploding(), state, interval=0.0)  # type: ignore[arg-type]
        )
        await asyncio.sleep(0.05)
        assert task.done() is False
        assert state.redis is False
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())

def _free_port() -> int:
    """Свободный порт: пробам нужен реальный сокет, а не мок."""
    import socket

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def test_probes_answer_while_the_broker_is_unreachable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Центральное утверждение гейта: пробы поднимаются ДО подключения.

    Если порядок в `_main` перевернуть (сначала `_connect_clients`, потом
    health-сервер), при недоступном брокере под не ответит ни на liveness, ни
    на readiness — k8s начнёт его убивать вместо ожидания сети. Здесь это
    ловится: подключение «висит» до сигнала остановки, а пробы обязаны
    отвечать.
    """
    import http.client
    from pathlib import Path

    port = _free_port()
    schema = (
        Path(__file__).resolve().parents[2]
        / "books-platform-openapi"
        / "asyncapi"
        / "comic-translation-events.yaml"
    )
    monkeypatch.setenv("TRANSLATION_HEALTH_PORT", str(port))
    monkeypatch.setenv("TRANSLATION_SCHEMA_PATH", str(schema))
    monkeypatch.setenv("TRANSLATION_FONT_PATH", "")

    captured: dict[str, object] = {}

    def remember_stop(stop: asyncio.Event) -> None:
        captured["stop"] = stop

    async def hanging(*_args: object, stop: asyncio.Event, **_kwargs: object) -> None:
        captured["connecting"] = True
        await stop.wait()
        return None

    monkeypatch.setattr(entrypoint, "_install_signal_handlers", remember_stop)
    monkeypatch.setattr(entrypoint, "_connect_clients", hanging)

    def probe(path: str) -> tuple[int, dict[str, object]]:
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        try:
            connection.request("GET", path)
            response = connection.getresponse()
            import json

            return response.status, json.loads(response.read())
        finally:
            connection.close()

    async def scenario() -> int:
        task = asyncio.create_task(entrypoint._main())
        for _ in range(200):
            if captured.get("connecting") or task.done():
                break
            await asyncio.sleep(0.01)
        assert captured.get("connecting") is True, "подключение к брокеру не началось"
        assert task.done() is False, "под не должен завершаться, пока брокера нет"

        # пробы обязаны отвечать, хотя подключение висит
        deadline = asyncio.get_running_loop().time() + 5.0
        status, body = await asyncio.to_thread(probe, "/ready")
        while status != 503 and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.05)
            status, body = await asyncio.to_thread(probe, "/ready")
        assert status == 503, "недоступный брокер обязан давать 503 на /ready"
        assert body["dependencies"]["kafka"] == "down"
        health_status, health_body = await asyncio.to_thread(probe, "/health")
        assert health_status == 200, "liveness не зависит от зависимостей"
        assert health_body == {"status": "UP"}

        stop = captured["stop"]
        assert isinstance(stop, asyncio.Event)
        stop.set()
        return await asyncio.wait_for(task, timeout=5.0)

    assert asyncio.run(scenario()) == 0