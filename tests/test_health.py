"""Пробы сервиса: liveness и readiness по зависимостям (known issue ревью).

`/health` остаётся liveness-пробой (процесс жив, отвечает 200) — её нельзя
ронять из-за недоступного Redis, иначе k8s начнёт рестартовать живой воркер.
Зависимости проверяет `/ready`: 503 и перечень нездоровых компонентов, чтобы
readiness снимала под с трафика, а не убивала его.
"""

from __future__ import annotations

import asyncio
import contextlib
import json


from translation_worker.__main__ import DependencyState, _refresh_redis, _serve_health

REQUEST = b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n"
READY_REQUEST = b"GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n"


class _Reader:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self._sent = False

    async def readuntil(self, _separator: bytes) -> bytes:
        if self._sent:
            raise asyncio.IncompleteReadError(b"", None)
        self._sent = True
        return self._payload


class _Writer:
    def __init__(self) -> None:
        self.buffer = b""
        self.closed = False

    def write(self, data: bytes) -> None:
        self.buffer += data

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None


def _serve(request: bytes, state: DependencyState) -> tuple[int, dict[str, object]]:
    writer = _Writer()
    asyncio.run(_serve_health(_Reader(request), writer, state))  # type: ignore[arg-type]
    head, _, body = writer.buffer.partition(b"\r\n\r\n")
    status = int(head.split(b" ")[1])
    return status, json.loads(body.decode("utf-8"))


def test_health_is_liveness_and_ignores_dependencies() -> None:
    state = DependencyState(kafka=False, redis=False)

    status, body = _serve(REQUEST, state)

    assert status == 200, "liveness не должна падать из-за недоступных зависимостей"
    assert body == {"status": "UP"}


def test_ready_reports_healthy_dependencies() -> None:
    status, body = _serve(READY_REQUEST, DependencyState(kafka=True, redis=True))

    assert status == 200
    assert body == {"status": "UP", "dependencies": {"kafka": "ok", "redis": "ok"}}


def test_ready_fails_with_unhealthy_dependencies() -> None:
    status, body = _serve(READY_REQUEST, DependencyState(kafka=False, redis=True))

    assert status == 503
    assert body["status"] == "DOWN"
    assert body["dependencies"] == {"kafka": "down", "redis": "ok"}


def test_unknown_path_is_not_ready() -> None:
    status, body = _serve(b"GET /metrics HTTP/1.1\r\n\r\n", DependencyState(kafka=True, redis=True))

    assert status == 404
    assert body["status"] == "NOT_FOUND"

def test_responses_are_well_formed_http() -> None:
    """Проба должна получать закрываемый ответ с длиной: иначе клиент зависнет."""
    writer = _Writer()
    asyncio.run(  # type: ignore[arg-type]
        _serve_health(_Reader(READY_REQUEST), writer, DependencyState(kafka=True, redis=True))
    )
    head = writer.buffer.split(b"\r\n\r\n", 1)[0].decode("ascii")

    assert head.startswith("HTTP/1.1 200 OK")
    assert "Content-Type: application/json" in head
    assert "Connection: close" in head
    assert "Content-Length:" in head
    assert writer.closed is True


def test_ready_fails_when_redis_is_down() -> None:
    status, body = _serve(READY_REQUEST, DependencyState(kafka=True, redis=False))

    assert status == 503
    assert body["dependencies"] == {"kafka": "ok", "redis": "down"}


def test_refresh_redis_tracks_availability() -> None:
    """Фоновый PING переводит readiness и вниз, и обратно вверх."""
    state = DependencyState(kafka=True, redis=True)

    class _Redis:
        def __init__(self) -> None:
            self.up = False

        async def ping(self) -> bool:
            if not self.up:
                raise OSError("connection refused")
            return True

    async def scenario() -> tuple[bool, bool]:
        redis = _Redis()
        task = asyncio.create_task(
            _refresh_redis(redis, state, interval=0.01)  # type: ignore[arg-type]
        )
        await asyncio.sleep(0.03)
        down = state.redis
        redis.up = True
        await asyncio.sleep(0.03)
        up = state.redis
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        return down, up

    down, up = asyncio.run(scenario())
    assert down is False, "недоступный Redis обязан сделать пробу неготовой"
    assert up is True, "восстановление Redis возвращает готовность"

def test_dependencies_start_down_until_confirmed() -> None:
    """Стартовая готовность — DOWN: /ready не должен врать до первого PING."""
    assert DependencyState().payload()["status"] == "DOWN"


def test_refresh_redis_reports_programming_errors_without_dying(caplog) -> None:
    """`AttributeError` — поломка кода, а не отказ зависимости: её видно в логе.

    Раньше проба падала целиком, и `/ready` застывал в последнем состоянии —
    при зелёном флаге это означало «готов» при мёртвой зависимости. Теперь
    программная ошибка снимает готовность, попадает в лог с трейсбеком, а сам
    зонд продолжает работать: исчезнувшая задача неотличима от здоровой.
    """

    class _Broken:
        async def ping(self) -> bool:
            raise AttributeError("ping is gone")

    state = DependencyState(redis=True)

    async def scenario() -> None:
        task = asyncio.create_task(
            _refresh_redis(_Broken(), state, interval=0.01)  # type: ignore[arg-type]
        )
        await asyncio.sleep(0.1)
        assert task.done() is False, "зонд обязан пережить поломку кода"
        assert state.redis is False, "готовность снимается, а не залипает в True"
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    with caplog.at_level("ERROR", logger="translation_worker.__main__"):
        asyncio.run(scenario())

    messages = [record.message for record in caplog.records]
    assert any("redis" in message for message in messages), "поломка обязана быть в логе"