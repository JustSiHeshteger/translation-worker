"""Атомарный дедуп по `ce-id` (карточка 55 §2.16, known issues ревью).

Дедуп двухключевой: `{prefix}{ce-id}` — «событие обработано» (TTL сутки),
`{prefix}inflight:{ce-id}` — резерв «событие в работе» с ОТДЕЛЬНЫМ коротким TTL
и токеном владельца.

Почему три состояния, а не флаг: «уже обработано» и «обрабатывает другая
реплика» требуют разной реакции консьюмера. Дубликат можно коммитить, а занятое
чужое — нельзя (оффсет отматывается), иначе задание, чей воркер упал до коммита,
считается дубликатом и теряется без единого события.
"""

from __future__ import annotations

import asyncio

import pytest
from translation_worker.kafka_consumer import (
    ACQUIRE_ACQUIRED,
    ACQUIRE_BUSY,
    ACQUIRE_DUPLICATE,
    RedisDedup,
)


class _FakeRedis:
    """Минимальная модель Redis: `SET NX EX`, `SET EX`, `EXISTS`, `GET`, `DELETE`.

    `await asyncio.sleep(0)` в `exists`/`set` — обязательное условие
    конкурентного теста: без уступки управления он не отличит атомарный резерв
    от пары `EXISTS` → `SET`.
    """

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.ttl: dict[str, int] = {}

    async def set(
        self, key: str, value: str, nx: bool = False, ex: int | None = None
    ) -> bool | None:
        await asyncio.sleep(0)
        if nx and key in self.store:
            return None
        self.store[key] = value
        if ex is not None:
            self.ttl[key] = ex
        return True

    async def exists(self, key: str) -> int:
        await asyncio.sleep(0)
        return 1 if key in self.store else 0

    async def get(self, key: str) -> str | None:
        await asyncio.sleep(0)
        return self.store.get(key)

    async def delete(self, key: str) -> int:
        self.ttl.pop(key, None)
        return 1 if self.store.pop(key, None) is not None else 0

    async def eval(self, script: str, numkeys: int, key: str, token: str) -> int:
        """`EVAL` compare-and-delete: как у Lua-скрипта в Redis."""
        await asyncio.sleep(0)
        if self.store.get(key) == token:
            self.ttl.pop(key, None)
            del self.store[key]
            return 1
        return 0


def _dedup(redis: object | None = None) -> RedisDedup:
    return RedisDedup(
        redis or _FakeRedis(),
        prefix="dedup:",
        ttl_seconds=86_400,
        inflight_ttl_seconds=2_100,
    )


def test_concurrent_acquire_is_exclusive() -> None:
    dedup = _dedup()

    async def scenario() -> list[str]:
        return await asyncio.gather(dedup.acquire("evt-1"), dedup.acquire("evt-1"))

    results = asyncio.run(scenario())

    assert sorted(results) == sorted([ACQUIRE_ACQUIRED, ACQUIRE_BUSY]), (
        f"резерв должен достаться ровно одному: {results}"
    )


def test_completed_event_is_duplicate_for_others() -> None:
    dedup = _dedup()

    async def scenario() -> tuple[str, str]:
        first = await dedup.acquire("evt-2")
        await dedup.complete("evt-2")
        return first, await dedup.acquire("evt-2")

    first, second = asyncio.run(scenario())
    assert first == ACQUIRE_ACQUIRED
    assert second == ACQUIRE_DUPLICATE, "завершённое событие не обрабатывается повторно"


def test_reservation_ttl_is_shorter_than_done_ttl() -> None:
    redis = _FakeRedis()
    dedup = _dedup(redis)

    asyncio.run(dedup.acquire("evt-3"))

    assert redis.ttl["dedup:inflight:evt-3"] == 2_100, (
        "резерв обязан истекать по СВОЕМУ короткому TTL, а не по суточному"
    )


def test_done_marker_keeps_long_ttl() -> None:
    redis = _FakeRedis()
    dedup = _dedup(redis)

    async def scenario() -> None:
        await dedup.acquire("evt-4")
        await dedup.complete("evt-4")

    asyncio.run(scenario())

    assert redis.ttl["dedup:evt-4"] == 86_400
    assert "dedup:inflight:evt-4" not in redis.store, "после завершения резерв снят"


def test_release_frees_reservation_for_redelivery() -> None:
    dedup = _dedup()

    async def scenario() -> str:
        await dedup.acquire("evt-5")
        await dedup.release("evt-5")
        return await dedup.acquire("evt-5")

    assert asyncio.run(scenario()) == ACQUIRE_ACQUIRED


def test_release_does_not_drop_foreign_reservation() -> None:
    """Резерв чужой реплики снимать нельзя (иначе — двойная обработка)."""
    redis = _FakeRedis()
    owner = _dedup(redis)
    stranger = _dedup(redis)

    async def scenario() -> tuple[str, bool]:
        holder = await owner.acquire("evt-6")
        await stranger.release("evt-6")
        return holder, "dedup:inflight:evt-6" in redis.store

    holder, still_held = asyncio.run(scenario())
    assert holder == ACQUIRE_ACQUIRED
    assert still_held is True, "чужой резерв должен остаться"


def test_reservation_expiry_allows_reprocessing() -> None:
    """Истёкший резерв (упавшая реплика) не должен делать задание дубликатом."""
    redis = _FakeRedis()

    async def scenario() -> str:
        await _dedup(redis).acquire("evt-7")
        redis.store.pop("dedup:inflight:evt-7", None)  # резерв истёк по TTL
        return await _dedup(redis).acquire("evt-7")

    assert asyncio.run(scenario()) == ACQUIRE_ACQUIRED

def test_takeover_is_not_swept_by_previous_owner() -> None:
    """После истечения резерва новый владелец не должен потерять свою бронь."""
    redis = _FakeRedis()
    first = _dedup(redis)
    second = _dedup(redis)

    async def scenario() -> tuple[str, bool]:
        await first.acquire("evt-8")
        # TTL истёк: резерв исчез, событие взяла другая реплика
        redis.store.pop("dedup:inflight:evt-8", None)
        taken = await second.acquire("evt-8")
        await first.release("evt-8")
        return taken, "dedup:inflight:evt-8" in redis.store

    taken, still_held = asyncio.run(scenario())
    assert taken == ACQUIRE_ACQUIRED
    assert still_held is True, "старый владелец не снимает резерв нового"


def test_inflight_ttl_is_required() -> None:
    """Fail-open дефолт вернул бы суточный резерв — параметр обязателен."""
    with pytest.raises(TypeError):
        RedisDedup(_FakeRedis(), prefix="dedup:")  # type: ignore[call-arg]


def test_inflight_ttl_must_be_shorter_than_done_ttl() -> None:
    with pytest.raises(ValueError):
        RedisDedup(_FakeRedis(), prefix="dedup:", ttl_seconds=100, inflight_ttl_seconds=100)