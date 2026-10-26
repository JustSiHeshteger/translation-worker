"""Кэш перевода помнит, каким промптом он сделан.

Ключ кэша — язык + провайдер + SHA-256 страницы, и в нём нет ничего про стиль
перевода. Поэтому после правки промпта («выбирай короткие синонимы») попадание
кэша возвращало старый, длинный текст, и книга выглядела непереведённой заново.
Запись кэша обязана нести версию стиля: чужой стиль — промах, а не перевод.
"""

from __future__ import annotations

import hashlib
import json

from translation_worker.cache import TranslationCache
from translation_worker.models import Box, TextBlock


def _blocks() -> list[TextBlock]:
    return [
        TextBlock(
            id="p1-b1",
            box=Box(x=10, y=20, w=100, h=40),
            kind="BUBBLE",
            original="You're late, cat.",
            text="Ты опоздал, кот.",
            confidence=0.9,
            font_size=22,
        )
    ]


def _digest() -> str:
    return hashlib.sha256(b"page-bytes").hexdigest()


def test_fresh_entry_records_style_version(settings, storage) -> None:
    """Свежая запись несёт версию стиля: по ней решается, годится ли кэш."""
    cache = TranslationCache(storage, settings)
    key = cache.key(language="ru", provider="LLM", sha256=_digest())

    cache.save(key, _blocks())

    payload = json.loads(storage.get_bytes(key).decode("utf-8"))
    assert payload["style"] == cache.style_version, "в записи нет версии стиля"


def test_entry_without_style_version_is_a_miss(settings, storage) -> None:
    """Старая запись без версии стиля — промах: иначе книга не переведётся заново."""
    cache = TranslationCache(storage, settings)
    key = cache.key(language="ru", provider="LLM", sha256=_digest())
    legacy = {
        "version": 1,
        "blocks": [{**block.to_json(), "id": ""} for block in _blocks()],
    }
    storage.put_json(key, legacy)

    assert cache.load(key) is None, "кэш без версии стиля обязан считаться устаревшим"


def test_entry_with_other_style_version_is_a_miss(settings, storage) -> None:
    """Запись, сделанная другим промптом, не подходит."""
    cache = TranslationCache(storage, settings)
    key = cache.key(language="ru", provider="LLM", sha256=_digest())
    foreign = {
        "version": 1,
        "style": cache.style_version + 1,
        "blocks": [{**block.to_json(), "id": ""} for block in _blocks()],
    }
    storage.put_json(key, foreign)

    assert cache.load(key) is None, "перевод, сделанный другим промптом, — промах"


def test_entry_with_current_style_is_used(settings, storage) -> None:
    """Своя версия стиля читается: кэш продолжает экономить прогоны."""
    cache = TranslationCache(storage, settings)
    key = cache.key(language="ru", provider="LLM", sha256=_digest())

    cache.save(key, _blocks())
    loaded = cache.load(key)

    assert loaded is not None
    assert loaded[0].text == "Ты опоздал, кот."
