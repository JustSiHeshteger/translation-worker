"""Общие фикстуры и fake-провайдеры тестов translation-worker.

Тесты НЕ тянут torch/manga-ocr, не ходят в Kafka/S3/Redis и не требуют GPU:
провайдеры OCR/перевода и хранилище подменяются fake-реализациями отсюда
(карточка 55 §2.11, §2.15).
"""

from __future__ import annotations

import io
import json
from dataclasses import dataclass, field
from typing import Any

import pytest
from PIL import Image

# ── Изображения ────────────────────────────────────────────────


@pytest.fixture
def page_png():
    """PNG-байты страницы нужного размера (размеры читает пайплайн)."""

    def _make(width: int = 1200, height: int = 1800, color: tuple[int, int, int] = (255, 255, 255)):
        buffer = io.BytesIO()
        Image.new("RGB", (width, height), color).save(buffer, format="PNG")
        return buffer.getvalue()

    return _make


# ── Хранилище артефактов (S3) ───────────────────────────────────


@dataclass
class FakeStorage:
    """S3-подобное хранилище в памяти: ключ → байты."""

    objects: dict[str, bytes] = field(default_factory=dict)
    puts: list[str] = field(default_factory=list)

    def get_bytes(self, key: str) -> bytes:
        if key not in self.objects:
            raise KeyError(key)
        return self.objects[key]

    def exists(self, key: str) -> bool:
        return key in self.objects

    def put_json(self, key: str, payload: dict) -> str:
        self.objects[key] = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        self.puts.append(key)
        return key

    def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream"):
        self.objects[key] = data
        self.puts.append(key)
        return key

    def json_at(self, key: str) -> dict:
        return json.loads(self.objects[key].decode("utf-8"))


@pytest.fixture
def storage() -> FakeStorage:
    return FakeStorage()


# ── Провайдеры OCR / перевода ───────────────────────────────────


@dataclass
class FakeOcr:
    """OCR-провайдер: отдаёт заранее заданные строки (по странице — по порядку)."""

    pages: list[list[Any]] = field(default_factory=list)
    error: Exception | None = None
    calls: int = 0
    default_lines: list[Any] = field(default_factory=list)

    async def recognize(self, image: bytes):
        self.calls += 1
        if self.error is not None:
            raise self.error
        index = min(self.calls - 1, len(self.pages) - 1)
        return self.pages[index] if self.pages else self.default_lines


@dataclass
class FakeTranslator:
    """Переводчик: детерминированный префикс, опциональные ошибка и задержка."""

    prefix: str = "переведено: "
    error: Exception | None = None
    delay: float = 0.0
    calls: int = 0
    seen: list[str] = field(default_factory=list)

    async def translate(
        self,
        text: str,
        *,
        source: str | None = None,
        target: str = "ru",
        glossary: dict[str, str] | None = None,
    ) -> str:
        import asyncio

        self.calls += 1
        self.seen.append(text)
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.error is not None:
            raise self.error
        if glossary and text in glossary:
            return glossary[text]
        return f"{self.prefix}{text}"


@pytest.fixture
def fake_ocr() -> FakeOcr:
    return FakeOcr()


@pytest.fixture
def fake_translator() -> FakeTranslator:
    return FakeTranslator()


# ── Публикация событий ──────────────────────────────────────────


@dataclass
class FakePublisher:
    """Запоминает (topic, event) — проверки публикаций без брокера."""

    published: list[tuple[str, Any]] = field(default_factory=list)

    async def publish(self, topic: str, event: Any) -> None:
        self.published.append((topic, event))

    def types(self) -> list[str]:
        return [event.headers.get("ce-type") for _, event in self.published]

    def payloads(self, ce_type: str) -> list[dict]:
        return [
            json.loads(event.value.decode("utf-8"))
            for _, event in self.published
            if event.headers.get("ce-type") == ce_type
        ]


@pytest.fixture
def publisher() -> FakePublisher:
    return FakePublisher()


# ── Настройки ───────────────────────────────────────────────────


@pytest.fixture
def settings():
    """Настройки с dev-дефолтами и fake-провайдерами (TRANSLATION_* env не нужны)."""
    from translation_worker.config import Settings

    return Settings(
        ocr_provider="fake",
        translate_provider="STUB",
        progress_interval=0.0,
        timeout=30.0,
        min_confidence=0.5,
        max_pages=1000,
        font_path="",
    )


# ── Payload-фабрики ─────────────────────────────────────────────


def _requested_payload(
    *,
    book_id: str = "018f0000-0000-7000-8000-000000000001",
    language: str = "ru",
    source_language: str | None = "en",
    provider: str = "LLM",
    pages: list[dict] | None = None,
    retry_count: int = 0,
    glossary: list[dict] | None = None,
) -> dict:
    """Payload ComicTranslationRequested по контракту карточки 54 §4."""
    base = "http://minio.test/books-platform/processed/up-1"
    return {
        "bookId": book_id,
        "bookSlug": "voin-i-ego-kot",
        "language": language,
        "sourceLanguage": source_language,
        "provider": provider,
        "pages": pages
        or [
            {"number": 1, "url": f"{base}/page/001/original.jpg"},
            {"number": 2, "url": f"{base}/page/002/original.jpg"},
        ],
        "glossary": glossary if glossary is not None else [],
        "requestedBy": "018f0000-0000-7000-8000-0000000000aa",
        "requestId": "req-1",
        "retryCount": retry_count,
        "createdAt": "2026-10-12T09:00:00Z",
    }


@pytest.fixture
def request_payload():
    """Фабрика payload ComicTranslationRequested (карточка 54 §4)."""
    return _requested_payload
