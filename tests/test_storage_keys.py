"""Ключи хранилища (карточка 55 §2.6).

Детерминированные ключи — основа идемпотентности: повторная обработка
перезаписывает те же артефакты. Ключ страницы выводится из публичного URL
`pages[].url` события, а не собирается из uploadId.
"""

from __future__ import annotations

import pytest
from translation_worker.storage import (
    cache_key,
    image_key,
    key_from_public_url,
    layer_key,
)

BOOK = "018f0000-0000-7000-8000-000000000001"
BASE = "http://minio.test/books-platform"


def test_page_keys_are_deterministic_and_padded_to_three_digits():
    assert layer_key(BOOK, "ru", 7) == f"translated/{BOOK}/ru/page/007/layer.json"
    assert image_key(BOOK, "ru", 7) == f"translated/{BOOK}/ru/page/007/web.webp"
    # паддинг до трёх знаков: 12 → 012, 1000 → 1000 (без обрезки)
    assert layer_key(BOOK, "ru", 12).endswith("/page/012/layer.json")
    assert layer_key(BOOK, "ru", 1000).endswith("/page/1000/layer.json")


def test_page_keys_differ_by_language():
    assert layer_key(BOOK, "ru", 1) != layer_key(BOOK, "de", 1)


def test_public_url_is_translated_into_processed_bucket_key():
    url = f"{BASE}/processed/up-42/page/003/original.jpg"
    assert key_from_public_url(url, BASE) == "processed/up-42/page/003/original.jpg"


def test_public_base_trailing_slash_is_tolerated():
    url = f"{BASE}/processed/up-42/page/003/original.jpg"
    assert key_from_public_url(url, BASE + "/") == "processed/up-42/page/003/original.jpg"


def test_foreign_host_or_base_yields_none():
    """Чужой хост/база — повод для HTTP-фолбэка, а не для угадывания ключа."""
    assert key_from_public_url("https://cdn.example.com/a/b.jpg", BASE) is None
    assert key_from_public_url("http://other.test/books-platform/a.jpg", BASE) is None
    assert key_from_public_url("", BASE) is None
    assert key_from_public_url(f"{BASE}/processed/up-42/x.jpg", "") is None


def test_cache_key_binds_language_provider_and_content_hash():
    expected = "translation-cache/ru/LLM/abc.json"
    assert cache_key("translation-cache/", "ru", "LLM", "abc") == expected
    # провайдер — часть ключа: смена провайдера не должна отдавать чужой перевод
    assert cache_key("translation-cache/", "ru", "LLM", "abc") != cache_key(
        "translation-cache/", "ru", "DEEPL", "abc"
    )
    # и язык тоже
    assert cache_key("translation-cache/", "ru", "LLM", "abc") != cache_key(
        "translation-cache/", "de", "LLM", "abc"
    )


def test_cache_key_normalizes_prefix_slashes():
    expected = "translation-cache/ru/LLM/abc.json"
    assert cache_key("translation-cache", "ru", "LLM", "abc") == expected
    assert cache_key("/translation-cache/", "ru", "LLM", "abc") == expected


@pytest.mark.parametrize("page", [1, 9, 10, 99, 100])
def test_layer_key_is_stable_for_every_page_number(page: int):
    assert layer_key(BOOK, "ru", page) == layer_key(BOOK, "ru", page)
    assert layer_key(BOOK, "ru", page).count("/page/") == 1
