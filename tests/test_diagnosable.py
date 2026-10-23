"""Идентифицируемость отказа по схеме (known issue ревью round 2).

Диагностические события публикуются только когда из отвергнутого payload'а
можно достать `bookId` (UUID) и код языка. Код языка в контракте — строка
1…32 символов, поэтому значение с дефисом/подчёркиванием тоже допустимо и
не должно молча превращаться в poison.
"""

from __future__ import annotations

import pytest

from translation_worker.kafka_consumer import _is_diagnosable

BOOK_ID = "018f0000-0000-7000-8000-000000000001"


@pytest.mark.parametrize("language", ["ru", "RUSSIAN", "pt-BR", "zh_hans", "ru ru"])
def test_printable_language_codes_are_diagnosable(language: str) -> None:
    assert _is_diagnosable(BOOK_ID, language) is True


@pytest.mark.parametrize("language", ["", "x" * 33, "\x00ru", "ru\x07"])
def test_broken_language_is_not_diagnosable(language: str) -> None:
    assert _is_diagnosable(BOOK_ID, language) is False


@pytest.mark.parametrize("book_id", ["", "not-a-uuid", "018f0000-0000-7000-8000"])
def test_broken_book_id_is_not_diagnosable(book_id: str) -> None:
    assert _is_diagnosable(book_id, "ru") is False
