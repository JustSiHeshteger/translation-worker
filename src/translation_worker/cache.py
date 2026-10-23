"""Кэш переводов по SHA-256 содержимого страницы (карточка 55 §2.14).

Ключ иммутабелен (язык + провайдер + хеш байтов страницы), поэтому TTL не нужен:
изменение страницы меняет хеш, смена провайдера — сегмент ключа. В кэше лежат
блоки без привязки к странице (`id` подставляет пайплайн как `p{page}-b{i}`) —
так `layer.json` побайтово стабилен и при повторном прогоне, и после кэш-хита.
Битая запись в кэше — промах, а не исключение: страница просто переведётся заново.
"""

from __future__ import annotations

import json
import logging

from translation_worker.config import Settings
from translation_worker.models import TextBlock
from translation_worker.storage import Storage, cache_key

_LOG = logging.getLogger(__name__)

_CACHE_VERSION = 1


class TranslationCache:
    """Кэш готовых блоков страницы (после перевода и вёрстки)."""

    def __init__(self, storage: Storage, settings: Settings) -> None:
        # Облачные тесты создают кэш двумя порядками аргументов: кэш-тесты —
        # (storage, settings), тесты пайплайна — (settings, storage). Принимаем
        # оба варианта: тесты — зона облака и правке не подлежат (§3).
        if isinstance(storage, Settings):
            storage, settings = settings, storage
        self._storage = storage
        self._settings = settings

    def key(self, *, language: str, provider: str, sha256: str) -> str:
        """Ключ записи кэша для (язык, провайдер, хеш страницы)."""
        return cache_key(self._settings.cache_prefix, language, provider, sha256)

    def load(self, key: str) -> list[TextBlock] | None:
        """Прочитать блоки страницы из кэша; промах/битая запись → None."""
        try:
            data = self._storage.get_bytes(key)
        except KeyError:
            return None
        try:
            payload = json.loads(data.decode("utf-8"))
            if int(payload.get("version", 0)) != _CACHE_VERSION:
                _LOG.warning("кэш перевода другой версии, промах: %s", key)
                return None
            blocks = [TextBlock.from_json(item) for item in payload["blocks"]]
        except (AttributeError, KeyError, TypeError, ValueError):
            _LOG.warning("кэш перевода повреждён, промах: %s", key)
            return None
        for block in blocks:
            # id привязан к номеру страницы — в кэше он не хранится
            block.id = ""
        return blocks

    def save(self, key: str, blocks: list[TextBlock]) -> None:
        """Положить блоки страницы в кэш (после успешного перевода и вёрстки)."""
        payload = {
            "version": _CACHE_VERSION,
            "blocks": [{**block.to_json(), "id": ""} for block in blocks],
        }
        self._storage.put_json(key, payload)
