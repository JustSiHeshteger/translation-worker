"""S3-хранилище и детерминированные ключи артефактов (карточка 55 §2.6, §4).

Ключи — основа идемпотентности: повторная обработка перезаписывает те же
объекты теми же байтами, поэтому слой страницы собирается из детерминированного
`layer.to_json()` (сортировка ключей, без временных меток).

Раскладка (processed-бакет):
    translated/{bookId}/{language}/page/{nnn:03d}/layer.json
    translated/{bookId}/{language}/page/{nnn:03d}/web.webp   (фаза 2)
    translated/{bookId}/{language}/manifest.json
    {cache_prefix}{language}/{provider}/{sha256}.json        (кэш переводов)

Ключ страницы-оригинала не вычисляется из uploadId: он выводится из публичного
URL `pages[].url` события (`key_from_public_url`), а если база не совпала —
страница докачивается по HTTP (фолбэк пайплайна).
"""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError

from translation_worker.config import Settings

_NOT_FOUND_CODES = {"404", "NoSuchKey", "NotFound"}


def layer_key(book_id: str, language: str, page: int) -> str:
    """Ключ слоя текста страницы: `translated/{bookId}/{lang}/page/{nnn}/layer.json`."""
    return f"{layer_base_key(book_id, language)}/{page:03d}/layer.json"


def image_key(book_id: str, language: str, page: int) -> str:
    """Ключ перерисованной страницы (фаза 2, арт-слой)."""
    return f"{layer_base_key(book_id, language)}/{page:03d}/web.webp"


def layer_base_key(book_id: str, language: str) -> str:
    """Префикс слоёв книги — он попадает в `layerBaseKey` события Completed."""
    return f"translated/{book_id}/{language}/page"


def manifest_key(book_id: str, language: str) -> str:
    """Ключ сводки книги по переводу."""
    return f"translated/{book_id}/{language}/manifest.json"


def cache_key(prefix: str, language: str, provider: str, sha256: str) -> str:
    """Ключ кэша перевода: префикс + язык + провайдер + хеш содержимого страницы."""
    parts = [part.strip("/") for part in (prefix or "", language, provider, sha256)]
    head = "/".join(part for part in parts[:3] if part)
    return f"{head}/{sha256}.json" if head else f"{sha256}.json"


def key_from_public_url(url: str, public_base: str) -> str | None:
    """Ключ объекта в бакете из публичного URL; чужая база → None (HTTP-фолбэк)."""
    base = (public_base or "").strip().rstrip("/")
    candidate = (url or "").strip()
    if not base or not candidate:
        return None
    parsed = urlsplit(candidate)
    if not parsed.scheme or not parsed.netloc:
        return None
    # query/fragment не входят в ключ объекта
    clean = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
    if not clean.startswith(base):
        return None
    rest = clean[len(base) :]
    if not rest.startswith("/"):
        return None
    key = rest.lstrip("/")
    return key or None


class Storage:
    """Тонкая обёртка над boto3-клиентом: get/put/exists по бакету из настроек."""

    def __init__(self, settings: Settings) -> None:
        self._bucket = settings.bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name=settings.s3_region,
            config=BotoConfig(
                signature_version="s3v4",
                # сетевой столл MinIO не должен вешать конвейер: жёсткие
                # таймауты + ограниченные ретраи (карточка 55 §2.6)
                connect_timeout=5,
                read_timeout=30,
                retries={"max_attempts": 3, "mode": "standard"},
            ),
        )

    def get_bytes(self, key: str) -> bytes:
        """Прочитать объект; отсутствие объекта — `KeyError` (как фейк в тестах)."""
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            if _error_code(exc) in _NOT_FOUND_CODES:
                raise KeyError(key) from exc
            raise
        with response["Body"] as body:
            return body.read()

    def put_json(self, key: str, payload: dict[str, Any]) -> str:
        """Записать JSON детерминированно: UTF-8, `ensure_ascii=False`, ключи сортируются."""
        data = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        return self.put_bytes(key, data, "application/json")

    def put_bytes(
        self, key: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> str:
        self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
        )
        return key

    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            if _error_code(exc) in _NOT_FOUND_CODES:
                return False
            raise
        return True


def _error_code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", ""))
