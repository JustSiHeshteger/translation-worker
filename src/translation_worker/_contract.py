"""Чтение JSON-схем из AsyncAPI-контракта событий (карточка 55 §2.4).

Контракт живёт в `books-platform-openapi/asyncapi/comic-translation-events.yaml`
и копируется в образ как есть, поэтому схемы payload'ов берутся из него, а не
дублируются в коде. Разбор — `yaml.safe_load` (PyYAML, утверждён облаком в
`requirements.txt`): собственный парсер YAML здесь был бы лишним источником
ошибок.

Ссылки `$ref: '#/components/schemas/...'` разворачиваются на месте, а
OpenAPI-флаг `nullable: true` переводится в `type: [<type>, 'null']`, чтобы
валидация `jsonschema` не отбрасывала легальные null-поля контракта.
"""

from __future__ import annotations

from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

DEFAULT_SCHEMA_PATH = "schemas/comic-translation-events.yaml"
_SCHEMA_PATH_KEYS = ("components", "schemas")


class ContractError(ValueError):
    """Контракт не читается или в нём нет запрошенной схемы."""


# ── Публичный интерфейс ──────────────────────────────────────────────


def load_document(path: str | Path | None = None) -> dict[str, Any]:
    """Разобрать документ контракта (результат кэшируется по пути и mtime)."""
    resolved = Path(path or DEFAULT_SCHEMA_PATH)
    try:
        stamp = resolved.stat().st_mtime_ns
    except OSError as exc:
        raise ContractError(f"контракт событий не найден: {resolved}") from exc
    return _load_cached(str(resolved), stamp)


def component_schema(name: str, path: str | Path | None = None) -> dict[str, Any]:
    """Вернуть развёрнутую схему `components.schemas.<name>` контракта."""
    schemas = _schema_section(load_document(path))
    if name not in schemas:
        raise ContractError(f"в контракте событий нет схемы components.schemas.{name}")
    return _prepare(deepcopy(schemas[name]), schemas)


# ── Загрузка и разворачивание ссылок ────────────────────────────────


@lru_cache(maxsize=16)
def _load_cached(path: str, _stamp: int) -> dict[str, Any]:
    text = Path(path).read_text(encoding="utf-8")
    try:
        document = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ContractError(f"контракт {path}: невалидный YAML ({exc})") from exc
    if not isinstance(document, dict):
        raise ContractError(f"контракт {path}: ожидался объект верхнего уровня")
    return document


def _schema_section(document: dict[str, Any]) -> dict[str, Any]:
    node: Any = document
    for key in _SCHEMA_PATH_KEYS:
        if not isinstance(node, dict):
            return {}
        node = node.get(key) or {}
    return node if isinstance(node, dict) else {}


def _prepare(node: Any, schemas: dict[str, Any], seen: frozenset[str] = frozenset()) -> Any:
    """Развернуть `$ref` и нормализовать `nullable` рекурсивно."""
    if isinstance(node, dict):
        reference = node.get("$ref")
        if isinstance(reference, str):
            target = reference.rsplit("/", 1)[-1]
            if target in seen:
                raise ContractError(f"циклическая ссылка $ref: {reference}")
            if target not in schemas:
                raise ContractError(f"неразрешимая ссылка $ref: {reference}")
            return _prepare(deepcopy(schemas[target]), schemas, seen | {target})
        prepared: dict[str, Any] = {
            key: _prepare(value, schemas, seen) for key, value in node.items()
        }
        if prepared.pop("nullable", None) is True:
            declared = prepared.get("type")
            if isinstance(declared, str) and declared != "null":
                prepared["type"] = [declared, "null"]
        return prepared
    if isinstance(node, list):
        return [_prepare(item, schemas, seen) for item in node]
    return node
