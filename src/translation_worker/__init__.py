"""translation-worker: конвейер автоперевода комиксов (фаза 1 — текстовый слой).

Пакет собирает фоновый сервис, который читает Kafka-топик
`comic-translations.requests`, качает страницы-изображения из processed-бакета,
распознаёт текст в баблах, переводит его построчно и публикует `layer.json`
(текст + рамки в координатах оригинала) в S3, отдавая прогресс/результат
событиями CloudEvents (контракт `asyncapi/comic-translation-events.yaml`).

Конвенции сервиса: src-layout, запуск `python -m translation_worker`,
конфигурация из env через pydantic-settings (префикс `TRANSLATION_`).
"""

from __future__ import annotations

__all__ = ["__version__"]

__version__ = "0.1.0"
