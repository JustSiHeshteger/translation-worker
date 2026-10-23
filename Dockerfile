# =====================================================================
# translation-worker — образ конвейера автоперевода комиксов (Python 3.12).
#
# Фаза 1: OCR/перевод/рендер — инъектируемые провайдеры (fake|http|manga,
# STUB|LLM|DEEPL), арт-слой не рисуется. ML-стек (torch/manga-ocr) в образ
# НЕ ставится — он живёт в requirements-ml.txt для GPU-поды/локального запуска.
#
# Контекст сборки — КОРЕНЬ монорепо (в образ кладётся asyncapi-контракт
# событий, источник правды — books-platform-openapi, а не копия в сервисе):
#   docker compose -f docker/docker-compose.yml build translation-worker
# =====================================================================
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/src \
    TRANSLATION_SCHEMA_PATH=/app/schemas/comic-translation-events.yaml \
    TRANSLATION_HEALTH_PORT=8093

WORKDIR /app

# non-root пользователь сервиса
RUN groupadd --system svc \
    && useradd --system --gid svc --home-dir /app --shell /usr/sbin/nologin svc

COPY translation-worker/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY translation-worker/pyproject.toml ./pyproject.toml
COPY translation-worker/src ./src
RUN pip install --no-cache-dir --no-deps .

# Контракт событий (карточка 54) — тот же файл, что в books-platform-openapi
COPY books-platform-openapi/asyncapi/comic-translation-events.yaml \
     ./schemas/comic-translation-events.yaml

USER svc
EXPOSE 8093

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8093/health', timeout=3)"

ENTRYPOINT ["python", "-m", "translation_worker"]
