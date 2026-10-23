# translation-worker

Фоновый сервис автоперевода комиксов (фаза 1 — **только текстовый слой**).
Читает задание из Kafka, качает страницы-изображения из processed-бакета,
распознаёт текст в баблах, переводит его и публикует слой `layer.json` в S3,
отдавая прогресс/результат событиями CloudEvents.

- Контракты: `books-platform-openapi/asyncapi/comic-translation-events.yaml`
  (карточка 54), карта цепочки — `feature-cards/chain-map-comic-translation.md`.
- Воркер **не пишет в БД** (D-004): только S3-артефакты и события Kafka.
- Фаза 1 публикует только `layer.json`; перерисовка страницы (`web.webp`) —
  фаза 2 (карточка 58), здесь `pages[].imageKey` всегда `null`.

## Конвейер

| Стадия | Что делает | Код |
|---|---|---|
| DOWNLOAD | публичный URL страницы → ключ processed-бакета → байты из S3 (HTTP-фолбэк, если база не совпала) | `pipeline.py`, `storage.py` |
| OCR | распознавание строк с рамками (`fake`/`http`/`manga`) | `providers/` |
| — | группировка строк в баблы, шум по `minConfidence` отбрасывается | `grouping.py` |
| TRANSLATE | перевод реплик (STUB/LLM/DeepL), пакетно по странице; кэш по SHA-256 страницы | `providers/`, `cache.py` |
| LAYOUT | подбор кегля 10–40 pt, перенос по словам, warning при переполнении | `layout.py` |
| S3 | `layer.json` по детерминированному ключу | `storage.py` |

## Топики и артефакты

| Направление | Топик |
|---|---|
| consumer | `comic-translations.requests` (at-least-once, ключ `bookId`) |
| producer | `comic-translations.results` (progress/completed/failed) |
| producer | `comic-translations.dlq` (ретраи исчерпаны / неисправимая ошибка) |

```
translated/{bookId}/{language}/page/{nnn}/layer.json      # фаза 1 (этот сервис)
translated/{bookId}/{language}/page/{nnn}/web.webp        # фаза 2 (карточка 58)
{cachePrefix}{language}/{provider}/{sha256}.json          # кэш переводов
{cachePrefix}dedup:{ce-id}                                # событие обработано, TTL 24 ч
{cachePrefix}dedup:inflight:{ce-id}                       # резерв «в работе», TTL 35 мин + токен владельца
```

Повторная доставка безопасна: ключи детерминированы, `layer.json` побайтово
стабилен, дубликат `ce-id` пропускается до запуска конвейера.

## Переменные окружения

Все настройки — с префиксом `TRANSLATION_` и dev-дефолтами (сервис поднимается
без переменных).

| Переменная | Дефолт | Смысл |
|---|---|---|
| `TRANSLATION_KAFKA_BOOTSTRAP` | `localhost:29092` | брокер |
| `TRANSLATION_REQUESTS_TOPIC` | `comic-translations.requests` | топик заданий |
| `TRANSLATION_RESULTS_TOPIC` | `comic-translations.results` | топик результатов |
| `TRANSLATION_DLQ_TOPIC` | `comic-translations.dlq` | топик DLQ |
| `TRANSLATION_GROUP_ID` | `translation-worker` | consumer group |
| `TRANSLATION_S3_ENDPOINT` | `http://localhost:9100` | S3/MinIO |
| `TRANSLATION_S3_ACCESS_KEY` / `TRANSLATION_S3_SECRET_KEY` | `minioadmin` | ключи S3 |
| `TRANSLATION_S3_REGION` | `us-east-1` | регион S3 |
| `TRANSLATION_BUCKET` | `books-platform` | бакет артефактов |
| `TRANSLATION_PUBLIC_BASE` | `http://localhost:9100/books-platform` | публичная база processed (из неё выводятся ключи страниц) |
| `TRANSLATION_OCR_PROVIDER` | `fake` | `fake` \| `http` \| `manga` |
| `TRANSLATION_OCR_URL` | — | URL OCR-сервиса (для `http`) |
| `TRANSLATION_MANGA_MODEL` | `kha-white/manga-ocr-base` | модель manga-ocr (для `manga`) |
| `TRANSLATION_TRANSLATE_PROVIDER` | `STUB` | `STUB` \| `LLM` \| `DEEPL` |
| `TRANSLATION_LLM_BASE_URL` / `_LLM_MODEL` / `_LLM_API_KEY` | — | OpenAI-совместимый endpoint |
| `TRANSLATION_DEEPL_API_KEY` | — | ключ DeepL |
| `TRANSLATION_MAX_RETRIES` | `2` | ретраев до DLQ |
| `TRANSLATION_TIMEOUT` | `1800` (30 мин) | таймаут задания, секунды |
| `TRANSLATION_PROGRESS_INTERVAL` | `1.0` | не чаще события прогресса, секунды |
| `TRANSLATION_MAX_PAGES` | `1000` | предел страниц на задание |
| `TRANSLATION_MIN_CONFIDENCE` | `0.5` | порог отбрасывания шумовых строк OCR |
| `TRANSLATION_REDIS_URL` | `redis://localhost:6379/0` | dedup и будущие состояния |
| `TRANSLATION_CACHE_PREFIX` | `translation-cache/` | префикс кэша переводов |
| `TRANSLATION_FONT_PATH` | — | TTF для вёрстки (без него — шрифт Pillow) |
| `TRANSLATION_SCHEMA_PATH` | `schemas/comic-translation-events.yaml` | контракт событий (в образе `/app/schemas/...`) |
| `TRANSLATION_HEALTH_PORT` | `8093` | порт проб: `/health` (liveness) и `/ready` (зависимости) |
| `TRANSLATION_DEDUP_INFLIGHT_TTL_SECONDS` | `2100` | TTL резерва «в работе» (не равен TTL отметки «готово») |

Несогласованные провайдеры ловятся на старте с понятным текстом: `http` без
`OCR_URL`, `manga` без модели, `LLM` без `LLM_BASE_URL`/`LLM_MODEL`, `DEEPL`
без ключа.

## Запуск

```powershell
# локально (dev, fake-провайдеры)
cd translation-worker
python -m venv .venv; .\.venv\Scripts\pip install -r requirements.txt
$env:PYTHONPATH = "src"
.\.venv\Scripts\python -m translation_worker            # /health и /ready на :8093

# тесты и линт
.\.venv\Scripts\python -m pytest -q
.\.venv\Scripts\python -m ruff check .

# в стеке compose (образ собирается из корня монорепо — нужен контракт asyncapi)
docker compose -f docker/docker-compose.yml up -d --build translation-worker
```

## Проверка

- `pytest -q` — 12 файлов тестов, все на fake-провайдерах: без Kafka, S3, Redis,
  GPU и моделей (`tests/conftest.py`).
- `ruff check .` — line-length 100, isort, py312.
- Пробы: `GET /health` → `200 {"status":"UP"}` (liveness, зависимости не
  проверяет — рестарт живого воркера не помогает), `GET /ready` → `200` с
  `dependencies` либо `503 {"status":"DOWN","dependencies":{"kafka":"down"}}`.
  Kafka-флаг ведёт цикл consume, Redis пингуется фоном раз в 15 с.

## Ограничения фазы 1

- Арт-слой не перерисовывается: `layer.json` — оверлей, `imageKey` = `null`.
- `kind` у всех блоков `BUBBLE`; SFX отдельной политикой не выделяются (D-008).
- `manga-ocr` возвращает текст всей страницы без рамок — боксы баблов даёт
  внешний OCR-сервис (`TRANSLATION_OCR_PROVIDER=http`); ML-стек в образ не входит.
- Глоссарий применяется LLM-провайдером; у DeepL glossary-ресурс появится в фазе 3.
- Топики создаёт `kafka-init` (в этом worktree топики перевода объявляет
  карточка 54/56 — сервис их только потребляет).
