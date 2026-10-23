"""Конвейер страницы: DOWNLOAD → OCR → TRANSLATE → LAYOUT → S3 (карточка 55 §2.15).

`Pipeline.process` прогоняет книгу страница за страницей и возвращает
`ComicTranslationCompleted` либо `ComicTranslationFailed` — исключения наружу не
выпускаются, поэтому консьюмер всегда знает, что публиковать.

Решения, зафиксированные карточкой:
- отсутствие объекта/битые байты страницы → `PAGE_FETCH_FAILED`;
- пустой OCR страницы → warning `page N: no text`, слой пишется с пустыми
  блоками (читалка не получает 404), книга продолжается;
- сбой провайдера → `OCR_FAILED`/`TRANSLATION_FAILED`, общий таймаут → `TIMEOUT`,
  `pagesDone` в ошибке — число уже готовых страниц, `failedPage` — номер упавшей;
- прогресс публикуется после каждой страницы не чаще `progress_interval`,
  последнее событие всегда `percent=100`;
- повторный прогон того же контента берёт страницы из кэша по SHA-256 и
  переписывает `layer.json` теми же байтами (идемпотентность).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections.abc import Awaitable, Callable
from contextlib import AbstractContextManager, nullcontext
from datetime import UTC, datetime
from io import BytesIO
from time import monotonic
from typing import Any

import httpx
from PIL import Image, UnidentifiedImageError

from translation_worker import events
from translation_worker.cache import TranslationCache
from translation_worker.config import Settings
from translation_worker.grouping import group_lines
from translation_worker.layout import fit_blocks
from translation_worker.models import Layer, PageOutcome, TextBlock
from translation_worker.providers.base import (
    BatchTranslator,
    OcrProvider,
    PageRenderer,
    ProviderError,
    Translator,
)
from translation_worker.storage import (
    Storage,
    image_key,
    key_from_public_url,
    layer_base_key,
    layer_key,
    manifest_key,
)
from translation_worker.telemetry import StageTiming, log_task_summary

_LOG = logging.getLogger(__name__)

STAGE_DOWNLOAD = "DOWNLOAD"
STAGE_OCR = "OCR"
STAGE_TRANSLATE = "TRANSLATE"
STAGE_LAYOUT = "LAYOUT"

# Неисправимые коды контракта: ретрай не поможет, задание сразу в DLQ (§2.17)
NON_RETRYABLE_CODES = frozenset({"NOT_A_COMIC", "CORRUPTED_PAGE", "UNSUPPORTED_LANGUAGE"})

_HTTP_TIMEOUT = 60.0
_MESSAGE_LIMIT = 500


class _PageFailure(RuntimeError):
    """Сбой обработки задания с кодом ошибки и номером страницы."""

    def __init__(self, code: str, page: int | None, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.page = page


class _RunState:
    """Счётчики одного задания: нужны и при сбое, и при таймауте."""

    def __init__(self, request: events.ComicTranslationRequested, started_at: datetime) -> None:
        self.request = request
        self.started_at = started_at
        self.pages_total = len(request.pages)
        self.pages_done = 0
        self.current_page: int | None = None
        self.outcomes: list[PageOutcome] = []
        self.warnings: list[str] = []


class Pipeline:
    """Обработка задания перевода: страницы → слои в S3 + события исхода."""

    def __init__(
        self,
        settings: Settings,
        storage: Storage,
        ocr: OcrProvider,
        translator: Translator,
        cache: TranslationCache,
        *,
        clock: Callable[[], datetime] | None = None,
        renderer: PageRenderer | None = None,
        timings: StageTiming | None = None,
    ) -> None:
        self._settings = settings
        self._storage = storage
        self._ocr = ocr
        self._translator = translator
        self._cache = cache
        self._clock = clock or (lambda: datetime.now(UTC))
        self._renderer = renderer
        self._timings = timings

    async def process(
        self,
        request: events.ComicTranslationRequested,
        on_progress: Callable[[events.ComicTranslationProgress], Awaitable[None]] | None = None,
    ) -> events.ComicTranslationCompleted | events.ComicTranslationFailed:
        """Обработать задание целиком и вернуть событие исхода.

        В события исхода превращаются ровно два класса сбоев — таймаут задания
        и `_PageFailure` (код стадии + страница). Любые прочие исключения
        (например, недоступный S3 при записи манифеста) пробрасываются
        наружу: консьюмер превращает их в retryable `INTERNAL_ERROR`, поэтому
        молча «проглотить» их здесь нельзя.
        """
        if self._timings is not None:
            # тайминги принадлежат ОДНОМУ заданию (консьюмер живёт процессом)
            self._timings.reset()
        run = _RunState(request, self._now())
        try:
            async with asyncio.timeout(self._settings.timeout):
                return await self._execute(run, on_progress)
        except TimeoutError:
            return self._failure(run, "TIMEOUT", run.current_page, "превышен таймаут задания")
        except _PageFailure as failure:
            return self._failure(run, failure.code, failure.page, str(failure))

    # ── Стадии задания ────────────────────────────────────────────

    async def _execute(
        self,
        run: _RunState,
        on_progress: Callable[[events.ComicTranslationProgress], Awaitable[None]] | None,
    ) -> events.ComicTranslationCompleted:
        pages = run.request.pages[: self._settings.max_pages]
        if len(pages) != len(run.request.pages):
            _LOG.warning(
                "задание %s: страниц %d, обрабатываем первые %d (TRANSLATION_MAX_PAGES)",
                run.request.request_id,
                len(run.request.pages),
                len(pages),
            )
        run.pages_total = len(pages)
        reporter = _ProgressReporter(
            request=run.request,
            pages_total=run.pages_total,
            interval=self._settings.progress_interval,
            callback=on_progress,
        )
        for page in pages:
            run.current_page = page.number
            outcome = await self._process_page(run, page.number, page.url)
            run.outcomes.append(outcome)
            run.warnings.extend(outcome.warnings)
            run.pages_done += 1
            await reporter.report(STAGE_LAYOUT, run.pages_done)
        await reporter.report(STAGE_LAYOUT, run.pages_done, force=True)
        await self._write_manifest(run)
        return self._completed(run)

    async def _write_manifest(self, run: _RunState) -> None:
        """Манифест книги (§4): перечень готовых слоёв для читалки, без времени."""
        request = run.request
        manifest = {
            "version": 1,
            "bookId": request.book_id,
            "language": request.language,
            "sourceLanguage": request.source_language,
            "provider": request.provider,
            "pagesTotal": run.pages_total,
            "pagesDone": run.pages_done,
            "layerBaseKey": layer_base_key(request.book_id, request.language),
            "pages": [
                {
                    "number": outcome.number,
                    "layerKey": outcome.layer_key,
                    "blocks": outcome.blocks,
                }
                for outcome in run.outcomes
            ],
            "warnings": list(run.warnings),
        }
        await asyncio.to_thread(
            self._storage.put_json, manifest_key(request.book_id, request.language), manifest
        )

    async def _process_page(self, run: _RunState, number: int, url: str) -> PageOutcome:
        request = run.request
        image, width, height, digest = await self._download(number, url)
        cache_key = self._cache.key(
            language=request.language, provider=request.provider, sha256=digest
        )
        cached = await asyncio.to_thread(self._cache.load, cache_key)
        warnings: list[str] = []
        blocks = await self._recognize(run, number, image)
        if not blocks:
            warnings.append(f"page {number}: no text")
        # Кэш экономит платный перевод: уже переведённые строки этой картинки
        # (совпал SHA-256) берутся из записи, переводятся только новые.
        blocks = await self._translate(run, number, blocks, known=_known_texts(cached))
        laid_out, layout_warnings = await self._layout(number, blocks)
        warnings.extend(layout_warnings)
        layer = Layer(
            version=1,
            page=number,
            language=request.language,
            source_language=request.source_language,
            width=width,
            height=height,
            provider=request.provider,
            blocks=laid_out,
        )
        key = layer_key(request.book_id, request.language, number)
        # boto3 синхронный: вызов уходит в поток, иначе столл MinIO
        # блокирует event loop (heartbeat, прогресс и /health встают)
        await asyncio.to_thread(self._storage.put_json, key, layer.to_json())
        await asyncio.to_thread(
            self._cache.save, cache_key, _merge_blocks(cached, laid_out)
        )
        return PageOutcome(
            number=number,
            layer_key=key,
            image_key=await self._render_art(run, number, image, layer),
            blocks=len(laid_out),
            warnings=warnings,
        )

    async def _download(self, number: int, url: str) -> tuple[bytes, int, int, str]:
        """Скачать страницу, прочитать размеры и посчитать SHA-256 содержимого."""
        key = key_from_public_url(url, self._settings.public_base)
        data: bytes | None = None
        if key:
            try:
                data = await asyncio.to_thread(self._storage.get_bytes, key)
            except KeyError as exc:
                raise _PageFailure(
                    "PAGE_FETCH_FAILED", number, f"страница {number}: объект {key} не найден"
                ) from exc
        if data is None:
            data = await self._download_http(number, url)
        if not data:
            raise _PageFailure("PAGE_FETCH_FAILED", number, f"страница {number}: пустой объект")
        try:
            # размеры читает Pillow (синхронно) — в поток, чтобы не держать loop
            width, height = await asyncio.to_thread(_image_size, data)
        except (UnidentifiedImageError, OSError) as exc:
            raise _PageFailure(
                "PAGE_FETCH_FAILED", number, f"страница {number}: битое изображение: {exc}"
            ) from exc
        return data, width, height, hashlib.sha256(data).hexdigest()

    async def _download_http(self, number: int, url: str) -> bytes:
        """Фолбэк: публичная база не совпала — тянем страницу по HTTP."""
        try:
            async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
                response = await client.get(url)
        except httpx.HTTPError as exc:
            raise _PageFailure(
                "PAGE_FETCH_FAILED", number, f"страница {number}: HTTP-ошибка: {exc}"
            ) from exc
        if not 200 <= response.status_code < 300:
            raise _PageFailure(
                "PAGE_FETCH_FAILED", number, f"страница {number}: HTTP {response.status_code}"
            )
        return response.content

    async def _recognize(self, run: _RunState, number: int, image: bytes) -> list[TextBlock]:
        """OCR страницы и группировка строк в баблы."""
        try:
            with self._stage(STAGE_OCR):
                lines = await self._ocr.recognize(image)
        except ProviderError as exc:
            raise _PageFailure(exc.code, number, str(exc)) from exc
        except Exception as exc:
            raise _PageFailure("OCR_FAILED", number, _short(exc)) from exc
        blocks = group_lines(lines, min_confidence=self._settings.min_confidence)
        return [_with_id(block, number, index) for index, block in enumerate(blocks, 1)]

    async def _translate(
        self,
        run: _RunState,
        number: int,
        blocks: list[TextBlock],
        *,
        known: dict[str, str] | None = None,
    ) -> list[TextBlock]:
        """Перевести блоки страницы; уже переведённые (кэш) не переводятся заново."""
        if not blocks:
            return []
        request = run.request
        translated = dict(known or {})
        pending = [block for block in blocks if not translated.get(_normalized(block.original))]
        if pending:
            try:
                with self._stage(STAGE_TRANSLATE):
                    translations = await self._translate_blocks(
                        request, pending, request.glossary_map()
                    )
            except ProviderError as exc:
                raise _PageFailure(exc.code, number, str(exc)) from exc
            except Exception as exc:
                raise _PageFailure("TRANSLATION_FAILED", number, _short(exc)) from exc
            if len(translations) != len(pending):
                raise _PageFailure(
                    "TRANSLATION_FAILED",
                    number,
                    f"страница {number}: получено {len(translations)} переводов на "
                    f"{len(pending)} блоков",
                )
            for block, translation in zip(pending, translations, strict=True):
                translated[_normalized(block.original)] = translation
        return [
            TextBlock(
                id=block.id,
                box=block.box,
                kind=block.kind,
                original=block.original,
                text=translated[_normalized(block.original)],
                confidence=block.confidence,
            )
            for block in blocks
        ]

    async def _translate_blocks(
        self,
        request: events.ComicTranslationRequested,
        blocks: list[TextBlock],
        glossary: dict[str, str],
    ) -> list[str]:
        texts = [block.original for block in blocks]
        if isinstance(self._translator, BatchTranslator):
            return list(
                await self._translator.translate_page(
                    texts,
                    source=request.source_language,
                    target=request.language,
                    glossary=glossary,
                )
            )
        return [
            await self._translator.translate(
                text,
                source=request.source_language,
                target=request.language,
                glossary=glossary,
            )
            for text in texts
        ]

    async def _layout(
        self, number: int, blocks: list[TextBlock]
    ) -> tuple[list[TextBlock], list[str]]:
        """Подобрать кегль и переносы; блоки без текста отбрасываются.

        `fit_blocks` работает с Pillow синхронно — вызов уходит в поток, чтобы
        подбор кегля не останавливал event loop.
        """
        try:
            with self._stage(STAGE_LAYOUT):
                return await asyncio.to_thread(
                    fit_blocks, blocks, page=number, font_path=self._settings.font_path
                )
        except Exception as exc:
            raise _PageFailure("LAYOUT_FAILED", number, _short(exc)) from exc

    async def _render_art(
        self, run: _RunState, number: int, image: bytes, layer: Layer
    ) -> str | None:
        """Фаза 2: перерисовка страницы. В фазе 1 рендера нет — `imageKey=None`."""
        if self._renderer is None:
            return None
        rendered = await self._renderer.render(image, layer)
        key = image_key(run.request.book_id, run.request.language, number)
        await asyncio.to_thread(self._storage.put_bytes, key, rendered, "image/webp")
        return key

    # ── Исходы задания ────────────────────────────────────────────

    def _completed(self, run: _RunState) -> events.ComicTranslationCompleted:
        request = run.request
        duration_ms = self._duration_ms(run.started_at)
        completed = events.ComicTranslationCompleted(
            book_id=request.book_id,
            language=request.language,
            completed_at=self._now(),
            pages_done=run.pages_done,
            pages_total=run.pages_total,
            layer_base_key=layer_base_key(request.book_id, request.language),
            pages=[
                events.ComicTranslationPage(
                    number=outcome.number,
                    layer_key=outcome.layer_key,
                    image_key=outcome.image_key,
                    blocks=outcome.blocks,
                    warnings=list(outcome.warnings),
                )
                for outcome in run.outcomes
            ],
            provider=request.provider,
            duration_ms=duration_ms,
            warnings=list(run.warnings),
        )
        log_task_summary(
            book_id=request.book_id,
            language=request.language,
            provider=request.provider,
            pages_done=run.pages_done,
            pages_total=run.pages_total,
            duration_ms=duration_ms,
            timings=self._timings,
        )
        return completed

    def _failure(
        self, run: _RunState, code: str, page: int | None, message: str
    ) -> events.ComicTranslationFailed:
        request = run.request
        failed = events.ComicTranslationFailed(
            book_id=request.book_id,
            language=request.language,
            failed_at=self._now(),
            error_code=code,
            retryable=code not in NON_RETRYABLE_CODES,
            pages_done=run.pages_done,
            failed_page=page,
            message=_short(message),
        )
        log_task_summary(
            book_id=request.book_id,
            language=request.language,
            provider=request.provider,
            pages_done=run.pages_done,
            pages_total=run.pages_total,
            duration_ms=self._duration_ms(run.started_at),
            error_code=code,
            timings=self._timings,
        )
        return failed

    # ── Мелочи ────────────────────────────────────────────────────

    def _now(self) -> datetime:
        return self._clock()

    def _duration_ms(self, started_at: datetime) -> int:
        return max(0, int((self._now() - started_at).total_seconds() * 1000))

    def _stage(self, stage: str) -> AbstractContextManager[Any]:
        if self._timings is None:
            return nullcontext()
        return self._timings.measure(stage)


class _ProgressReporter:
    """Публикация прогресса: не чаще интервала, только вперёд, финал — 100 %."""

    def __init__(
        self,
        *,
        request: events.ComicTranslationRequested,
        pages_total: int,
        interval: float,
        callback: Callable[[events.ComicTranslationProgress], Awaitable[None]] | None,
    ) -> None:
        self._request = request
        self._pages_total = pages_total
        self._interval = interval
        self._callback = callback
        self._last_at: float | None = None

    async def report(self, stage: str, pages_done: int, *, force: bool = False) -> None:
        if self._callback is None:
            return
        moment = monotonic()
        if not force and self._last_at is not None and moment - self._last_at < self._interval:
            return
        self._last_at = moment
        await self._callback(
            events.ComicTranslationProgress(
                book_id=self._request.book_id,
                language=self._request.language,
                stage=stage,
                pages_done=pages_done,
                pages_total=self._pages_total,
                percent=100 if force else self._percent(pages_done),
            )
        )

    def _percent(self, pages_done: int) -> int:
        """Процент готовности 0…100 целым числом (контракт asyncapi, §4)."""
        if self._pages_total <= 0:
            return 100
        return min(100, round(pages_done * 100 / self._pages_total))


def _image_size(data: bytes) -> tuple[int, int]:
    """Размеры изображения без полной загрузки пикселей (Pillow, синхронно)."""
    with Image.open(BytesIO(data)) as picture:
        return picture.size


def _with_id(block: TextBlock, page: int, index: int) -> TextBlock:
    """Привязать блок к странице: `id = p{page}-b{i}` (формат карты цепочки)."""
    return TextBlock(
        id=f"p{page}-b{index}",
        box=block.box,
        kind=block.kind,
        original=block.original,
        text=block.text,
        confidence=block.confidence,
        font_size=block.font_size,
    )


def _normalized(text: str) -> str:
    """Ключ соответствия «оригинал → перевод» в кэше (пробелы нормализованы)."""
    return " ".join(text.split())


def _known_texts(cached: list[TextBlock] | None) -> dict[str, str]:
    """Переводы из записи кэша: `оригинал → готовый текст`."""
    known: dict[str, str] = {}
    for block in cached or []:
        original = _normalized(block.original)
        if original and block.text:
            known[original] = block.text
    return known


def _merge_blocks(cached: list[TextBlock] | None, fresh: list[TextBlock]) -> list[TextBlock]:
    """Слить запись кэша с блоками страницы (детерминированно, по оригиналу)."""
    merged: dict[str, TextBlock] = {}
    for block in cached or []:
        if _normalized(block.original):
            merged[_normalized(block.original)] = block
    for block in fresh:
        if _normalized(block.original):
            merged[_normalized(block.original)] = block
    return [merged[key] for key in sorted(merged)]


def _short(message: Any) -> str:
    """Читаемое сообщение для события: без переносов, не длиннее 500 символов."""
    return " ".join(str(message).split())[:_MESSAGE_LIMIT]


__all__ = ["NON_RETRYABLE_CODES", "Pipeline"]
