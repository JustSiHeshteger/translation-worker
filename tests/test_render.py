"""Стадия RENDER: арт-слой страницы (карточка 58 §2.3, §5).

Проверяется главный инвариант карточки: сбой рендера НЕ валит книгу — страница
отдаётся оверлеем фазы 1 (`imageKey=null` + warning), книга доходит до
`COMPLETED`; исключение — `strict_render`, где сбой инпанта даёт `LAYOUT_FAILED`.
Плюс проводка настроек: `render_art=false` не создаёт рендер вовсе.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from pydantic import ValidationError
from translation_worker import events
from translation_worker.cache import TranslationCache
from translation_worker.config import Settings
from translation_worker.models import Box, Layer, TextBlock, TextLine
from translation_worker.pipeline import Pipeline
from translation_worker.providers.inpaint import FakeInpainter, InpaintError
from translation_worker.providers.render import ArtRenderer, build_art_renderer
from translation_worker.providers.typeset import Typesetter

BOOK = "018f0000-0000-7000-8000-000000000001"
PUBLIC_BASE = "http://minio.test/books-platform"
SCHEMA = (
    Path(__file__).resolve().parents[2]
    / "books-platform-openapi"
    / "asyncapi"
    / "comic-translation-events.yaml"
)


def lines(count: int = 2) -> list[TextLine]:
    return [
        TextLine(
            text=f"line {index}",
            box=Box(x=100, y=200 + index * 50, w=200, h=40),
            confidence=0.9,
        )
        for index in range(count)
    ]


@pytest.fixture
def prepared(settings):
    return settings.model_copy(update={"public_base": PUBLIC_BASE})


@pytest.fixture
def request_pages(request_payload):
    def _make(numbers: tuple[int, ...] = (1, 2)):
        return request_payload(
            pages=[
                {
                    "number": number,
                    "url": f"{PUBLIC_BASE}/processed/up-1/page/{number:03d}/original.jpg",
                }
                for number in numbers
            ]
        )

    return _make


def load_pages(storage, page_png, numbers: tuple[int, ...] = (1, 2)) -> None:
    for number in numbers:
        storage.put_bytes(f"processed/up-1/page/{number:03d}/original.jpg", page_png(600, 900))


def execute(config, storage, ocr, translator, payload, *, renderer=None):
    pipeline = Pipeline(
        config,
        storage,
        ocr,
        translator,
        TranslationCache(config, storage),
        renderer=renderer,
    )
    request = events.parse_requested(payload, path=SCHEMA)
    return asyncio.run(pipeline.process(request))


class BrokenInpainter:
    """Инпант недоступен: HttpInpainter на не-2xx (карточка 58 §2.1)."""

    def __init__(self) -> None:
        self.calls = 0

    async def erase(self, image: bytes, boxes: list[Box]) -> bytes:
        self.calls += 1
        raise InpaintError("inpaint service returned HTTP 503")


# ── Сборка рендера по настройкам ───────────────────────────────


def test_render_art_disabled_creates_no_renderer(settings) -> None:
    assert build_art_renderer(settings.model_copy(update={"render_art": False})) is None


def test_render_art_enabled_creates_art_renderer(settings) -> None:
    enabled = settings.model_copy(update={"render_art": True, "inpaint_provider": "fake"})

    renderer = build_art_renderer(enabled)

    assert isinstance(renderer, ArtRenderer)
    assert isinstance(renderer._inpainter, FakeInpainter), (  # noqa: SLF001 - проверка проводки
        "фабрика обязана собирать инпантер по настройкам, а не подставлять Noop"
    )


def test_http_inpainter_requires_url() -> None:
    with pytest.raises(ValueError, match="TRANSLATION_INPAINT_URL"):
        Settings(inpaint_provider="http", inpaint_url="")


# ── ArtRenderer ────────────────────────────────────────────────


def _layer(blocks: list[TextBlock]) -> Layer:
    return Layer(
        version=1,
        page=1,
        language="ru",
        source_language="en",
        width=600,
        height=900,
        provider="LLM",
        blocks=blocks,
    )


def _blocks() -> list[TextBlock]:
    return [
        TextBlock(
            id="p1-b1",
            box=Box(x=50, y=60, w=300, h=120),
            kind="BUBBLE",
            original="Hi",
            text="Привет",
            confidence=0.9,
        )
    ]


def test_art_renderer_repaints_page_via_inpainter(page_png) -> None:
    inpainter = FakeInpainter()

    rendered = asyncio.run(
        ArtRenderer(inpainter, Typesetter(), strict=False).render(
            page_png(600, 900, color=(255, 255, 255)), _layer(_blocks())
        )
    )

    assert rendered is not None
    assert inpainter.calls == [[Box(x=50, y=60, w=300, h=120)]], (
        "инпант получает рамки блоков с текстом"
    )


def test_art_renderer_skips_page_when_inpaint_fails(page_png) -> None:
    renderer = ArtRenderer(BrokenInpainter(), Typesetter(), strict=False)

    rendered = asyncio.run(renderer.render(page_png(600, 900), _layer(_blocks())))

    assert rendered is None, "без strict сбой рендера отдаёт None (оверлей фазы 1)"


def test_art_renderer_raises_when_strict(page_png) -> None:
    renderer = ArtRenderer(BrokenInpainter(), Typesetter(), strict=True)

    with pytest.raises(InpaintError):
        asyncio.run(renderer.render(page_png(600, 900), _layer(_blocks())))


def test_strict_generic_failure_reports_layout_failed(page_png) -> None:
    class BrokenTypesetter:
        def render(self, image: bytes, blocks: list[TextBlock]) -> bytes:
            raise ValueError("cannot render")

    renderer = ArtRenderer(FakeInpainter(), BrokenTypesetter(), strict=True)

    with pytest.raises(InpaintError) as caught:
        asyncio.run(renderer.render(page_png(600, 900), _layer(_blocks())))

    assert caught.value.code == "LAYOUT_FAILED", (
        "строгий режим обязан отдавать код контракта, а не INTERNAL_ERROR"
    )


def test_art_renderer_without_text_blocks_skips_inpaint(page_png) -> None:
    inpainter = FakeInpainter()

    rendered = asyncio.run(
        ArtRenderer(inpainter, Typesetter(), strict=True).render(
            page_png(600, 900), _layer([])
        )
    )

    assert rendered is None
    assert inpainter.calls == [], "без блоков перерисовывать нечего"


# ── Пайплайн ───────────────────────────────────────────────────


def test_render_art_true_fills_image_key_and_upload(
    prepared, storage, page_png, fake_ocr, fake_translator, request_pages
) -> None:
    load_pages(storage, page_png, numbers=(1,))
    fake_ocr.pages = [lines(1)]
    config = prepared.model_copy(update={"render_art": True, "inpaint_provider": "fake"})
    renderer = build_art_renderer(config)

    result = execute(config, storage, fake_ocr, fake_translator, request_pages(numbers=(1,)),
                     renderer=renderer)

    assert isinstance(result, events.ComicTranslationCompleted)
    assert result.pages[0].image_key == f"translated/{BOOK}/ru/page/001/web.webp"
    assert storage.exists(f"translated/{BOOK}/ru/page/001/web.webp"), (
        "перерисованная страница выгружена ключом страницы"
    )
    assert result.warnings == []


def test_art_page_is_uploaded_as_webp(
    prepared, storage, page_png, fake_ocr, fake_translator, request_pages
) -> None:
    load_pages(storage, page_png, numbers=(1,))
    fake_ocr.pages = [lines(1)]
    config = prepared.model_copy(update={"render_art": True, "inpaint_provider": "fake"})

    execute(config, storage, fake_ocr, fake_translator, request_pages(numbers=(1,)),
            renderer=build_art_renderer(config))

    assert storage.content_types[f"translated/{BOOK}/ru/page/001/web.webp"] == "image/webp"


def test_page_without_text_has_no_skip_warning(
    prepared, storage, page_png, fake_ocr, fake_translator, request_pages
) -> None:
    load_pages(storage, page_png, numbers=(1,))
    fake_ocr.pages = [[]]  # страница без единой строки OCR
    config = prepared.model_copy(update={"render_art": True, "inpaint_provider": "fake"})

    result = execute(config, storage, fake_ocr, fake_translator, request_pages(numbers=(1,)),
                     renderer=build_art_renderer(config))

    assert isinstance(result, events.ComicTranslationCompleted)
    assert result.pages[0].image_key is None
    assert not any("art layer skipped" in warning for warning in result.warnings), (
        "отсутствие текста — не сбой рендера, ложный warning не нужен"
    )


def test_render_failure_keeps_book_completed_with_warning(
    prepared, storage, page_png, fake_ocr, fake_translator, request_pages
) -> None:
    load_pages(storage, page_png, numbers=(1,))
    fake_ocr.pages = [lines(1)]
    renderer = ArtRenderer(BrokenInpainter(), Typesetter(), strict=False)

    result = execute(prepared, storage, fake_ocr, fake_translator, request_pages(numbers=(1,)),
                     renderer=renderer)

    assert isinstance(result, events.ComicTranslationCompleted)
    assert result.pages[0].image_key is None, "оверлей фазы 1 остаётся рабочим"
    assert any("art layer skipped" in warning for warning in result.warnings)
    assert not storage.exists(f"translated/{BOOK}/ru/page/001/web.webp")


def test_strict_render_fails_page_with_layout_failed(
    prepared, storage, page_png, fake_ocr, fake_translator, request_pages
) -> None:
    load_pages(storage, page_png, numbers=(1,))
    fake_ocr.pages = [lines(1)]
    renderer = ArtRenderer(BrokenInpainter(), Typesetter(), strict=True)

    result = execute(prepared, storage, fake_ocr, fake_translator, request_pages(numbers=(1,)),
                     renderer=renderer)

    assert isinstance(result, events.ComicTranslationFailed)
    assert result.error_code == "LAYOUT_FAILED"
    assert result.failed_page == 1


def test_render_disabled_uploades_nothing(
    prepared, storage, page_png, fake_ocr, fake_translator, request_pages
) -> None:
    load_pages(storage, page_png, numbers=(1,))
    fake_ocr.pages = [lines(1)]
    disabled = prepared.model_copy(update={"render_art": False, "inpaint_provider": "fake"})

    result = execute(disabled, storage, fake_ocr, fake_translator, request_pages(numbers=(1,)),
                     renderer=build_art_renderer(disabled))

    assert isinstance(result, events.ComicTranslationCompleted)
    assert result.pages[0].image_key is None
    assert not storage.exists(f"translated/{BOOK}/ru/page/001/web.webp")
    assert result.warnings == [], "без арт-слоя страница не «пропускается», просто нет рендера"
def test_renderer_from_settings_keeps_quality_and_font(settings, monkeypatch, tmp_path) -> None:
    from translation_worker.providers import render as render_module

    font = tmp_path / "font.ttf"
    font.write_bytes(b"not a real font")
    captured: dict[str, object] = {}

    class SpyTypesetter:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(render_module, "Typesetter", SpyTypesetter)
    enabled = settings.model_copy(
        update={
            "render_art": True,
            "typeset_quality": 50,
            "font_path": str(font),
        }
    )

    build_art_renderer(enabled)

    assert captured == {"quality": 50, "font_path": str(font)}, (
        "настройки качества и шрифта обязаны доходить до вёрстки"
    )


def test_renderer_from_settings_keeps_strict_flag(settings, monkeypatch) -> None:
    from translation_worker.providers import render as render_module

    captured: dict[str, object] = {}

    class SpyRenderer:
        def __init__(self, inpainter: object, typesetter: object, **kwargs: object) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(render_module, "ArtRenderer", SpyRenderer)
    enabled = settings.model_copy(
        update={"render_art": True, "inpaint_provider": "fake", "strict_render": True}
    )

    build_art_renderer(enabled)

    assert captured == {"strict": True}, "TRANSLATION_STRICT_RENDER обязан доходить до рендера"
def test_degenerate_boxes_skip_art_layer_instead_of_storing_original(
    prepared, storage, page_png, fake_translator, request_pages
) -> None:
    class DegenerateOcr:
        async def recognize(self, image: bytes):
            del image
            return [
                TextLine(
                    text="Hi",
                    box=Box(x=0, y=0, w=0, h=0),
                    confidence=0.9,
                )
            ]

    load_pages(storage, page_png, numbers=(1,))
    config = prepared.model_copy(update={"render_art": True, "inpaint_provider": "fake"})

    result = execute(
        config, storage, DegenerateOcr(), fake_translator, request_pages(numbers=(1,)),
        renderer=build_art_renderer(config),
    )

    assert isinstance(result, events.ComicTranslationCompleted)
    assert result.pages[0].image_key is None
    assert not storage.exists(f"translated/{BOOK}/ru/page/001/web.webp"), (
        "неперекодированный оригинал не должен появляться под ключом web.webp"
    )
    assert any("art layer skipped" in warning for warning in result.warnings)
# ── Стартовая валидация настроек арт-слоя ─────────────────────


def test_broken_font_path_is_rejected_at_startup() -> None:
    with pytest.raises(ValidationError, match="TRANSLATION_FONT_PATH"):
        Settings(font_path="/no/such/font.ttf")


def test_broken_font_path_is_rejected_without_art_layer_too() -> None:
    # тот же TTF грузит фаза 1: дефолтный прод-конфиг (шрифт задан, рендер
    # выключен) обязан падать на старте, а не на LAYOUT каждой книги
    with pytest.raises(ValidationError, match="TRANSLATION_FONT_PATH"):
        Settings(render_art=False, font_path="/no/such/font.ttf")


def test_strict_render_requires_art_layer() -> None:
    with pytest.raises(ValidationError, match="TRANSLATION_STRICT_RENDER"):
        Settings(strict_render=True)


def test_typeset_quality_is_bounded() -> None:
    with pytest.raises(ValidationError):
        Settings(typeset_quality=0)
    with pytest.raises(ValidationError):
        Settings(typeset_quality=101)


def test_typesetter_falls_back_on_unreadable_font(tmp_path, page_png, caplog) -> None:
    broken = tmp_path / "broken.ttf"
    broken.write_bytes(b"not a font")

    rendered = Typesetter(font_path=str(broken)).render(
        page_png(200, 200), [TextBlock(id="b1", box=Box(x=10, y=10, w=180, h=80), kind="BUBBLE",
                                       original="Hi", text="Привет", confidence=0.9)]
    )

    assert rendered is not None, "битый шрифт не должен отменять арт-слой"
    assert any("falling back" in message for message in caplog.messages)


def test_layout_font_falls_back_on_unreadable_file(tmp_path, caplog) -> None:
    from translation_worker.layout import _font

    broken = tmp_path / "broken.ttf"
    broken.write_bytes(b"not a font")

    assert _font(str(broken), 14) is not None
    assert any("falling back" in message for message in caplog.messages)
