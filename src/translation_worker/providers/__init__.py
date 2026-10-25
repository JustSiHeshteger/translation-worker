"""Сборка провайдеров по настройкам (карточка 55 §2.7).

Единственное место, где настройки превращаются в конкретные адаптеры: остальной
код видит только порты `OcrProvider`/`Translator`/`PageRenderer`. Так тесты
подставляют fake'и, а compose/k8s переключают провайдера переменными окружения.
"""

from __future__ import annotations

from translation_worker.config import Settings
from translation_worker.providers.base import (
    BatchTranslator,
    OcrError,
    OcrProvider,
    PageRenderer,
    ProviderError,
    TranslationError,
    Translator,
)
from translation_worker.providers.deepl import DeepLTranslator
from translation_worker.providers.fake import FakeOcr, FakeTranslator
from translation_worker.providers.http_ocr import HttpOcr
from translation_worker.providers.inpaint import (
    FakeInpainter,
    HttpInpainter,
    Inpainter,
    InpaintError,
    NoopInpainter,
    build_inpainter,
)
from translation_worker.providers.llm import LlmTranslator
from translation_worker.providers.manga import MangaOcrProvider
from translation_worker.providers.render import ArtRenderer, build_art_renderer
from translation_worker.providers.stub import StubTranslator
from translation_worker.providers.typeset import Typesetter

__all__ = [
    "BatchTranslator",
    "DeepLTranslator",
    "FakeOcr",
    "FakeTranslator",
    "HttpOcr",
    "LlmTranslator",
    "MangaOcrProvider",
    "NoopInpainter",
    "InpaintError",
    "Inpainter",
    "FakeInpainter",
    "HttpInpainter",
    "ArtRenderer",
    "OcrError",
    "OcrProvider",
    "PageRenderer",
    "ProviderError",
    "StubTranslator",
    "TranslationError",
    "Translator",
    "Typesetter",
    "build_art_renderer",
    "build_inpainter",
    "build_ocr",
    "build_renderer",
    "build_translator",
]


def build_ocr(settings: Settings) -> OcrProvider:
    """OCR-провайдер по `TRANSLATION_OCR_PROVIDER`."""
    if settings.ocr_provider == "http":
        return HttpOcr(settings.ocr_url)
    if settings.ocr_provider == "manga":
        return MangaOcrProvider(settings.manga_model)
    return FakeOcr()


def build_translator(settings: Settings) -> Translator:
    """Переводчик по `TRANSLATION_TRANSLATE_PROVIDER`."""
    if settings.translate_provider == "LLM":
        return LlmTranslator(
            base_url=settings.llm_base_url,
            model=settings.llm_model,
            api_key=settings.llm_api_key,
            timeout=settings.llm_timeout,
        )
    if settings.translate_provider == "DEEPL":
        return DeepLTranslator(settings.deepl_api_key)
    return StubTranslator()



def build_renderer(settings: Settings) -> PageRenderer | None:
    """Рендер арт-слоя: `render_art=false` → рендера нет (поведение фазы 1)."""
    return build_art_renderer(settings)
