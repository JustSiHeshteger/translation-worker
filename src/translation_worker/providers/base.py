"""Порты провайдеров OCR/перевода/рендера (карточка 55 §2.7).

Провайдеры инъектируются (D-007): тесты и CI работают на fake-реализациях, а
production-адаптеры (HTTP OCR, LLM, DeepL, manga-ocr) подключаются по настройкам.
Ошибки провайдеров несут машинно-читаемый код из контракта
(`TranslationErrorCode`), чтобы пайплайн отдал нужный `errorCode` в событии.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from translation_worker.models import Layer, TextLine


class ProviderError(RuntimeError):
    """Ошибка провайдера с кодом из контракта событий."""

    code = "INTERNAL_ERROR"


class OcrError(ProviderError):
    """Распознавание не удалось."""

    code = "OCR_FAILED"


class TranslationError(ProviderError):
    """Перевод не удался."""

    code = "TRANSLATION_FAILED"


class OcrProvider(Protocol):
    """Распознавание текста на странице: байты картинки → строки с рамками."""

    async def recognize(self, image: bytes) -> list[TextLine]: ...


class Translator(Protocol):
    """Перевод одной реплики (по одному блоку за вызов)."""

    async def translate(
        self,
        text: str,
        *,
        source: str | None = None,
        target: str = "ru",
        glossary: dict[str, str] | None = None,
    ) -> str: ...


@runtime_checkable
class BatchTranslator(Protocol):
    """Перевод страницы пачкой реплик (LLM/DeepL): дешевле и точнее по контексту."""

    async def translate_page(
        self,
        texts: list[str],
        *,
        source: str | None = None,
        target: str = "ru",
        glossary: dict[str, str] | None = None,
    ) -> list[str]: ...


class PageRenderer(Protocol):
    """Рендер перерисованной страницы (фаза 2, карточка 58); в фазе 1 не используется.

    `None` — рендер не удался (или выключен): страница остаётся оверлеем фазы 1,
    книга продолжается (карточка 58 §2.3). Исключение — `strict_render`.
    """

    async def render(self, image: bytes, layer: Layer) -> bytes | None: ...
