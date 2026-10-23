"""OCR на manga-ocr (карточка 55 §2.8/§2.3, requirements-ml.txt).

torch/manga-ocr в CI не ставится: импорт ML-стека — ленивый, внутри метода, а
модель создаётся один раз на процесс. Распознанный текст возвращается одной
строкой на всю страницу: детектор баблов в фазе 1 не выделяется (D-008), рамку
даёт сама страница — этого достаточно для оверлея.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from io import BytesIO
from typing import Any

from translation_worker.models import Box, TextLine
from translation_worker.providers.base import OcrError


@dataclass
class MangaOcrProvider:
    """Обёртка над manga-ocr с ленивой загрузкой модели."""

    model_name: str = "kha-white/manga-ocr-base"
    confidence: float = 1.0
    _model: Any = field(default=None, init=False, repr=False)

    async def recognize(self, image: bytes) -> list[TextLine]:
        # torch/manga-ocr — блокирующий CPU-код: уводим в поток, не блокируем loop
        return await asyncio.to_thread(self._recognize_sync, image)

    def _recognize_sync(self, image: bytes) -> list[TextLine]:
        from PIL import Image

        try:
            picture = Image.open(BytesIO(image))
            picture.load()
        except OSError as exc:
            raise OcrError(f"страница не читается как изображение: {exc}") from exc
        text = " ".join(str(self._predict(picture)).split())
        if not text:
            return []
        width, height = picture.size
        return [
            TextLine(
                text=text,
                box=Box(x=0, y=0, w=width, h=height),
                confidence=self.confidence,
            )
        ]

    def _predict(self, picture: Any) -> str:
        if self._model is None:
            try:
                from manga_ocr import MangaOcr
            except ImportError as exc:
                raise OcrError(
                    "manga-ocr не установлен: поставьте requirements-ml.txt "
                    "(TRANSLATION_OCR_PROVIDER=manga требует ML-стек)"
                ) from exc
            self._model = MangaOcr(pretrained_model_name_or_path=self.model_name)
        return self._model(picture)
