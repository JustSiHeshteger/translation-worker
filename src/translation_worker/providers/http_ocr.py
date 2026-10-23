"""OCR через внешний HTTP-сервис распознавания (карточка 55 §2.10).

Ручка: `POST TRANSLATION_OCR_URL` multipart-полем `image`, ответ —
`{"lines": [{"text": ..., "box": {"x","y","w","h"}, "confidence": ...}]}`.
Любой не-2xx или нечитаемый ответ — `OcrError` (пайплайн отдаст `OCR_FAILED`).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from translation_worker.models import Box, TextLine
from translation_worker.providers.base import OcrError


@dataclass
class HttpOcr:
    """Провайдер OCR, у которого распознавание вынесено в отдельный сервис."""

    url: str
    timeout: float = 120.0

    async def recognize(self, image: bytes) -> list[TextLine]:
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    self.url, files={"image": ("page.png", image, "image/png")}
                )
        except httpx.HTTPError as exc:
            raise OcrError(f"OCR недоступен: {exc}") from exc
        if not 200 <= response.status_code < 300:
            raise OcrError(f"OCR ответил HTTP {response.status_code}")
        try:
            payload = response.json()
        except ValueError as exc:
            raise OcrError(f"OCR вернул не-JSON: {exc}") from exc
        return _parse_lines(payload)


def _parse_lines(payload: Any) -> list[TextLine]:
    raw_lines = payload.get("lines") if isinstance(payload, dict) else None
    if not isinstance(raw_lines, list):
        raise OcrError("OCR вернул ответ без поля lines")
    lines: list[TextLine] = []
    for item in raw_lines:
        if not isinstance(item, dict):
            continue
        text = str(item.get("text") or "").strip()
        box = item.get("box")
        if not text or not isinstance(box, dict):
            continue
        lines.append(
            TextLine(
                text=text,
                box=Box.from_json(box),
                confidence=float(item.get("confidence") or 0.0),
            )
        )
    return lines
