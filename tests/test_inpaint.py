"""HTTP-адаптер инпанта (карточка 58 §2.1).

Единственный production-адаптер арт-слоя проверяется без сети: `httpx.MockTransport`
отдаёт заранее заданный ответ и запоминает запрос, поэтому фиксируются ровно те
гарантии контракта, на которые рассчитывает сервис инпанта, — имя и тип
multipart-полей, таймаут, реакция на не-2xx, пустое тело, не-картинку и
изменение размера страницы (боксы приходят в координатах оригинала).
"""

from __future__ import annotations

import asyncio
import io
import json

import httpx
import pytest
from PIL import Image
from translation_worker.models import Box
from translation_worker.providers.inpaint import (
    INPAINT_FAILED,
    FakeInpainter,
    HttpInpainter,
    InpaintError,
    NoopInpainter,
    build_inpainter,
)

BOXES = [Box(x=10, y=20, w=30, h=40)]


def png(width: int = 100, height: int = 80, color=(255, 255, 255)) -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (width, height), color).save(buffer, format="PNG")
    return buffer.getvalue()


def transport(handler) -> httpx.MockTransport:
    return httpx.MockTransport(handler)


def run(inpainter: HttpInpainter, image: bytes | None = None) -> bytes:
    return asyncio.run(inpainter.erase(image if image is not None else png(), BOXES))


def test_request_carries_page_and_boxes() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["method"] = request.method
        seen["content_type"] = request.headers["content-type"]
        seen["body"] = request.content
        return httpx.Response(200, content=png(color=(10, 20, 30)))

    inpainter = HttpInpainter("http://inpaint.test/erase", transport=transport(handler))
    result = run(inpainter)

    assert seen["method"] == "POST"
    assert seen["url"] == "http://inpaint.test/erase"
    assert str(seen["content_type"]).startswith("multipart/form-data; boundary=")
    body = str(seen["body"])
    assert 'name="image"' in body, "поле файла страницы обязано называться image"
    assert 'name="boxes"' in body, "поле рамок обязано называться boxes"
    assert json.dumps([{"x": 10, "y": 20, "w": 30, "h": 40}]) in body
    with Image.open(io.BytesIO(result)) as picture:
        assert picture.size == (100, 80)


def test_timeout_is_passed_to_the_http_client(monkeypatch) -> None:
    captured: dict[str, object] = {}
    real_client = httpx.AsyncClient

    def client_factory(**kwargs: object) -> httpx.AsyncClient:
        captured.update(kwargs)
        return real_client(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(httpx, "AsyncClient", client_factory)
    inpainter = HttpInpainter(
        "http://inpaint.test/erase",
        transport=transport(lambda _request: httpx.Response(200, content=png())),
    )

    run(inpainter)

    assert captured["timeout"] == 120.0, "тяжёлый инпант ограничен 120 с (карточка §2.1)"
    assert captured["transport"] is inpainter._transport, "транспорт доходит до клиента"


@pytest.mark.parametrize("status", [400, 404, 500, 503])
def test_non_2xx_becomes_layout_failed(status: int) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, content=png())

    inpainter = HttpInpainter("http://inpaint.test/erase", transport=transport(handler))

    with pytest.raises(InpaintError) as caught:
        run(inpainter)

    assert caught.value.code == INPAINT_FAILED == "LAYOUT_FAILED"
    assert str(status) in str(caught.value)


def test_empty_body_is_an_error() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"")

    inpainter = HttpInpainter("http://inpaint.test/erase", transport=transport(handler))

    with pytest.raises(InpaintError, match="empty body"):
        run(inpainter)


def test_non_image_body_is_an_error() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"<html>oops</html>")

    inpainter = HttpInpainter("http://inpaint.test/erase", transport=transport(handler))

    with pytest.raises(InpaintError, match="non-image"):
        run(inpainter)


def test_changed_page_size_is_an_error() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=png(width=50, height=40))

    inpainter = HttpInpainter("http://inpaint.test/erase", transport=transport(handler))

    with pytest.raises(InpaintError, match="page size"):
        run(inpainter)


def test_transport_failure_becomes_inpaint_error() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route to host")

    inpainter = HttpInpainter("http://inpaint.test/erase", transport=transport(handler))

    with pytest.raises(InpaintError) as caught:
        run(inpainter)

    assert caught.value.code == INPAINT_FAILED
    assert isinstance(caught.value.__cause__, httpx.HTTPError)


def test_noop_inpainter_returns_page_untouched() -> None:
    page = png()

    assert asyncio.run(NoopInpainter().erase(page, BOXES)) == page


def test_fake_inpainter_paints_boxes_and_records_calls() -> None:
    inpainter = FakeInpainter()

    result = asyncio.run(inpainter.erase(png(), BOXES))

    assert inpainter.calls == [BOXES]
    with Image.open(io.BytesIO(result)) as picture:
        assert picture.convert("RGB").getpixel((15, 25)) == (128, 128, 128)


def test_fake_inpainter_skips_degenerate_boxes() -> None:
    inpainter = FakeInpainter()

    result = asyncio.run(inpainter.erase(png(), [Box(x=1, y=1, w=0, h=0)]))

    with Image.open(io.BytesIO(result)) as picture:
        assert picture.size == (100, 80)


def test_factory_follows_settings(settings) -> None:
    assert isinstance(build_inpainter(settings.model_copy(update={"inpaint_provider": "none"})),
                      NoopInpainter)
    assert isinstance(build_inpainter(settings.model_copy(update={"inpaint_provider": "fake"})),
                      FakeInpainter)
    http = build_inpainter(
        settings.model_copy(update={"inpaint_provider": "http", "inpaint_url": "http://x/erase"})
    )
    assert isinstance(http, HttpInpainter)
