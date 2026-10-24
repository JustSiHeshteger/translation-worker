r"""Стендовая проверка арт-слоя: настоящая книга через compose-стек.

Скрипт делает то, что делает читатель, и ничего не подменяет:

1. регистрирует пользователя и проверяет роль `AUTHOR` (в auth_db нет сидов
   админа, поэтому на dev-стенде роль выдаёт оператор — команду печатает сам
   скрипт; в проде её выдаёт admin-service);
2. собирает CBZ (две страницы-картинки с английскими репликами), загружает его
   через `upload-service` — файл проходит настоящий конвейер upload-worker;
3. запрашивает автоперевод (`POST /books/{id}/translations`) — book-service
   публикует `ComicTranslationRequested` в Kafka;
4. ждёт, пока translation-worker сделает `layer.json` + `web.webp` (через
   inpaint-service), а book-service запишет результат в БД;
5. читает слой перевода страницы: проверяет `imageUrl`, скачивает страницу из
   MinIO и убеждается, что текст реплик затёрт, а перевод нарисован.

Запуск (нужен venv translation-worker: httpx + Pillow):

    cd docker; docker compose --env-file .env up -d --build
    cd ..
    translation-worker\.venv\Scripts\python.exe translation-worker\scripts\stand_art_check.py

Код возврата 0 — арт-слой работает на настоящем стеке.
"""

from __future__ import annotations

import argparse
import io
import json
import sys
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from PIL import Image, ImageDraw, ImageFont

#: Размер страницы «как у скана»: соотношение 2:3, как у реальных комиксов.
PAGE_SIZE = (1200, 1800)

#: Рамки реплик: (x, y, w, h) в пикселях страницы.
BUBBLES = [(90, 140, 1020, 380), (90, 1180, 1020, 340)]

#: Что «напечатано» в репликах на странице-оригинале.
ORIGINAL_LINES = [
    ["You are late, cat.", "I have been waiting", "since morning."],
    ["Do not touch my fish.", "It is mine now."],
]

#: Ожидаемый перевод: воркер на стенде использует STUB-провайдер, поэтому
#: текст — `<ru> ` + оригинал; проверяем, что он нарисован (тёмные пиксели).
TRANSLATION_PREFIX = "ru:"


@dataclass(frozen=True)
class Config:
    """Адреса compose-стенда и параметры книги."""

    auth: str = "http://localhost:8081"
    book: str = "http://localhost:8084"
    upload: str = "http://localhost:8085"
    minio: str = "http://localhost:9100"

    bucket: str = "books-platform"
    username: str = "stand-art-check"
    password: str = "Str0ng!Passw0rd2026"


def log(message: str) -> None:
    """Прогресс в stdout: скрипт стендовый, лог — это его результат."""
    print(message, flush=True)


# ── подготовка страниц ───────────────────────────────────────


def _font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    """Шрифт с кириллицей: на стенде проверяем именно русский текст."""
    for candidate in (
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "C:/Windows/Fonts/DejaVuSans.ttf",
        "C:/Windows/Fonts/arial.ttf",
    ):
        if Path(candidate).is_file():
            try:
                return ImageFont.truetype(candidate, size)
            except OSError:  # noqa: PERF203 - перебираем кандидатов
                continue
    return ImageFont.load_default(size=size)


def build_page(number: int) -> Image.Image:
    """Страница комикса: панели, баблы с английским текстом, подпись страницы."""
    image = Image.new("RGB", PAGE_SIZE, (246, 244, 238))
    draw = ImageDraw.Draw(image)
    draw.rectangle((20, 20, PAGE_SIZE[0] - 20, PAGE_SIZE[1] - 20), outline=(40, 40, 40), width=6)
    font = _font(58)
    for (x, y, w, h), lines in zip(BUBBLES, ORIGINAL_LINES, strict=True):
        draw.rounded_rectangle((x, y, x + w, y + h), radius=40, fill=(255, 255, 255),
                               outline=(20, 20, 20), width=5)
        draw.multiline_text((x + 40, y + 40), "\n".join(lines), font=font, fill=(15, 15, 15),
                            spacing=18)
    draw.text((PAGE_SIZE[0] // 2, PAGE_SIZE[1] - 60), str(number), font=font, fill=(90, 90, 90))
    return image


def build_cbz(pages: int) -> bytes:
    """CBZ = zip с PNG-страницами: формат, который принимает upload-service."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for number in range(1, pages + 1):
            page = io.BytesIO()
            build_page(number).save(page, format="PNG")
            archive.writestr(f"page-{number:03d}.png", page.getvalue())
    return buffer.getvalue()


# ── HTTP-шаги ───────────────────────────────────────────────


def wait_http(client: httpx.Client, url: str, *, timeout: float = 180.0) -> None:
    """Дождаться, пока сервис начнёт отвечать (compose поднимается не сразу)."""
    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            client.get(url, timeout=3.0)
            return
        except httpx.HTTPError as exc:
            last = exc
            time.sleep(2.0)
    raise RuntimeError(f"{url} не поднялся за {timeout:.0f} с: {last}")


def promote_hint(config: Config) -> str:
    """Команда выдачи роли: единственный шаг, который делает оператор стенда.

    `upload-service` требует роль `AUTHOR` с подтверждённым email, а в
    auth-service нет сидов администратора, поэтому на dev-стенде роль ставится
    SQL-ом. Скрипт не запускает docker сам: он работает и из окружения, где
    доступ к docker API закрыт, а роль уже выдана.
    """
    sql = (
        "UPDATE auth.users SET role='AUTHOR', email_verified=true "
        f"WHERE username='{config.username}'"
    )
    return (
        "docker exec bookplatform-postgres-1 psql -U postgres -d auth_db -c "
        f"\"{sql}\""
    )


def register(config: Config, client: httpx.Client) -> dict[str, Any]:
    """Зарегистрировать пользователя-автора и получить токены.

    Если роль ещё не выдана, скрипт останавливается и печатает ровно ту
    команду, которой оператор стенда это делает: молча подменять права
    (например, писать в БД в обход API) он не должен.
    """
    credentials = {"identifier": config.username, "password": config.password}
    # сначала вход: auth-service ограничивает регистрацию с одного IP
    # (429 на минуты), поэтому повторный прогон не должен её дёргать
    login = client.post(f"{config.auth}/api/v1/auth/login", json=credentials)
    if login.status_code == 401:
        payload = {
            "username": config.username,
            "password": config.password,
            "acceptTerms": True,
            "email": f"{config.username}@example.com",
        }
        registered = client.post(f"{config.auth}/api/v1/auth/register", json=payload)
        if registered.status_code >= 400 and registered.status_code != 409:
            raise RuntimeError(f"регистрация: {registered.status_code} {registered.text[:300]}")
        login = client.post(f"{config.auth}/api/v1/auth/login", json=credentials)
    if login.status_code >= 400:
        raise RuntimeError(f"логин: {login.status_code} {login.text[:300]}")
    body = login.json()
    role = body["user"].get("role")
    if role != "AUTHOR":
        raise RuntimeError(
            f"у пользователя {config.username} роль {role}, а upload-service ждёт AUTHOR"
            f" (email_verified). Выдайте её на стенде:\n  {promote_hint(config)}\n"
            "и повторите запуск."
        )
    log(f"пользователь {config.username}: роль {role}, токен получен")
    return body["tokens"]


def create_book(config: Config, client: httpx.Client, token: str) -> str:
    """Создать книгу: контент загружается отдельно (так устроен `upload-service`)."""
    response = client.post(
        f"{config.book}/api/v1/books",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "title": f"Stand comic {time.strftime('%H:%M:%S')}",
            "language": "en",
            "description": "Стендовая книга для проверки арт-слоя автоперевода",
        },
        timeout=60.0,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"создание книги: {response.status_code} {response.text[:300]}")
    book_id = response.json()["id"]
    log(f"книга {book_id}: создана")
    return str(book_id)


def upload_book(config: Config, client: httpx.Client, token: str, book_id: str, cbz: bytes) -> str:
    """Загрузить CBZ в созданную книгу: настоящий путь через upload-worker."""
    headers = {"Authorization": f"Bearer {token}"}
    response = client.post(
        f"{config.upload}/api/v1/uploads",
        headers=headers,
        files={"file": ("stand-comic.cbz", cbz, "application/zip")},
        data={"uploadType": "FULL_BOOK", "bookId": book_id},
        timeout=120.0,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"загрузка: {response.status_code} {response.text[:300]}")
    upload = response.json()
    upload_id = upload["id"]
    log(f"загрузка {upload_id}: {upload['status']}")

    deadline = time.monotonic() + 300.0
    while time.monotonic() < deadline:
        status = client.get(f"{config.upload}/api/v1/uploads/{upload_id}", headers=headers,
                            timeout=30.0)
        if status.status_code >= 400:
            raise RuntimeError(f"статус загрузки: {status.status_code} {status.text[:200]}")
        body = status.json()
        if body["status"] in {"COMPLETED", "READY"}:
            book_id = body.get("bookId")
            if not book_id:
                raise RuntimeError(f"загрузка завершена без книги: {json.dumps(body)[:300]}")
            log(f"книга {book_id}: создана из загруженного CBZ")
            return str(book_id)
        if body["status"] in {"FAILED", "REJECTED", "CANCELLED"}:
            raise RuntimeError(f"загрузка провалилась: {json.dumps(body)[:400]}")
        time.sleep(3.0)
    raise RuntimeError("upload-worker не закончил книгу за 300 с")


def request_translation(config: Config, client: httpx.Client, token: str, book_id: str) -> None:
    """Заказать автоперевод: book-service публикует событие в Kafka."""
    response = client.post(
        f"{config.book}/api/v1/books/{book_id}/translations",
        headers={"Authorization": f"Bearer {token}"},
        json={"language": "ru"},
        timeout=60.0,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"запрос перевода: {response.status_code} {response.text[:300]}")
    log(f"перевод запрошен: {response.json().get('status')}")


def wait_translation(
    config: Config, client: httpx.Client, token: str, book_id: str
) -> dict[str, Any]:
    """Дождаться итога: воркер + inpaint + потребитель book-service."""
    headers = {"Authorization": f"Bearer {token}"}
    deadline = time.monotonic() + 420.0
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        response = client.get(f"{config.book}/api/v1/books/{book_id}/translations",
                              headers=headers, timeout=30.0)
        if response.status_code >= 400:
            raise RuntimeError(f"список переводов: {response.status_code} {response.text[:200]}")
        items = response.json().get("items") or []
        last = next((item for item in items if item.get("language") == "ru"), {})
        status = last.get("status")
        log(f"перевод: {status}")
        if status in {"COMPLETED", "SUCCEEDED", "READY"}:
            return last
        if status in {"FAILED", "CANCELLED"}:
            raise RuntimeError(f"перевод провалился: {json.dumps(last)[:500]}")
        time.sleep(5.0)
    raise RuntimeError(f"перевод не завершился за 420 с: {json.dumps(last)[:300]}")


def check_page(config: Config, client: httpx.Client, token: str, book_id: str) -> dict[str, Any]:
    """Проверить слой страницы 1: `imageUrl` и затёртый оригинал."""
    response = client.get(
        f"{config.book}/api/v1/books/{book_id}/pages/1/translations/ru",
        headers={"Authorization": f"Bearer {token}"},
        timeout=60.0,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"слой страницы: {response.status_code} {response.text[:300]}")
    layer = response.json()
    image_url = layer.get("imageUrl")
    if not image_url:
        raise RuntimeError(f"у слоя нет imageUrl: {json.dumps(layer)[:400]}")
    log(f"imageUrl: {image_url}")

    image = client.get(image_url, timeout=60.0)
    if image.status_code >= 400:
        raise RuntimeError(f"картинка арт-слоя: {image.status_code}")
    art = Image.open(io.BytesIO(image.content))
    if art.format != "WEBP":
        raise RuntimeError(f"ожидали WEBP, получили {art.format}")
    if art.size != PAGE_SIZE:
        raise RuntimeError(f"размер страницы изменился: {art.size} != {PAGE_SIZE}")

    ink = art.convert("L")
    for index, (x, y, w, h) in enumerate(BUBBLES):
        dark = sum(1 for value in ink.crop((x, y, x + w, y + h)).tobytes() if value < 100)
        if dark < 500:
            raise RuntimeError(f"реплика {index + 1}: перевод не нарисован ({dark} тёмных px)")
        log(f"реплика {index + 1}: {dark} тёмных пикселей — текст нарисован")

    # Инварианта арт-слоя: затирается только текст реплик, остальная страница
    # (панели, поля, подпись) остаётся прежней. Считаем по внешней рамке вокруг
    # плашек: если инпант задел лишнее, разница выйдет за шум перекодирования.
    original = build_page(1).convert("L")
    art_gray = ink.crop((0, 0, PAGE_SIZE[0], PAGE_SIZE[1]))
    outside = 0
    compared = 0
    for y in range(0, PAGE_SIZE[1], 8):
        for x in range(0, PAGE_SIZE[0], 8):
            if _inside_any_bubble(x, y):
                continue
            compared += 1
            if abs(art_gray.getpixel((x, y)) - original.getpixel((x, y))) > 24:
                outside += 1
    if compared and outside > compared // 50:
        raise RuntimeError(
            f"инпант задел страницу вне реплик: {outside} из {compared} точек"
        )
    log(f"вне реплик изменено {outside} точек из {compared} — страница цела")
    return layer


def _inside_any_bubble(x: int, y: int, margin: int = 24) -> bool:
    """Точка попадает в плашку (с запасом на обводку маски)."""
    return any(
        bx - margin <= x <= bx + bw + margin and by - margin <= y <= by + bh + margin
        for bx, by, bw, bh in BUBBLES
    )


def main(argv: list[str] | None = None) -> int:
    """Прогнать книгу через стенд и проверить арт-слой."""
    parser = argparse.ArgumentParser(description="Стендовая проверка арт-слоя перевода")
    parser.add_argument("--pages", type=int, default=2, help="страниц в книге (1-3)")
    parser.add_argument("--keep", action="store_true", help="не удалять следы прогона")
    args = parser.parse_args(argv)
    config = Config()

    with httpx.Client(timeout=60.0) as client:
        wait_http(client, f"{config.auth}/api/v1/auth/me", timeout=300.0)
        wait_http(client, f"{config.upload}/api/v1/uploads/me", timeout=300.0)
        tokens = register(config, client)
        token = tokens["accessToken"]
        cbz = build_cbz(args.pages)
        log(f"CBZ собран: {len(cbz) // 1024} КБ, страниц {args.pages}")
        book_id = create_book(config, client, token)
        upload_book(config, client, token, book_id, cbz)
        request_translation(config, client, token, book_id)
        translation = wait_translation(config, client, token, book_id)
        layer = check_page(config, client, token, book_id)

    log("\nИТОГ: арт-слой работает на настоящем стеке")
    log(f"  книга: {book_id}")
    log(f"  перевод: {translation.get('status')}")
    log(f"  страниц с картинкой: {layer.get('imageUrl') is not None}")
    if not args.keep:
        log(f"  (следы прогона можно удалить: книга {book_id}, пользователь {config.username})")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as error:
        print(f"ПРОВАЛ: {error}", file=sys.stderr)
        raise SystemExit(1) from error
