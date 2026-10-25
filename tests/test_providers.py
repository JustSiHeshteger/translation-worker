"""Сборка провайдеров: настройки воркера обязаны доезжать до адаптеров."""

from __future__ import annotations

from translation_worker.config import Settings
from translation_worker.providers import build_ocr, build_translator
from translation_worker.providers.deepl import DeepLTranslator
from translation_worker.providers.http_ocr import HttpOcr
from translation_worker.providers.llm import LlmTranslator
from translation_worker.providers.stub import StubTranslator


def test_llm_provider_reads_model_and_custom_timeout() -> None:
    """Таймаут LLM — настройка развёртывания, а не константа адаптера.

    Локальная модель на CPU отвечает дольше облачной: жёсткие 60 секунд
    превращали бы корректный прогон в `TRANSLATION_FAILED`.
    """
    translator = build_translator(Settings(
        translate_provider="LLM",
        llm_base_url="http://host.docker.internal:11434/v1",
        llm_model="ornith-1.5:9b",
        llm_timeout=180.0,
    ))
    assert isinstance(translator, LlmTranslator)
    assert translator.base_url == "http://host.docker.internal:11434/v1"
    assert translator.model == "ornith-1.5:9b"
    assert translator.timeout == 180.0


def test_llm_timeout_defaults_to_sixty_seconds() -> None:
    """Дефолт остаётся прежним: облачные API укладываются в минуту."""
    assert Settings().llm_timeout == 60.0


def test_deepl_and_stub_selection() -> None:
    assert isinstance(build_translator(Settings()), StubTranslator)
    deepl = build_translator(Settings(translate_provider="DEEPL", deepl_api_key="key"))
    assert isinstance(deepl, DeepLTranslator)


def test_http_ocr_uses_configured_url() -> None:
    ocr = build_ocr(Settings(
        ocr_provider="http",
        ocr_url="http://ocr-service:8096/recognize",
    ))
    assert isinstance(ocr, HttpOcr)
    assert ocr.url == "http://ocr-service:8096/recognize"
