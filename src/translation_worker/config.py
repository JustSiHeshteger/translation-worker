"""Конфигурация сервиса (карточка 55 §2.2).

Все значения читаются из env с префиксом `TRANSLATION_` и имеют dev-дефолты,
поэтому сервис поднимается локально без единой переменной, а в compose/k8s
переопределяются только нужные. Несогласованные настройки провайдеров ловятся
на старте (`SettingsError` с понятным текстом), а не посреди задания.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

OcrProviderName = Literal["fake", "http", "manga"]
TranslateProviderName = Literal["LLM", "DEEPL", "STUB"]


class Settings(BaseSettings):
    """Настройки воркера: Kafka, S3, провайдеры, лимиты конвейера."""

    model_config = SettingsConfigDict(
        env_prefix="TRANSLATION_",
        env_file=None,
        extra="ignore",
        case_sensitive=False,
    )

    # ── Kafka ──────────────────────────────────────────────────────
    kafka_bootstrap: str = "localhost:29092"
    requests_topic: str = "comic-translations.requests"
    results_topic: str = "comic-translations.results"
    dlq_topic: str = "comic-translations.dlq"
    group_id: str = "translation-worker"

    # ── Хранилище (S3/MinIO) ───────────────────────────────────────
    s3_endpoint: str = "http://localhost:9100"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_region: str = "us-east-1"
    bucket: str = "books-platform"
    # Публичная база processed-бакета: из неё выводятся S3-ключи страниц
    public_base: str = "http://localhost:9100/books-platform"

    # ── Провайдеры OCR / перевода ──────────────────────────────────
    ocr_provider: OcrProviderName = "fake"
    ocr_url: str = ""
    manga_model: str = "kha-white/manga-ocr-base"
    llm_base_url: str = ""
    llm_api_key: str = ""
    llm_model: str = ""
    deepl_api_key: str = ""
    translate_provider: TranslateProviderName = "STUB"

    # ── Конвейер ───────────────────────────────────────────────────
    max_retries: int = Field(default=2, ge=0)
    # Общий таймаут задания в секундах (дефолт — 30 минут)
    timeout: float = Field(default=1800.0, gt=0)
    # Не чаще одного события прогресса на интервал, секунды
    progress_interval: float = Field(default=1.0, ge=0)
    max_pages: int = Field(default=1000, ge=1)
    min_confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    redis_url: str = "redis://localhost:6379/0"
    cache_prefix: str = "translation-cache/"
    dedup_prefix: str = "translation-cache/dedup:"
    dedup_ttl_seconds: int = Field(default=86_400, ge=1)
    font_path: str = ""
    schema_path: str = "schemas/comic-translation-events.yaml"
    health_port: int = Field(default=8093, ge=1, le=65_535)
    log_level: str = "INFO"

    @property
    def max_poll_interval_ms(self) -> int:
        """Kafka `max_poll_interval_ms` = таймаут задания + 5 минут (запас)."""
        return int((self.timeout + 300) * 1000)

    @model_validator(mode="after")
    def _validate_providers(self) -> Settings:
        if self.ocr_provider == "http" and not self.ocr_url:
            raise ValueError(
                "TRANSLATION_OCR_PROVIDER=http требует TRANSLATION_OCR_URL "
                "(адрес HTTP-сервиса распознавания)"
            )
        if self.ocr_provider == "manga" and not self.manga_model:
            raise ValueError(
                "TRANSLATION_OCR_PROVIDER=manga требует TRANSLATION_MANGA_MODEL "
                "(имя модели manga-ocr)"
            )
        if self.translate_provider == "LLM" and not (self.llm_base_url and self.llm_model):
            raise ValueError(
                "TRANSLATION_TRANSLATE_PROVIDER=LLM требует TRANSLATION_LLM_BASE_URL "
                "и TRANSLATION_LLM_MODEL"
            )
        if self.translate_provider == "DEEPL" and not self.deepl_api_key:
            raise ValueError(
                "TRANSLATION_TRANSLATE_PROVIDER=DEEPL требует TRANSLATION_DEEPL_API_KEY"
            )
        return self
