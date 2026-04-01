# app/config/settings.py
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All runtime configuration values.

    Loaded from environment variables or a .env file at the project root.
    """

    # OpenAI / Azure OpenAI
    openai_api_key: str = ""
    openai_model: str = "gpt-4o-mini"

    # File handling
    upload_dir: str = "data"
    output_dir: str = "output"
    max_upload_size_mb: int = 20

    # Pipeline behaviour
    anomaly_threshold: float = 0.85
    enable_ai_validation: bool = True

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


# Singleton used by the rest of the application
settings = Settings()  # type: ignore[call-arg]
