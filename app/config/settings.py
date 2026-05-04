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
    inbound_mail_attachment_dir: str = "data/Post_validation_data"
    output_dir: str = "output"
    max_upload_size_mb: int = 20

    # Pipeline behaviour
    anomaly_threshold: float = 0.85
    enable_ai_validation: bool = True

    # Celery configuration
    celery_broker_url: str = "redis://localhost:6379/0"
    celery_result_backend: str = "redis://localhost:6379/1"
    celery_timezone: str = "UTC"
    celery_enable_utc: bool = True
    celery_beat_call_task_minutes: int = 5

    # Mail processor defaults
    amea_europe_mail_from: str = "sa_gwz.gapi@ihg.onmicrosoft.com"
    amea_europe_mail_to: list[str] = ["GWZ_IA_RPA@ihg.com"]
    amea_europe_mail_cc: list[str] = ['sono.pathak2@ihg.com']
    amea_europe_mail_subject: str = "AMEA and Europe Billing Files"
    amea_europe_mail_template_name: str = "AMEA_and_Europe_Billing_Files"
    amea_europe_mail_body_type: str = "html"
    amea_europe_mail_attachments: list[str] = [
        "EMEAA/EMEAA_Intercompany/Output",
        "APAC/APAC_Intercompny/Output",
    ]
    
    # Error notification configuration
    error_notifications_mail: list[str] = ["sono.pathak2@ihg.com"]
    error_notifications_cc: list[str] = ["Shaik.Mohiddin@ihg.com"]  # Optional CC list for error notifications

    # SharePoint configuration
    sharepoint_tenant_id: str = ""
    sharepoint_client_id: str = ""
    sharepoint_client_secret: str = ""
    sharepoint_username: str = ""
    sharepoint_password: str = ""
    sharepoint_site_url: str = ""
    sharepoint_site_id: str = ""
    sharepoint_library_name: str = "Documents"
    sharepoint_download_root_path: str = "Documents/LMS Billing"
    sharepoint_timeout_seconds: int = 30

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


# Singleton used by the rest of the application
settings = Settings()  # type: ignore[call-arg]
