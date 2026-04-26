from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _default_env_files() -> tuple[Path, ...]:
    root = Path(__file__).resolve().parents[2]
    return (root / ".env.local", root / ".env")


class Settings(BaseSettings):
    """Application settings loaded from environment and optional .env files."""

    model_config = SettingsConfigDict(
        env_file=_default_env_files(),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    supabase_url: str = Field(default="", validation_alias="SUPABASE_URL")
    supabase_key: str = Field(default="", validation_alias="SUPABASE_KEY")
    supabase_anon_key: str = Field(default="", validation_alias="SUPABASE_ANON_KEY")
    supabase_service_role_key: str = Field(
        default="", validation_alias="SUPABASE_SERVICE_ROLE_KEY"
    )

    google_places_api_key: str = Field(
        default="", validation_alias="GOOGLE_PLACES_API_KEY"
    )
    google_maps_api_key: str = Field(
        default="", validation_alias="GOOGLE_MAPS_API_KEY"
    )

    openai_api_key: str = Field(default="", validation_alias="OPENAI_API_KEY")
    anthropic_api_key: str = Field(default="", validation_alias="ANTHROPIC_API_KEY")

    scraper_user_agent: str = Field(
        default="MilestoneDataBuilder/0.1",
        validation_alias="SCRAPER_USER_AGENT",
    )
    http_proxy: str = Field(default="", validation_alias="HTTP_PROXY")
    https_proxy: str = Field(default="", validation_alias="HTTPS_PROXY")
    request_timeout_seconds: float = Field(
        default=30.0, validation_alias="REQUEST_TIMEOUT_SECONDS"
    )

    scraper_delay_ms: int = Field(default=1000, validation_alias="SCRAPER_DELAY_MS")
    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")


def get_settings() -> Settings:
    return Settings()
