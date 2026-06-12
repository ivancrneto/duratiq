"""Runtime configuration, read from the environment / a local ``.env`` file."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Point at the same database the duratiq engine writes to.
    database_url: str = "sqlite:///./duratiq.db"
    # Single shared admin token. Empty => auth disabled (local dev only).
    admin_token: str = ""
    # Comma-separated list of allowed CORS origins (the frontend dev server).
    cors_origins: str = "http://localhost:5173"
    # Broker the duratiq workers consume (redis:// or amqp://). Required for the
    # Retry action, which enqueues a tick. Empty => retry is unavailable.
    broker_url: str = Field(default="", validation_alias="DURATIQ_BROKER_URL")
    # Queue the duratiq_tick actor listens on (matches DramatiqDriver's default).
    broker_queue: str = Field(default="duratiq", validation_alias="DURATIQ_BROKER_QUEUE")

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


settings = Settings()
