from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/jobq"

    # Worker
    worker_concurrency: int = 4
    poll_interval_seconds: float = 1.0
    job_timeout_seconds: float = 30.0
    # A job still "running" after this long is assumed to belong to a dead worker.
    visibility_timeout_seconds: float = 300.0
    retry_base_seconds: float = 2.0
    retry_max_seconds: float = 600.0

    # Webhooks
    webhook_secret: str = "dev-only-webhook-secret-change-me"
    webhook_timeout_seconds: float = 10.0
    webhook_max_attempts: int = 8


@lru_cache
def get_settings() -> Settings:
    return Settings()
