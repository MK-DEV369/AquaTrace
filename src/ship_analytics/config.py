from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration loaded from SHIP_* environment variables and .env."""

    model_config = SettingsConfigDict(env_file=".env", env_prefix="SHIP_", extra="ignore")

    kafka_brokers: str = "localhost:29092"
    database_url: str = "postgresql://ship:ship_dev_password@localhost:5432/ship"
    aisstream_api_key: str | None = None
    log_level: str = "INFO"
    jwt_secret: str = "change_this_jwt_secret_in_production"


@lru_cache
def get_settings() -> Settings:
    return Settings()

