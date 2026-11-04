from functools import lru_cache

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration loaded from SHIP_* environment variables and .env."""

    model_config = SettingsConfigDict(env_file=".env", env_prefix="SHIP_", extra="ignore")

    kafka_brokers: str = "localhost:29092"
    database_url: str = "postgresql://ship:ship_dev_password@localhost:5432/ship"
    aisstream_api_key: str | None = None
    log_level: str = "INFO"
    environment: str = "local"
    api_token: str | None = None

    # Required secrets (no fallbacks).
    jwt_secret: str = Field(..., description="JWT signing secret; must be set via env")
    jwt_algorithm: str = "HS256"

    # Stream processor
    checkpoint_dir: str = "/opt/ship/checkpoints"
    kafka_starting_offset: str = "latest"
    enable_prediction: bool = False

    # Kafka security (PLAINTEXT for local dev; SASL_SSL for prod)
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_sasl_mechanism: str | None = None
    kafka_sasl_username: str | None = None
    kafka_sasl_password: str | None = None
    kafka_ssl_ca_location: str | None = None

    # PostgreSQL SSL
    pg_sslmode: str = "prefer"
    pg_ssl_root_cert: str | None = None

    # LLM provider (ollama | transformers | openai-compatible)
    llm_provider: str = "ollama"
    llm_model: str = "qwen2.5:3b"
    llm_base_url: str = "http://localhost:11434"
    llm_api_key: str | None = None

    # Consumer retry / DLQ
    kafka_max_retries: int = 3
    kafka_dlq_topic: str = "ais.dlq"

    @field_validator("jwt_secret")
    @classmethod
    def _jwt_secret_must_be_set(cls, value: str) -> str:
        if not value or value.strip() in {"", "change_this_jwt_secret_in_production"}:
            raise ValueError(
                "SHIP_JWT_SECRET must be set to a strong, non-default value"
            )
        if len(value) < 32:
            raise ValueError("SHIP_JWT_SECRET must be at least 32 characters")
        return value

    @field_validator("kafka_starting_offset")
    @classmethod
    def _valid_offset(cls, value: str) -> str:
        if value not in {"earliest", "latest"}:
            raise ValueError("SHIP_KAFKA_STARTING_OFFSET must be 'earliest' or 'latest'")
        return value

    @model_validator(mode="after")
    def _require_production_transport_security(self) -> "Settings":
        if self.environment.lower() not in {"local", "development", "test"}:
            if self.kafka_security_protocol not in {"SSL", "SASL_SSL"}:
                raise ValueError("non-local environments require SSL or SASL_SSL for Kafka")
            if self.pg_sslmode not in {"require", "verify-ca", "verify-full"}:
                raise ValueError("non-local environments require verified PostgreSQL TLS")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()