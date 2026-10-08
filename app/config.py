from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    # Core Security
    github_webhook_secret: str = Field(
        default="development_webhook_secret",
        description="HMAC secret configured in GitHub Webhook settings"
    )

    # Downstream Notification Target
    discord_webhook_url: str = Field(
        default="https://discord.com/api/webhooks/mock/test",
        description="Discord Incoming Webhook URL"
    )

    # Database connection string
    # Defaults to local SQLite, but seamlessly supports postgresql+asyncpg://
    database_url: str = Field(
        default="sqlite+aiosqlite:///./hookrelay.db",
        description="Async database connection URL"
    )

    # Resiliency & Performance Limits
    max_payload_bytes: int = Field(
        default=5 * 1024 * 1024,
        description="Maximum accepted request body size (5MB)"
    )
    max_retries: int = Field(
        default=5,
        description="Max retry attempts for downstream Discord deliveries"
    )
    request_timeout_seconds: float = Field(
        default=10.0,
        description="HTTP timeout for Discord webhook calls"
    )

    # Features
    enable_embeds: bool = Field(
        default=True,
        description="Format notifications as rich Discord embeds instead of raw text"
    )
    enable_reconciliation: bool = Field(
        default=True,
        description="Run background sweep task to recover stranded 'received' deliveries"
    )
    reconciliation_interval_seconds: int = Field(
        default=60,
        description="Interval between reconciliation sweep executions"
    )
    stale_threshold_seconds: int = Field(
        default=120,
        description="Age in seconds after which an uncompleted delivery is deemed stale"
    )

    # Environment
    environment: str = Field(
        default="development",
        description="Environment name: development, test, production"
    )


settings = Settings()
