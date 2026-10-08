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

    # Authentication & RBAC for Management APIs
    # Default API keys for dev/testing (format: plain key; in production these can be rotated)
    admin_api_key: str = Field(
        default="hr_admin_secret_key_12345",
        description="Admin API Key with full control (read, redrive, config, replay)"
    )
    operator_api_key: str = Field(
        default="hr_operator_key_67890",
        description="Operator API Key for reading and redriving deliveries"
    )
    viewer_api_key: str = Field(
        default="hr_viewer_key_abcde",
        description="Viewer API Key with read-only access"
    )

    # Downstream Default Webhooks
    discord_webhook_url: str = Field(
        default="https://discord.com/api/webhooks/mock/test",
        description="Default Discord Incoming Webhook URL"
    )
    slack_webhook_url: str = Field(
        default="",
        description="Default Slack Incoming Webhook URL (optional)"
    )

    # Database connection string
    database_url: str = Field(
        default="sqlite+aiosqlite:///./hookrelay.db",
        description="Async database connection URL (SQLite or PostgreSQL)"
    )

    # Queue configuration
    redis_url: str = Field(
        default="",
        description="Optional Redis URL for distributed queue (e.g., redis://localhost:6379/0)"
    )

    # Rate Limiting
    rate_limit_requests_per_minute: int = Field(
        default=300,
        description="Max webhook requests per minute per IP"
    )

    # Resiliency Limits
    max_payload_bytes: int = Field(
        default=5 * 1024 * 1024,
        description="Maximum accepted request body size (5MB)"
    )
    max_retries: int = Field(
        default=5,
        description="Max retry attempts for downstream deliveries"
    )
    request_timeout_seconds: float = Field(
        default=10.0,
        description="HTTP timeout for webhook calls"
    )

    # Features
    enable_embeds: bool = Field(
        default=True,
        description="Format notifications as rich embeds"
    )
    enable_reconciliation: bool = Field(
        default=True,
        description="Run background sweep task to recover stranded deliveries"
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
