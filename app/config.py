import secrets
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, model_validator


INSECURE_DEFAULTS = {
    "development_webhook_secret",
    "hr_admin_secret_key_12345",
    "hr_operator_key_67890",
    "hr_viewer_key_abcde",
    "super_secret_github_key_123",
    "changeme",
    "",
}


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    # Environment: development | test | production
    environment: str = Field(
        default="development",
        description="Runtime environment (development, test, production)"
    )

    # Core Webhook Security
    github_webhook_secret: str = Field(
        default="",
        description="HMAC secret configured in GitHub Webhook settings (min 16 chars in prod)"
    )

    # Authentication & RBAC for Management APIs
    admin_api_key: str = Field(
        default="",
        description="Admin API Key with full control (min 32 chars in production)"
    )
    operator_api_key: str = Field(
        default="",
        description="Operator API Key for reading and redriving deliveries"
    )
    viewer_api_key: str = Field(
        default="",
        description="Viewer API Key with read-only access"
    )

    # Downstream Default Webhooks
    discord_webhook_url: str = Field(
        default="",
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

    # Optional Redis URL for distributed rate limiting
    redis_url: str = Field(
        default="",
        description="Optional Redis URL (e.g. redis://localhost:6379/0)"
    )

    # Rate Limiting
    rate_limit_requests_per_minute: int = Field(
        default=300,
        description="Max webhook requests per minute per IP"
    )
    api_rate_limit_per_minute: int = Field(
        default=60,
        description="Max management API requests per minute per client"
    )
    redrive_rate_limit_per_minute: int = Field(
        default=10,
        description="Max redrive/replay operations per minute per client"
    )
    auth_failure_limit_per_minute: int = Field(
        default=10,
        description="Max failed authentication attempts per minute per IP before lockout"
    )

    # Resiliency & DoS Limits
    max_payload_bytes: int = Field(
        default=5 * 1024 * 1024,
        description="Maximum accepted request body size (5MB streaming cutoff)"
    )
    max_error_body_bytes: int = Field(
        default=2048,
        description="Maximum downstream error body characters read/logged"
    )
    max_retries: int = Field(
        default=5,
        description="Max retry attempts for downstream deliveries"
    )
    max_retry_after_seconds: float = Field(
        default=60.0,
        description="Maximum sleep duration honored from downstream Retry-After header"
    )
    request_timeout_seconds: float = Field(
        default=10.0,
        description="HTTP timeout for webhook calls"
    )

    # Worker Lease & Reconciliation Settings
    worker_lease_seconds: int = Field(
        default=120,
        description="Duration in seconds a worker holds an exclusive lease on a delivery"
    )
    worker_poll_interval_seconds: float = Field(
        default=0.2,
        description="Polling interval for database-backed durable job worker"
    )
    reconciliation_batch_size: int = Field(
        default=100,
        description="Number of stale deliveries recovered per batch"
    )
    reconciliation_max_batches_per_cycle: int = Field(
        default=50,
        description="Max batches processed in a single reconciliation sweep cycle"
    )
    reconciliation_interval_seconds: int = Field(
        default=60,
        description="Interval between reconciliation sweep executions"
    )
    stale_threshold_seconds: int = Field(
        default=120,
        description="Age in seconds after which an uncompleted delivery is deemed stale"
    )

    # Replay Protection & Data Retention Policy
    replay_window_seconds: int = Field(
        default=300,
        description="Bounded time window (seconds) in which identical signed webhook bodies with different delivery IDs are rejected as replays"
    )
    payload_retention_days: int = Field(
        default=14,
        description="Days to retain raw webhook payloads on completed deliveries before scrubbing"
    )
    attempt_retention_days: int = Field(
        default=30,
        description="Days to retain granular delivery attempt records"
    )
    audit_log_retention_days: int = Field(
        default=90,
        description="Days to retain security audit log records"
    )

    # Features & Access Control
    enable_embeds: bool = Field(
        default=True,
        description="Format notifications as rich embeds"
    )
    enable_reconciliation: bool = Field(
        default=True,
        description="Run background sweep task to recover stranded deliveries"
    )
    require_metrics_auth: bool = Field(
        default=True,
        description="Require authentication on /metrics endpoint (enabled by default)"
    )
    allow_private_destinations: bool = Field(
        default=False,
        description="Allow loopback/private IPs in webhook destinations (only for local testing)"
    )

    @model_validator(mode="after")
    def validate_security_configuration(self) -> "Settings":
        env = self.environment.lower()
        if env == "production":
            if self.allow_private_destinations:
                raise ValueError(
                    "CRITICAL: ALLOW_PRIVATE_DESTINATIONS cannot be enabled in production."
                )
            if not self.github_webhook_secret or self.github_webhook_secret in INSECURE_DEFAULTS or len(self.github_webhook_secret) < 16:
                raise ValueError(
                    "CRITICAL: GITHUB_WEBHOOK_SECRET must be set to a strong secret (>=16 chars) in production."
                )
            if not self.admin_api_key or self.admin_api_key in INSECURE_DEFAULTS or len(self.admin_api_key) < 32:
                raise ValueError(
                    "CRITICAL: ADMIN_API_KEY must be set to a cryptographically strong key (>=32 chars) in production."
                )
            for key_name, key_val in [("OPERATOR_API_KEY", self.operator_api_key), ("VIEWER_API_KEY", self.viewer_api_key)]:
                if key_val and (key_val in INSECURE_DEFAULTS or len(key_val) < 32):
                    raise ValueError(
                        f"CRITICAL: {key_name} cannot use a known default or weak key (<32 chars) in production."
                    )
        else:
            # In development/test, provide ephemeral or test-only fallbacks if not set via env
            if not self.github_webhook_secret:
                self.github_webhook_secret = "dev_only_webhook_secret_do_not_use_in_prod"
            if not self.admin_api_key:
                self.admin_api_key = "dev_only_admin_key_" + secrets.token_hex(16)
            if not self.operator_api_key:
                self.operator_api_key = "dev_only_operator_key_" + secrets.token_hex(16)
            if not self.viewer_api_key:
                self.viewer_api_key = "dev_only_viewer_key_" + secrets.token_hex(16)
            if not self.discord_webhook_url:
                self.discord_webhook_url = "https://discord.com/api/webhooks/000000000000000000/dev_placeholder"
        return self


settings = Settings()
