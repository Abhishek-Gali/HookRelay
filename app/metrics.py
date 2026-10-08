from prometheus_client import Counter, Histogram

# Metric definitions
WEBHOOK_REQUESTS_TOTAL = Counter(
    "hookrelay_webhook_requests_total",
    "Total incoming GitHub webhook requests received",
    ["event", "outcome"]  # outcome: accepted, duplicate, ping, rejected_signature, rejected_size
)

DISCORD_DISPATCHES_TOTAL = Counter(
    "hookrelay_discord_dispatches_total",
    "Total messages dispatched to Discord",
    ["event", "status"]  # status: sent, failed
)

DISCORD_RETRIES_TOTAL = Counter(
    "hookrelay_discord_retries_total",
    "Total retry attempts made when delivering to Discord"
)

WEBHOOK_RESPONSE_SECONDS = Histogram(
    "hookrelay_webhook_response_seconds",
    "Latency of the immediate webhook ingestion endpoint",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]
)

RECONCILIATION_RUNS_TOTAL = Counter(
    "hookrelay_reconciliation_runs_total",
    "Total times the crash recovery sweep worker executed"
)

RECONCILIATION_RECOVERED_TOTAL = Counter(
    "hookrelay_reconciliation_recovered_total",
    "Total stuck 'received' deliveries recovered and dispatched"
)
