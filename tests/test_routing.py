import pytest
from pydantic import ValidationError
from app.providers import get_provider
from app.routing import (
    RoutingEngine,
    RouteRule,
    RouteDestination,
    parse_persisted_destinations,
)


def test_routing_with_branch_filter():
    engine = RoutingEngine()
    engine.add_rule(RouteRule(
        name="prod-rule",
        events=["push"],
        branch_pattern="main",
        destinations=[RouteDestination(provider="discord", url="https://discord.com/api/webhooks/prod")]
    ))
    engine.add_rule(RouteRule(
        name="dev-rule",
        events=["push"],
        branch_pattern="feature/*",
        destinations=[RouteDestination(provider="slack", url="https://hooks.slack.com/services/dev")]
    ))

    payload_main = {"repository": {"full_name": "org/repo"}, "ref": "refs/heads/main"}
    dests_main = engine.resolve_destinations("push", payload_main, default_url="https://discord.com/default")
    assert len(dests_main) == 1
    assert dests_main[0].url == "https://discord.com/api/webhooks/prod"

    payload_feat = {"repository": {"full_name": "org/repo"}, "ref": "refs/heads/feature/login"}
    dests_feat = engine.resolve_destinations("push", payload_feat, default_url="https://discord.com/default")
    assert len(dests_feat) == 1
    assert dests_feat[0].provider == "slack"
    assert dests_feat[0].url == "https://hooks.slack.com/services/dev"


def test_routing_fallback_to_default():
    engine = RoutingEngine()
    payload = {"repository": {"full_name": "org/repo"}, "ref": "refs/heads/main"}
    dests = engine.resolve_destinations("ping", payload, default_url="https://discord.com/fallback")
    assert len(dests) == 1
    assert dests[0].url == "https://discord.com/fallback"


def test_unknown_provider_rejected():
    """Proves typos in provider names raise ValueError instead of silently routing to Discord."""
    with pytest.raises(ValueError, match="Unsupported"):
        get_provider("slak")

    with pytest.raises(ValidationError):
        RouteDestination(provider="slak", url="https://hooks.slack.com/services/123")


def test_private_ip_destination_rejected():
    """Proves RFC1918 private IPs are blocked by SSRF validation."""
    for bad_ip in [
        "https://10.0.0.1/webhook",
        "https://172.16.0.5/webhook",
        "https://192.168.1.100/webhook",
    ]:
        with pytest.raises(ValidationError, match="SSRF Protection"):
            RouteDestination(provider="http", url=bad_ip)


def test_localhost_destination_rejected():
    """Proves localhost and loopback addresses are blocked by SSRF validation."""
    for loopback_url in [
        "https://localhost/webhook",
        "https://127.0.0.1:8000/webhook",
        "https://[::1]/webhook",
        "http://discord.com/api/webhooks/123",  # Non-HTTPS also rejected
    ]:
        with pytest.raises(ValidationError):
            RouteDestination(provider="http", url=loopback_url)


def test_metadata_endpoint_rejected():
    """Proves cloud metadata endpoints (169.254.169.254, metadata.google.internal) are blocked."""
    for meta_url in [
        "https://169.254.169.254/latest/meta-data/",
        "https://metadata.google.internal/computeMetadata/v1/",
    ]:
        with pytest.raises(ValidationError, match="SSRF Protection"):
            RouteDestination(provider="http", url=meta_url)


def test_original_route_preserved():
    """Proves parse_persisted_destinations restores the exact structured destinations saved at ingestion."""
    raw_saved = '[{"provider": "slack", "url": "https://hooks.slack.com/services/original"}]'
    restored = parse_persisted_destinations(raw_saved, default_url="https://discord.com/api/webhooks/new_default")
    assert len(restored) == 1
    assert restored[0].provider == "slack"
    assert restored[0].url == "https://hooks.slack.com/services/original"
