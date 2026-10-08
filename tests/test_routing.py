import pytest
from app.routing import RoutingEngine, RouteRule, RouteDestination


def test_routing_with_branch_filter():
    engine = RoutingEngine()
    engine.add_rule(RouteRule(
        name="prod-rule",
        events=["push"],
        branch_pattern="main",
        destinations=[RouteDestination(provider="discord", url="https://discord.prod")]
    ))
    engine.add_rule(RouteRule(
        name="dev-rule",
        events=["push"],
        branch_pattern="feature/*",
        destinations=[RouteDestination(provider="slack", url="https://slack.dev")]
    ))

    # Test main branch event
    payload_main = {"repository": {"full_name": "org/repo"}, "ref": "refs/heads/main"}
    dests_main = engine.resolve_destinations("push", payload_main, default_url="https://default")
    assert len(dests_main) == 1
    assert dests_main[0].url == "https://discord.prod"

    # Test feature branch event
    payload_feat = {"repository": {"full_name": "org/repo"}, "ref": "refs/heads/feature/login"}
    dests_feat = engine.resolve_destinations("push", payload_feat, default_url="https://default")
    assert len(dests_feat) == 1
    assert dests_feat[0].provider == "slack"
    assert dests_feat[0].url == "https://slack.dev"


def test_routing_fallback_to_default():
    engine = RoutingEngine()
    payload = {"repository": {"full_name": "org/repo"}, "ref": "refs/heads/main"}
    dests = engine.resolve_destinations("ping", payload, default_url="https://fallback.discord")
    assert len(dests) == 1
    assert dests[0].url == "https://fallback.discord"
