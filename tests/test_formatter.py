import pytest
from app.formatter import (
    sanitize_mentions,
    truncate,
    format_payload,
    format_push_content,
    format_issues_content,
    format_pull_request_content
)


def test_sanitize_mentions():
    text = "Fixed critical issue for @everyone and notified @here!"
    sanitized = sanitize_mentions(text)
    assert "@everyone" not in sanitized
    assert "@here" not in sanitized
    assert "@\u200beveryone" in sanitized
    assert "@\u200bhere" in sanitized


def test_truncate():
    short = "hello world"
    assert truncate(short, 100) == short

    long_text = "a" * 2500
    truncated = truncate(long_text, 2000)
    assert len(truncated) == 2000
    assert truncated.endswith("...")


def test_push_format_embed():
    payload = {
        "repository": {"full_name": "acme/widget", "html_url": "https://github.com/acme/widget"},
        "pusher": {"name": "alice"},
        "ref": "refs/heads/main",
        "compare": "https://github.com/acme/widget/compare/abc...def",
        "commits": [
            {"id": "a1b2c3d4e5f6", "message": "feat: add user authentication", "url": "https://commit/1"},
            {"id": "b2c3d4e5f6a1", "message": "fix: resolve token expiry bug\nExtra lines here", "url": "https://commit/2"}
        ]
    }
    result = format_payload("push", payload, use_embeds=True)
    assert "embeds" in result
    embed = result["embeds"][0]
    assert "[acme/widget:main]" in embed["title"]
    assert "2 new commit(s)" in embed["title"]
    assert "feat: add user authentication" in embed["description"]
    assert "resolve token expiry bug" in embed["description"]


def test_issue_format():
    payload = {
        "action": "opened",
        "repository": {"full_name": "acme/widget"},
        "sender": {"login": "bob"},
        "issue": {
            "number": 42,
            "title": "Bug in billing calculation",
            "html_url": "https://github.com/acme/widget/issues/42"
        }
    }
    result = format_payload("issues", payload, use_embeds=False)
    assert "content" in result
    content = result["content"]
    assert "Issue #42 opened by **bob**" in content
    assert "Bug in billing calculation" in content


def test_pull_request_merged_format():
    payload = {
        "action": "closed",
        "repository": {"full_name": "acme/widget"},
        "sender": {"login": "charlie"},
        "pull_request": {
            "number": 105,
            "title": "New onboarding flow",
            "html_url": "https://github.com/acme/widget/pull/105",
            "merged": True,
            "head": {"ref": "feature/onboarding"},
            "base": {"ref": "main"}
        }
    }
    result = format_payload("pull_request", payload, use_embeds=True)
    embed = result["embeds"][0]
    assert "Merged" in embed["description"]
    assert embed["color"] == 0x8957E5  # Merged purple
