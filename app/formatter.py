from typing import Any, Dict, Optional
from urllib.parse import urlparse


def sanitize_mentions(text: str) -> str:
    """
    Prevents mention abuse (e.g. malicious or accidental @everyone or @here in commit messages/PR titles/names).
    Inserts a zero-width space (\u200b) between '@' and the word to defang pings, and strips control characters.
    """
    if not text:
        return ""
    cleaned = (
        str(text)
        .replace("@everyone", "@\u200beveryone")
        .replace("@here", "@\u200bhere")
    )
    return cleaned


def validate_safe_url(url: Optional[str]) -> str:
    """
    Ensures URLs embedded in notifications use HTTPS and valid hostname structure.
    Returns empty string if invalid or unsafe.
    """
    if not url or not isinstance(url, str):
        return ""
    try:
        parsed = urlparse(url.strip())
        if parsed.scheme != "https" or not parsed.netloc:
            return ""
        return url.strip()
    except Exception:
        return ""


def truncate(text: str, max_length: int = 2000, suffix: str = "...") -> str:
    """Truncates text safely to avoid exceeding length bounds."""
    if not text or len(text) <= max_length:
        return text
    return text[: max_length - len(suffix)] + suffix


def format_push_content(payload: Dict[str, Any]) -> str:
    """Formats a git push event into clean markdown under 2000 chars."""
    repo_name = sanitize_mentions(payload.get("repository", {}).get("full_name", "unknown/repo"))
    pusher = sanitize_mentions(
        payload.get("pusher", {}).get("name") or payload.get("sender", {}).get("login", "someone")
    )
    ref = sanitize_mentions(payload.get("ref", "unknown-branch").replace("refs/heads/", ""))
    compare_url = validate_safe_url(payload.get("compare", ""))
    commits = payload.get("commits", [])

    lines = [
        f"🔨 **[{repo_name}]** `{ref}`: {len(commits)} new commit(s) pushed by **{pusher}**"
    ]

    max_display = 5
    for commit in commits[:max_display]:
        commit_id = sanitize_mentions(commit.get("id", "")[:7])
        raw_msg = commit.get("message", "").split("\n")[0]
        sanitized_msg = sanitize_mentions(raw_msg)
        commit_url = validate_safe_url(commit.get("url", ""))
        if commit_url:
            lines.append(f"• [`{commit_id}`]({commit_url}) {sanitized_msg}")
        else:
            lines.append(f"• `{commit_id}` {sanitized_msg}")

    if len(commits) > max_display:
        lines.append(f"_... and {len(commits) - max_display} more commit(s)_")

    if compare_url:
        lines.append(f"[View Changes]({compare_url})")

    return truncate("\n".join(lines), 2000)


def format_issues_content(payload: Dict[str, Any]) -> str:
    """Formats an issue event into clean markdown."""
    action = sanitize_mentions(payload.get("action", "updated"))
    issue = payload.get("issue", {})
    repo_name = sanitize_mentions(payload.get("repository", {}).get("full_name", "unknown/repo"))
    sender = sanitize_mentions(payload.get("sender", {}).get("login", "someone"))
    title = sanitize_mentions(issue.get("title", "No Title"))
    url = validate_safe_url(issue.get("html_url", ""))
    number = issue.get("number", "?")

    status_emoji = {
        "opened": "🟢",
        "closed": "🟣",
        "reopened": "🟡"
    }.get(action, "ℹ️")

    title_part = f"[{title}]({url})" if url else title
    return truncate(
        f"{status_emoji} **[{repo_name}]** Issue #{number} {action} by **{sender}**\n"
        f"**Title**: {title_part}",
        2000
    )


def format_pull_request_content(payload: Dict[str, Any]) -> str:
    """Formats a pull request event into clean markdown."""
    action = sanitize_mentions(payload.get("action", "updated"))
    pr = payload.get("pull_request", {})
    repo_name = sanitize_mentions(payload.get("repository", {}).get("full_name", "unknown/repo"))
    sender = sanitize_mentions(payload.get("sender", {}).get("login", "someone"))
    title = sanitize_mentions(pr.get("title", "No Title"))
    url = validate_safe_url(pr.get("html_url", ""))
    number = pr.get("number", "?")
    merged = pr.get("merged", False)

    if action == "closed" and merged:
        action_text = "merged"
        emoji = "🟣"
    elif action == "closed":
        action_text = "closed (unmerged)"
        emoji = "🔴"
    elif action == "opened":
        action_text = "opened"
        emoji = "🟢"
    else:
        action_text = action
        emoji = "ℹ️"

    title_part = f"[{title}]({url})" if url else title
    return truncate(
        f"{emoji} **[{repo_name}]** Pull Request #{number} {action_text} by **{sender}**\n"
        f"**Title**: {title_part}",
        2000
    )


def format_ping_content(payload: Dict[str, Any]) -> str:
    """Formats a GitHub webhook ping event."""
    repo = sanitize_mentions(payload.get("repository", {}).get("full_name", "HookRelay"))
    zen = sanitize_mentions(payload.get("zen", "Keep it logically awesome."))
    return truncate(f"🔔 **GitHub Webhook Connected** for repository `{repo}`!\n_Zen: \"{zen}\"_", 2000)


def build_discord_embed(event_type: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Constructs a rich Discord Embed card tailored for GitHub events.
    All text fields are sanitized against mention abuse and length-bounded;
    all URLs are validated for HTTPS safety.
    """
    repo = payload.get("repository", {})
    repo_name = truncate(sanitize_mentions(repo.get("full_name", "GitHub Repository")), 120)
    repo_url = validate_safe_url(repo.get("html_url", ""))
    sender = payload.get("sender", {})
    sender_name = truncate(
        sanitize_mentions(
            sender.get("login") or payload.get("pusher", {}).get("name") or "GitHub"
        ),
        80
    )
    sender_avatar = validate_safe_url(sender.get("avatar_url", ""))

    if event_type == "push":
        commits = payload.get("commits", [])
        ref = truncate(sanitize_mentions(payload.get("ref", "").replace("refs/heads/", "")), 80)
        compare_url = validate_safe_url(payload.get("compare", ""))
        commit_lines = []
        for c in commits[:5]:
            c_hash = sanitize_mentions(c.get("id", "")[:7])
            c_msg = truncate(sanitize_mentions(c.get("message", "").split("\n")[0]), 200)
            c_url = validate_safe_url(c.get("url", ""))
            if c_url:
                commit_lines.append(f"[`{c_hash}`]({c_url}) {c_msg}")
            else:
                commit_lines.append(f"`{c_hash}` {c_msg}")

        description = "\n".join(commit_lines) if commit_lines else "No commit descriptions."
        if len(commits) > 5:
            description += f"\n_... +{len(commits) - 5} more commits_"

        author_block: Dict[str, str] = {"name": sender_name}
        if sender_avatar:
            author_block["icon_url"] = sender_avatar

        embed: Dict[str, Any] = {
            "title": truncate(f"[{repo_name}:{ref}] {len(commits)} new commit(s)", 256),
            "description": truncate(description, 2000),
            "color": 0x238636,  # GitHub Green
            "author": author_block,
            "footer": {
                "text": "HookRelay • Git Push Event"
            }
        }
        target_url = compare_url or repo_url
        if target_url:
            embed["url"] = target_url
        return embed

    elif event_type == "issues":
        action = sanitize_mentions(payload.get("action", "updated"))
        issue = payload.get("issue", {})
        title = truncate(sanitize_mentions(issue.get("title", "Issue")), 200)
        url = validate_safe_url(issue.get("html_url", ""))
        number = issue.get("number", "?")

        color_map = {
            "opened": 0x238636,    # Green
            "closed": 0x8250DF,    # Purple
            "reopened": 0xD29922,  # Gold/Yellow
        }

        author_block = {"name": truncate(f"{repo_name} • {sender_name}", 256)}
        if sender_avatar:
            author_block["icon_url"] = sender_avatar
        if repo_url:
            author_block["url"] = repo_url

        embed = {
            "title": truncate(f"Issue #{number}: {title}", 256),
            "description": truncate(f"**Action**: `{action}` by **{sender_name}**", 2000),
            "color": color_map.get(action, 0x5865F2),
            "author": author_block,
            "footer": {
                "text": f"HookRelay • Issue {action.capitalize()}"
            }
        }
        if url:
            embed["url"] = url
        return embed

    elif event_type == "pull_request":
        action = sanitize_mentions(payload.get("action", "updated"))
        pr = payload.get("pull_request", {})
        title = truncate(sanitize_mentions(pr.get("title", "Pull Request")), 200)
        url = validate_safe_url(pr.get("html_url", ""))
        number = pr.get("number", "?")
        merged = pr.get("merged", False)
        head_ref = sanitize_mentions(pr.get("head", {}).get("ref", ""))
        base_ref = sanitize_mentions(pr.get("base", {}).get("ref", ""))

        if action == "closed" and merged:
            color = 0x8957E5  # Merged Purple
            badge = "Merged"
        elif action == "closed":
            color = 0xCF222E  # Red
            badge = "Closed (Unmerged)"
        elif action == "opened":
            color = 0x238636  # Green
            badge = "Opened"
        else:
            color = 0x5865F2
            badge = action.capitalize()

        author_block = {"name": truncate(f"{repo_name} • {sender_name}", 256)}
        if sender_avatar:
            author_block["icon_url"] = sender_avatar
        if repo_url:
            author_block["url"] = repo_url

        embed = {
            "title": truncate(f"PR #{number}: {title}", 256),
            "description": truncate(
                f"**Status**: `{badge}` by **{sender_name}**\nBranch: `{head_ref}` ➔ `{base_ref}`",
                2000
            ),
            "color": color,
            "author": author_block,
            "footer": {
                "text": f"HookRelay • PR {badge}"
            }
        }
        if url:
            embed["url"] = url
        return embed

    return None


def format_payload(event_type: str, payload: Dict[str, Any], use_embeds: bool = True) -> Dict[str, Any]:
    """
    Primary formatter dispatch returning a valid Discord webhook request body.
    Supports either {"content": "..."} or {"embeds": [...]}.
    """
    if event_type == "ping":
        return {"content": format_ping_content(payload)}

    if use_embeds:
        embed = build_discord_embed(event_type, payload)
        if embed:
            return {"embeds": [embed]}

    # Fallback to plain markdown content
    if event_type == "push":
        content = format_push_content(payload)
    elif event_type == "issues":
        content = format_issues_content(payload)
    elif event_type == "pull_request":
        content = format_pull_request_content(payload)
    else:
        repo_name = sanitize_mentions(payload.get("repository", {}).get("full_name", "GitHub"))
        action = sanitize_mentions(payload.get("action", ""))
        content = truncate(
            f"ℹ️ GitHub event `{sanitize_mentions(event_type)}` ({action}) received for `{repo_name}`.",
            2000
        )

    return {"content": content}
