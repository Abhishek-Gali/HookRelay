from typing import Any, Dict, List, Optional


def sanitize_mentions(text: str) -> str:
    """
    Prevents mention abuse (e.g. malicious or accidental @everyone or @here in commit messages/PR titles).
    Inserts a zero-width space (\u200b) between '@' and the word to defang pings.
    """
    if not text:
        return ""
    return (
        text.replace("@everyone", "@\u200beveryone")
            .replace("@here", "@\u200bhere")
    )


def truncate(text: str, max_length: int = 2000, suffix: str = "...") -> str:
    """Truncates text safely to avoid exceeding length bounds."""
    if not text or len(text) <= max_length:
        return text
    return text[: max_length - len(suffix)] + suffix


def format_push_content(payload: Dict[str, Any]) -> str:
    """Formats a git push event into clean markdown under 2000 chars."""
    repo_name = payload.get("repository", {}).get("full_name", "unknown/repo")
    pusher = payload.get("pusher", {}).get("name") or payload.get("sender", {}).get("login", "someone")
    ref = payload.get("ref", "unknown-branch").replace("refs/heads/", "")
    compare_url = payload.get("compare", "")
    commits = payload.get("commits", [])

    lines = [
        f"🔨 **[{sanitize_mentions(repo_name)}]** `{sanitize_mentions(ref)}`: {len(commits)} new commit(s) pushed by **{sanitize_mentions(pusher)}**"
    ]

    max_display = 5
    for commit in commits[:max_display]:
        commit_id = commit.get("id", "")[:7]
        raw_msg = commit.get("message", "").split("\n")[0]
        sanitized_msg = sanitize_mentions(raw_msg)
        commit_url = commit.get("url", "")
        lines.append(f"• [`{commit_id}`]({commit_url}) {sanitized_msg}")

    if len(commits) > max_display:
        lines.append(f"_... and {len(commits) - max_display} more commit(s)_")

    if compare_url:
        lines.append(f"[View Changes]({compare_url})")

    return truncate("\n".join(lines), 2000)


def format_issues_content(payload: Dict[str, Any]) -> str:
    """Formats an issue event into clean markdown."""
    action = payload.get("action", "updated")
    issue = payload.get("issue", {})
    repo_name = payload.get("repository", {}).get("full_name", "unknown/repo")
    sender = payload.get("sender", {}).get("login", "someone")
    title = sanitize_mentions(issue.get("title", "No Title"))
    url = issue.get("html_url", "")
    number = issue.get("number", "?")

    status_emoji = {
        "opened": "🟢",
        "closed": "🟣",
        "reopened": "🟡"
    }.get(action, "ℹ️")

    return truncate(
        f"{status_emoji} **[{sanitize_mentions(repo_name)}]** Issue #{number} {action} by **{sanitize_mentions(sender)}**\n"
        f"**Title**: [{title}]({url})",
        2000
    )


def format_pull_request_content(payload: Dict[str, Any]) -> str:
    """Formats a pull request event into clean markdown."""
    action = payload.get("action", "updated")
    pr = payload.get("pull_request", {})
    repo_name = payload.get("repository", {}).get("full_name", "unknown/repo")
    sender = payload.get("sender", {}).get("login", "someone")
    title = sanitize_mentions(pr.get("title", "No Title"))
    url = pr.get("html_url", "")
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

    return truncate(
        f"{emoji} **[{sanitize_mentions(repo_name)}]** Pull Request #{number} {action_text} by **{sanitize_mentions(sender)}**\n"
        f"**Title**: [{title}]({url})",
        2000
    )


def format_ping_content(payload: Dict[str, Any]) -> str:
    """Formats a GitHub webhook ping event."""
    repo = payload.get("repository", {}).get("full_name", "HookRelay")
    zen = payload.get("zen", "Keep it logically awesome.")
    return f"🔔 **GitHub Webhook Connected** for repository `{sanitize_mentions(repo)}`!\n_Zen: \"{zen}\"_"


def build_discord_embed(event_type: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Constructs a rich Discord Embed card tailored for GitHub events.
    Returns None if event should use raw text fallback.
    """
    repo = payload.get("repository", {})
    repo_name = repo.get("full_name", "GitHub Repository")
    repo_url = repo.get("html_url", "")
    sender = payload.get("sender", {})
    sender_name = sender.get("login", "GitHub")
    sender_avatar = sender.get("avatar_url", "")

    if event_type == "push":
        commits = payload.get("commits", [])
        ref = payload.get("ref", "").replace("refs/heads/", "")
        compare_url = payload.get("compare", "")
        commit_lines = []
        for c in commits[:5]:
            c_hash = c.get("id", "")[:7]
            c_msg = sanitize_mentions(c.get("message", "").split("\n")[0])
            c_url = c.get("url", "")
            commit_lines.append(f"[`{c_hash}`]({c_url}) {c_msg}")

        description = "\n".join(commit_lines) if commit_lines else "No commit descriptions."
        if len(commits) > 5:
            description += f"\n_... +{len(commits) - 5} more commits_"

        return {
            "title": f"[{repo_name}:{ref}] {len(commits)} new commit(s)",
            "url": compare_url or repo_url,
            "description": description[:2000],
            "color": 0x238636,  # GitHub Green
            "author": {
                "name": sender_name,
                "icon_url": sender_avatar
            },
            "footer": {
                "text": "HookRelay • Git Push Event"
            }
        }

    elif event_type == "issues":
        action = payload.get("action", "updated")
        issue = payload.get("issue", {})
        title = sanitize_mentions(issue.get("title", "Issue"))
        url = issue.get("html_url", "")
        number = issue.get("number", "?")

        color_map = {
            "opened": 0x238636,    # Green
            "closed": 0x8250DF,    # Purple
            "reopened": 0xD29922,  # Gold/Yellow
        }

        return {
            "title": f"Issue #{number}: {title}",
            "url": url,
            "description": f"**Action**: `{action}` by **{sender_name}**",
            "color": color_map.get(action, 0x5865F2),
            "author": {
                "name": f"{repo_name} • {sender_name}",
                "icon_url": sender_avatar,
                "url": repo_url
            },
            "footer": {
                "text": f"HookRelay • Issue {action.capitalize()}"
            }
        }

    elif event_type == "pull_request":
        action = payload.get("action", "updated")
        pr = payload.get("pull_request", {})
        title = sanitize_mentions(pr.get("title", "Pull Request"))
        url = pr.get("html_url", "")
        number = pr.get("number", "?")
        merged = pr.get("merged", False)

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

        return {
            "title": f"PR #{number}: {title}",
            "url": url,
            "description": f"**Status**: `{badge}` by **{sender_name}**\nBranch: `{pr.get('head', {}).get('ref', '')}` ➔ `{pr.get('base', {}).get('ref', '')}`",
            "color": color,
            "author": {
                "name": f"{repo_name} • {sender_name}",
                "icon_url": sender_avatar,
                "url": repo_url
            },
            "footer": {
                "text": f"HookRelay • PR {badge}"
            }
        }

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
        # Unknown or unhandled event: summarize safely without error
        repo_name = payload.get("repository", {}).get("full_name", "GitHub")
        action = payload.get("action", "")
        content = f"ℹ️ GitHub event `{sanitize_mentions(event_type)}` ({action}) received for `{sanitize_mentions(repo_name)}`."

    return {"content": content}
