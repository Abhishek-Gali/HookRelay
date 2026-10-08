from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple
import httpx
from app.config import settings
from app.formatter import format_payload, sanitize_mentions, truncate


def extract_retry_after(resp: httpx.Response) -> Optional[float]:
    """
    Safely parses Retry-After duration in seconds from HTTP 429 response headers or JSON body.
    Returns None if missing or malformed.
    """
    header_val = resp.headers.get("Retry-After")
    if header_val is not None:
        try:
            val = float(header_val.strip())
            if val >= 0:
                return val
        except (ValueError, TypeError):
            pass

    try:
        data = resp.json()
        if isinstance(data, dict) and "retry_after" in data:
            val = float(data["retry_after"])
            if val >= 0:
                return val
    except Exception:
        pass

    return None


def read_bounded_error(resp: httpx.Response) -> Optional[str]:
    """Reads at most settings.max_error_body_bytes characters from an error response."""
    if resp.status_code < 400:
        return None
    try:
        raw_text = resp.text or ""
        return raw_text[: settings.max_error_body_bytes]
    except Exception:
        return f"HTTP {resp.status_code}"


class NotificationProvider(ABC):
    """Abstract interface for all notification dispatch providers."""

    @abstractmethod
    def get_name(self) -> str:
        """Provider identifier, e.g. 'discord', 'slack', 'http'."""
        pass

    @abstractmethod
    async def send(
        self,
        event_type: str,
        payload: Dict[str, Any],
        destination_url: str,
        client: httpx.AsyncClient,
        use_embeds: bool = True,
        delivery_id: Optional[str] = None
    ) -> Tuple[int, Optional[str], Optional[str]]:
        """
        Executes dispatch to target destination.
        Returns: (http_status, bounded_error_message_or_none, response_headers_summary)
        """
        pass


class DiscordProvider(NotificationProvider):
    def get_name(self) -> str:
        return "discord"

    async def send(
        self,
        event_type: str,
        payload: Dict[str, Any],
        destination_url: str,
        client: httpx.AsyncClient,
        use_embeds: bool = True,
        delivery_id: Optional[str] = None
    ) -> Tuple[int, Optional[str], Optional[str]]:
        body = format_payload(event_type, payload, use_embeds=use_embeds)
        headers = {"Content-Type": "application/json"}
        if delivery_id:
            headers["X-HookRelay-Delivery-ID"] = delivery_id

        resp = await client.post(destination_url, json=body, headers=headers)
        headers_summary = None
        if resp.status_code == 429:
            retry_after = extract_retry_after(resp)
            if retry_after is not None:
                headers_summary = f"Retry-After={retry_after}"
        error_msg = read_bounded_error(resp)
        return resp.status_code, error_msg, headers_summary


class SlackProvider(NotificationProvider):
    def get_name(self) -> str:
        return "slack"

    async def send(
        self,
        event_type: str,
        payload: Dict[str, Any],
        destination_url: str,
        client: httpx.AsyncClient,
        use_embeds: bool = True,
        delivery_id: Optional[str] = None
    ) -> Tuple[int, Optional[str], Optional[str]]:
        repo = truncate(sanitize_mentions(payload.get("repository", {}).get("full_name", "Repository")), 120)
        sender = truncate(sanitize_mentions(payload.get("sender", {}).get("login", "GitHub")), 80)
        safe_event = truncate(sanitize_mentions(event_type), 60)

        text_summary = f"🔔 *GitHub Event: {safe_event}* in `{repo}` by *{sender}*"
        slack_body = {
            "text": text_summary,
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": text_summary
                    }
                }
            ]
        }
        headers = {"Content-Type": "application/json"}
        if delivery_id:
            headers["X-HookRelay-Delivery-ID"] = delivery_id

        resp = await client.post(destination_url, json=slack_body, headers=headers)
        headers_summary = None
        if resp.status_code == 429:
            retry_after = extract_retry_after(resp)
            if retry_after is not None:
                headers_summary = f"Retry-After={retry_after}"
        error_msg = read_bounded_error(resp)
        return resp.status_code, error_msg, headers_summary


class GenericHttpProvider(NotificationProvider):
    def get_name(self) -> str:
        return "http"

    async def send(
        self,
        event_type: str,
        payload: Dict[str, Any],
        destination_url: str,
        client: httpx.AsyncClient,
        use_embeds: bool = True,
        delivery_id: Optional[str] = None
    ) -> Tuple[int, Optional[str], Optional[str]]:
        headers = {
            "Content-Type": "application/json",
            "X-HookRelay-Event": event_type,
            "User-Agent": "HookRelay/2.1"
        }
        if delivery_id:
            headers["X-HookRelay-Delivery-ID"] = delivery_id

        resp = await client.post(destination_url, json=payload, headers=headers)
        headers_summary = None
        if resp.status_code == 429:
            retry_after = extract_retry_after(resp)
            if retry_after is not None:
                headers_summary = f"Retry-After={retry_after}"
        error_msg = read_bounded_error(resp)
        return resp.status_code, error_msg, headers_summary


PROVIDER_REGISTRY: Dict[str, NotificationProvider] = {
    "discord": DiscordProvider(),
    "slack": SlackProvider(),
    "http": GenericHttpProvider(),
}


def get_provider(name: str) -> NotificationProvider:
    """
    Returns provider instance by name.
    Explicitly raises ValueError on unknown providers instead of silently falling back to Discord.
    """
    if not name or not isinstance(name, str):
        raise ValueError("Provider name must be a non-empty string.")
    provider = PROVIDER_REGISTRY.get(name.strip().lower())
    if provider is None:
        raise ValueError(f"Unsupported notification provider: '{name}'. Valid providers: {list(PROVIDER_REGISTRY.keys())}")
    return provider
