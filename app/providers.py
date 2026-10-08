from abc import ABC, abstractmethod
import json
import re
from typing import Any, Dict, Optional, Tuple
import httpx
from app.config import settings
from app.formatter import format_payload, sanitize_mentions, truncate

_WEBHOOK_SECRET_RE = re.compile(
    r"(/api/webhooks/\d+/)([A-Za-z0-9_\-\.]+)|(/services/T[A-Za-z0-9_]+/B[A-Za-z0-9_]+/)([A-Za-z0-9_\-\.]+)",
    re.IGNORECASE
)
_URL_QUERY_OR_AUTH_RE = re.compile(
    r"(https?://)([^/\s:@]+:[^/\s@]+@)?([^\s/?#]+)(/[^\s?#]*)?(\?[^\s#]*)?",
    re.IGNORECASE
)


def sanitize_error_message(raw_msg: Optional[str]) -> Optional[str]:
    """
    Redacts webhook secret tokens, URL userinfo credentials, and query strings from error
    messages and bounds length to settings.max_error_body_bytes.
    """
    if not raw_msg:
        return None
    cleaned = _WEBHOOK_SECRET_RE.sub(
        lambda m: f"{m.group(1) or m.group(3)}[REDACTED]",
        str(raw_msg)
    )
    cleaned = _URL_QUERY_OR_AUTH_RE.sub(
        lambda m: f"{m.group(1)}{'[REDACTED]@' if m.group(2) else ''}{m.group(3)}{m.group(4) or ''}{'?[REDACTED]' if m.group(5) else ''}",
        cleaned
    )
    return cleaned[: settings.max_error_body_bytes]


def format_safe_exception(exc: Exception, provider_name: str) -> str:
    """
    Produces a structured, credential-safe error record without persisting raw
    arbitrary exception strings that could leak destination URLs or tokens (HR-10).
    """
    exc_type = type(exc).__name__
    if isinstance(exc, (httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout, httpx.TimeoutException)):
        category = "network_timeout"
    elif isinstance(exc, httpx.ConnectError):
        category = "connection_error"
    elif isinstance(exc, ValueError) and "SSRF Protection" in str(exc):
        category = "ssrf_blocked"
    else:
        category = "transport_error"

    return json.dumps({
        "type": exc_type,
        "provider": provider_name,
        "category": category
    })


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


def read_bounded_error(resp: httpx.Response, include_body: bool = True) -> Optional[str]:
    """Reads at most settings.max_error_body_bytes characters from an error response and redacts secrets."""
    if resp.status_code < 300:
        return None
    if 300 <= resp.status_code < 400:
        return f"Redirect responses ({resp.status_code}) are disallowed by SSRF policy."
    if not include_body:
        return f"HTTP {resp.status_code}"
    try:
        raw_text = resp.text or ""
        return sanitize_error_message(raw_text)
    except Exception:
        return f"HTTP {resp.status_code}"


def _enforce_outbound_ssrf_guard(destination_url: str) -> Dict[str, Any]:
    """
    Re-validates destination URL and pins validated DNS resolution metadata right
    before outbound connection (HR-05).
    """
    from app.routing import resolve_and_pin_destination
    _, _, sni_extensions = resolve_and_pin_destination(
        destination_url,
        allow_private=settings.allow_private_destinations
    )
    return sni_extensions


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
        extensions = _enforce_outbound_ssrf_guard(destination_url)
        body = format_payload(event_type, payload, use_embeds=use_embeds)
        headers = {"Content-Type": "application/json"}
        if delivery_id:
            headers["X-HookRelay-Delivery-ID"] = delivery_id

        resp = await client.post(
            destination_url,
            json=body,
            headers=headers,
            follow_redirects=False,
            extensions=extensions
        )
        headers_summary = None
        if resp.status_code == 429:
            retry_after = extract_retry_after(resp)
            if retry_after is not None:
                headers_summary = f"Retry-After={retry_after}"
        error_msg = read_bounded_error(resp, include_body=True)
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
        extensions = _enforce_outbound_ssrf_guard(destination_url)
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

        resp = await client.post(
            destination_url,
            json=slack_body,
            headers=headers,
            follow_redirects=False,
            extensions=extensions
        )
        headers_summary = None
        if resp.status_code == 429:
            retry_after = extract_retry_after(resp)
            if retry_after is not None:
                headers_summary = f"Retry-After={retry_after}"
        error_msg = read_bounded_error(resp, include_body=True)
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
        extensions = _enforce_outbound_ssrf_guard(destination_url)
        headers = {
            "Content-Type": "application/json",
            "X-HookRelay-Event": event_type,
            "User-Agent": "HookRelay/2.1"
        }
        if delivery_id:
            headers["X-HookRelay-Delivery-ID"] = delivery_id

        resp = await client.post(
            destination_url,
            json=payload,
            headers=headers,
            follow_redirects=False,
            extensions=extensions
        )
        headers_summary = None
        if resp.status_code == 429:
            retry_after = extract_retry_after(resp)
            if retry_after is not None:
                headers_summary = f"Retry-After={retry_after}"
        # HR-10: Do not persist arbitrary response bodies from generic HTTP endpoints
        error_msg = read_bounded_error(resp, include_body=False)
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
