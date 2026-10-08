from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple
import time
import httpx
from app.formatter import format_payload, sanitize_mentions


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
        use_embeds: bool = True
    ) -> Tuple[int, Optional[str], Optional[str]]:
        """
        Executes dispatch to target destination.
        Returns: (http_status, error_message_or_none, response_headers_summary)
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
        use_embeds: bool = True
    ) -> Tuple[int, Optional[str], Optional[str]]:
        body = format_payload(event_type, payload, use_embeds=use_embeds)
        resp = await client.post(destination_url, json=body, headers={"Content-Type": "application/json"})
        headers_summary = f"Retry-After={resp.headers.get('Retry-After')}" if resp.status_code == 429 else None
        error_msg = resp.text if resp.status_code >= 400 else None
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
        use_embeds: bool = True
    ) -> Tuple[int, Optional[str], Optional[str]]:
        repo = sanitize_mentions(payload.get("repository", {}).get("full_name", "Repository"))
        sender = sanitize_mentions(payload.get("sender", {}).get("login", "GitHub"))
        
        # Slack Block Kit payload
        text_summary = f"🔔 *GitHub Event: {event_type}* in `{repo}` by *{sender}*"
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
        resp = await client.post(destination_url, json=slack_body, headers={"Content-Type": "application/json"})
        error_msg = resp.text if resp.status_code >= 400 else None
        return resp.status_code, error_msg, None


class GenericHttpProvider(NotificationProvider):
    def get_name(self) -> str:
        return "http"

    async def send(
        self,
        event_type: str,
        payload: Dict[str, Any],
        destination_url: str,
        client: httpx.AsyncClient,
        use_embeds: bool = True
    ) -> Tuple[int, Optional[str], Optional[str]]:
        headers = {
            "Content-Type": "application/json",
            "X-HookRelay-Event": event_type,
            "User-Agent": "HookRelay/2.0"
        }
        resp = await client.post(destination_url, json=payload, headers=headers)
        error_msg = resp.text if resp.status_code >= 400 else None
        return resp.status_code, error_msg, None


# Provider Registry
PROVIDER_REGISTRY: Dict[str, NotificationProvider] = {
    "discord": DiscordProvider(),
    "slack": SlackProvider(),
    "http": GenericHttpProvider()
}


def get_provider(name: str) -> NotificationProvider:
    """Returns provider instance by name; defaults to Discord if unspecified."""
    return PROVIDER_REGISTRY.get(name.lower(), PROVIDER_REGISTRY["discord"])
