import fnmatch
import ipaddress
import json
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
from pydantic import BaseModel, Field, field_validator
from app.config import settings
from app.providers import PROVIDER_REGISTRY

BLOCKED_HOSTNAMES = {
    "localhost",
    "localhost.localdomain",
    "ip6-localhost",
    "ip6-loopback",
    "metadata.google.internal",
    "metadata",
    "instance-data",
}


def validate_ssrf_safe_url(url: str, allow_private: bool = False) -> str:
    """
    Validates destination webhook URLs against SSRF vectors:
    - Enforces HTTPS scheme (unless allow_private=True in local testing).
    - Blocks localhost, loopback (127.0.0.0/8, ::1), unspecified (0.0.0.0),
      private RFC1918 ranges (10/8, 172.16/12, 192.168/16),
      link-local / cloud metadata endpoints (169.254.169.254, 169.254.0.0/16, fe80::/10).
    """
    if not url or not isinstance(url, str):
        raise ValueError("Destination URL must be a non-empty string.")

    parsed = urlparse(url.strip())
    allowed_schemes = ("https", "http") if allow_private else ("https",)
    if parsed.scheme not in allowed_schemes:
        raise ValueError(f"Destination URL must use HTTPS scheme (got '{parsed.scheme}').")

    hostname = (parsed.hostname or "").strip().lower()
    if not hostname:
        raise ValueError("Destination URL must include a valid hostname.")

    if not allow_private:
        if hostname in BLOCKED_HOSTNAMES or hostname.endswith(".localhost") or hostname.endswith(".internal"):
            raise ValueError(f"SSRF Protection: Destination hostname '{hostname}' is forbidden.")

        # Check if hostname is an IP literal
        try:
            ip = ipaddress.ip_address(hostname)
            if (
                ip.is_loopback
                or ip.is_private
                or ip.is_link_local
                or ip.is_multicast
                or ip.is_reserved
                or ip.is_unspecified
            ):
                raise ValueError(
                    f"SSRF Protection: Private, loopback, or link-local IP address '{hostname}' is forbidden."
                )
        except ValueError as exc:
            if "SSRF Protection" in str(exc):
                raise
            # Hostname is a domain name, not an IP literal

    return url.strip()


class RouteDestination(BaseModel):
    provider: str = Field(default="discord", description="discord, slack, or http")
    url: str = Field(description="Target webhook endpoint URL")

    @field_validator("provider")
    @classmethod
    def validate_provider_name(cls, v: str) -> str:
        norm = (v or "").strip().lower()
        if norm not in PROVIDER_REGISTRY:
            raise ValueError(f"Unsupported provider: '{v}'. Must be one of {list(PROVIDER_REGISTRY.keys())}")
        return norm

    @field_validator("url")
    @classmethod
    def validate_url_ssrf(cls, v: str) -> str:
        return validate_ssrf_safe_url(v, allow_private=settings.allow_private_destinations)


class RouteRule(BaseModel):
    name: str
    events: List[str] = Field(
        default_factory=list,
        description="List of events to match, e.g. ['push', 'pull_request'] or empty for all"
    )
    repo_pattern: Optional[str] = Field(
        default=None,
        description="Wildcard pattern for repo full_name, e.g. 'Abhishek-Gali/*'"
    )
    branch_pattern: Optional[str] = Field(
        default=None,
        description="Wildcard pattern for branch e.g. 'main'"
    )
    destinations: List[RouteDestination]


def parse_persisted_destinations(
    destinations_raw: Optional[str],
    default_url: str
) -> List[RouteDestination]:
    """
    Reconstructs the exact RouteDestination list persisted at webhook ingestion time.
    Used by worker loops, reconciliation sweeps, and redrive operations so original
    routing is never lost.
    """
    if destinations_raw:
        try:
            parsed = json.loads(destinations_raw)
            if isinstance(parsed, list) and parsed:
                return [RouteDestination(**item) for item in parsed if isinstance(item, dict)]
        except Exception:
            pass

    if default_url:
        return [RouteDestination(provider="discord", url=default_url)]
    return []


class RoutingEngine:
    """
    Evaluates incoming GitHub events against configured routing and filtering rules
    to determine which destinations should receive notifications.
    """
    def __init__(self, routes: Optional[List[RouteRule]] = None):
        self.routes = routes or []

    def add_rule(self, rule: RouteRule) -> None:
        self.routes.append(rule)

    def resolve_destinations(
        self,
        event_type: str,
        payload: Dict[str, Any],
        default_url: str
    ) -> List[RouteDestination]:
        """
        Matches event against all rules. If matches are found, returns the combined destinations.
        If no rules match or no rules are configured, falls back to default Discord destination.
        """
        matched_destinations: List[RouteDestination] = []

        repo = payload.get("repository", {}).get("full_name", "")
        ref = payload.get("ref", "").replace("refs/heads/", "")
        if not ref and "pull_request" in payload:
            ref = payload.get("pull_request", {}).get("base", {}).get("ref", "")

        for rule in self.routes:
            if rule.events and event_type not in rule.events:
                continue
            if rule.repo_pattern and not fnmatch.fnmatch(repo, rule.repo_pattern):
                continue
            if rule.branch_pattern and not fnmatch.fnmatch(ref, rule.branch_pattern):
                continue
            matched_destinations.extend(rule.destinations)

        if not matched_destinations and default_url:
            return [RouteDestination(provider="discord", url=default_url)]

        return matched_destinations


routing_engine = RoutingEngine()
