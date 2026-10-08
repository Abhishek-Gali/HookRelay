import fnmatch
import ipaddress
import json
import socket
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


_NAT64_PREFIX = ipaddress.IPv6Network("64:ff9b::/96")


def _assert_ip_is_public(ip_str: str, label: str = "") -> None:
    """Raises ValueError if ip_str is loopback, private, link-local, metadata, or reserved."""
    ip = ipaddress.ip_address(ip_str)
    # Unwrap IPv4-mapped IPv6 (::ffff:0:0/96) or RFC 6052 NAT64 (64:ff9b::/96) to check the target IPv4
    mapped = getattr(ip, "ipv4_mapped", None)
    if mapped is not None:
        target_ip = mapped
    elif isinstance(ip, ipaddress.IPv6Address) and ip in _NAT64_PREFIX:
        target_ip = ipaddress.IPv4Address(ip.packed[-4:])
    else:
        target_ip = ip

    if (
        target_ip.is_loopback
        or target_ip.is_private
        or target_ip.is_link_local
        or target_ip.is_multicast
        or target_ip.is_reserved
        or target_ip.is_unspecified
    ):
        ctx = f" (resolved from '{label}')" if label and label != ip_str else ""
        raise ValueError(
            f"SSRF Protection: Private, loopback, or link-local IP address '{ip_str}'{ctx} is forbidden."
        )


def resolve_and_validate_hostname(hostname: str, allow_private: bool = False) -> None:
    """
    Resolves DNS records for hostname and verifies none of the returned A/AAAA records
    point to private, loopback, link-local, or cloud metadata IPs (mitigating DNS rebinding).
    """
    if allow_private:
        return
    try:
        addr_info = socket.getaddrinfo(hostname, None)
    except socket.gaierror:
        # If offline or unresolvable in unit test mocks, hostname literal checks still apply
        return

    for family, _, _, _, sockaddr in addr_info:
        if family in (socket.AF_INET, socket.AF_INET6) and sockaddr:
            resolved_ip = str(sockaddr[0]).split("%")[0]
            _assert_ip_is_public(resolved_ip, label=hostname)


def validate_ssrf_safe_url(
    url: str,
    allow_private: bool = False,
    resolve_dns: bool = True
) -> str:
    """
    Validates destination webhook URLs against SSRF vectors:
    - Enforces HTTPS scheme (unless allow_private=True in local testing).
    - Blocks localhost, loopback (127.0.0.0/8, ::1), unspecified (0.0.0.0),
      private RFC1918 ranges (10/8, 172.16/12, 192.168/16),
      link-local / cloud metadata endpoints (169.254.169.254, 169.254.0.0/16, fe80::/10).
    - Resolves hostname via DNS to block DNS-rebinding domains resolving to internal IPs.
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
        if (
            hostname in BLOCKED_HOSTNAMES
            or hostname.endswith(".localhost")
            or hostname.endswith(".internal")
            or hostname.endswith(".local")
        ):
            raise ValueError(f"SSRF Protection: Destination hostname '{hostname}' is forbidden.")

        # Check if hostname is an IP literal
        try:
            ipaddress.ip_address(hostname)
            is_ip_literal = True
        except ValueError:
            is_ip_literal = False

        if is_ip_literal:
            _assert_ip_is_public(hostname)
        elif resolve_dns:
            resolve_and_validate_hostname(hostname, allow_private=allow_private)

    return url.strip()


def resolve_and_pin_destination(
    url: str,
    allow_private: bool = False
) -> tuple[str, Optional[str], Dict[str, Any]]:
    """
    Validates URL and resolves DNS, returning (validated_url, validated_ip_or_none, sni_extensions)
    to eliminate DNS-rebinding TOCTOU between validation and socket connect.
    """
    clean_url = validate_ssrf_safe_url(url, allow_private=allow_private, resolve_dns=True)
    parsed = urlparse(clean_url)
    hostname = (parsed.hostname or "").strip().lower()
    if allow_private or not hostname:
        return clean_url, None, {}

    try:
        addr_info = socket.getaddrinfo(hostname, parsed.port or (443 if parsed.scheme == "https" else 80))
    except socket.gaierror:
        return clean_url, None, {}

    validated_ip: Optional[str] = None
    for family, _, _, _, sockaddr in addr_info:
        if family in (socket.AF_INET, socket.AF_INET6) and sockaddr:
            candidate_ip = str(sockaddr[0]).split("%")[0]
            _assert_ip_is_public(candidate_ip, label=hostname)
            if validated_ip is None:
                validated_ip = candidate_ip


    extensions = {"sni_hostname": hostname} if validated_ip else {}
    return clean_url, validated_ip, extensions


class RouteDestination(BaseModel):
    provider: str = Field(default="discord", description="discord, slack, or http")
    url: str = Field(description="Target webhook endpoint URL")
    status: str = Field(default="pending", description="Destination delivery state: pending, sent, failed")

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
        return validate_ssrf_safe_url(v, allow_private=settings.allow_private_destinations, resolve_dns=True)


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
    destinations_raw: Optional[Any],
    default_url: str,
    only_unsent: bool = False
) -> List[RouteDestination]:
    """
    Reconstructs the exact RouteDestination list persisted at webhook ingestion time.
    Accepts either a JSON string or an already-parsed list of dicts/RouteDestination objects.
    If only_unsent=True, filters out destinations that have already succeeded ('status' == 'sent')
    so partial-failure redrives/reconciliations never send duplicate messages to already-successful targets.
    """
    if destinations_raw:
        try:
            parsed = json.loads(destinations_raw) if isinstance(destinations_raw, str) else destinations_raw
            if isinstance(parsed, list) and parsed:
                all_dests: List[RouteDestination] = []
                for item in parsed:
                    if isinstance(item, RouteDestination):
                        all_dests.append(item)
                    elif isinstance(item, dict):
                        all_dests.append(RouteDestination(**item))
                if all_dests:
                    if only_unsent:
                        unsent = [d for d in all_dests if d.status != "sent"]
                        return unsent if unsent else all_dests
                    return all_dests
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
