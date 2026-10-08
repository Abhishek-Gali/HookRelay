import fnmatch
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class RouteDestination(BaseModel):
    provider: str = Field(default="discord", description="discord, slack, or http")
    url: str = Field(description="Target webhook endpoint URL")


class RouteRule(BaseModel):
    name: str
    events: List[str] = Field(default_factory=list, description="List of events to match, e.g. ['push', 'pull_request'] or empty for all")
    repo_pattern: Optional[str] = Field(default=None, description="Wildcard pattern for repo full_name, e.g. 'Abhishek-Gali/*'")
    branch_pattern: Optional[str] = Field(default=None, description="Wildcard pattern for branch e.g. 'main'")
    destinations: List[RouteDestination]


class RoutingEngine:
    """
    Evaluates incoming GitHub events against configured routing and filtering rules
    to determine which destinations should receive notifications.
    """
    def __init__(self, routes: Optional[List[RouteRule]] = None):
        self.routes = routes or []

    def add_rule(self, rule: RouteRule):
        self.routes.append(rule)

    def resolve_destinations(self, event_type: str, payload: Dict[str, Any], default_url: str) -> List[RouteDestination]:
        """
        Matches event against all rules. If matches are found, returns the combined unique destinations.
        If no rules match or no rules are configured, falls back to default Discord destination.
        """
        matched_destinations: List[RouteDestination] = []

        repo = payload.get("repository", {}).get("full_name", "")
        ref = payload.get("ref", "").replace("refs/heads/", "")
        if not ref and "pull_request" in payload:
            ref = payload.get("pull_request", {}).get("base", {}).get("ref", "")

        for rule in self.routes:
            # 1. Event match
            if rule.events and event_type not in rule.events:
                continue

            # 2. Repo pattern match
            if rule.repo_pattern and not fnmatch.fnmatch(repo, rule.repo_pattern):
                continue

            # 3. Branch pattern match
            if rule.branch_pattern and not fnmatch.fnmatch(ref, rule.branch_pattern):
                continue

            # Rule matched!
            matched_destinations.extend(rule.destinations)

        if not matched_destinations and default_url:
            return [RouteDestination(provider="discord", url=default_url)]

        return matched_destinations


# Global routing engine initialized with default fallback
routing_engine = RoutingEngine()
