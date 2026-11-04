"""Runtime singletons for the multi-agent system.

Importing this module creates the default registry and orchestrator lazily so
that importing ship_analytics does not trigger heavy model loading until needed.
"""

from __future__ import annotations

from functools import lru_cache

from ship_analytics.agents.orchestrator import Orchestrator
from ship_analytics.agents.registry import AgentRegistry
from ship_analytics.agents.specialists import build_default_registry


@lru_cache(maxsize=1)
def get_registry() -> AgentRegistry:
    return build_default_registry()


@lru_cache(maxsize=1)
def get_orchestrator(registry: AgentRegistry | None = None) -> Orchestrator:
    return Orchestrator(registry or get_registry())