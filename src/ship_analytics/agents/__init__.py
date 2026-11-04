"""Multi-agent registry for AquaTrace AI.

A true multi-agent system decomposes the pipeline into cooperating specialists
that register their capabilities with a central orchestrator. The orchestrator
decomposes a request into a capability graph, dispatches it, and aggregates the
results into a single structured event.

Public surface:
    Agent, Capability, AgentMessage            from ship_analytics.agents.base
    AgentRegistry                              from ship_analytics.agents.registry
    Orchestrator, OrchestratorTrace            from ship_analytics.agents.orchestrator
"""

from ship_analytics.agents.base import Agent, AgentMessage, Capability
from ship_analytics.agents.orchestrator import AgentTask, Orchestrator, OrchestratorTrace
from ship_analytics.agents.registry import AgentRegistry
from ship_analytics.agents.runtime import get_orchestrator, get_registry

__all__ = [
    "Agent",
    "AgentMessage",
    "Capability",
    "AgentRegistry",
    "Orchestrator",
    "OrchestratorTrace",
    "AgentTask",
    "get_orchestrator",
    "get_registry",
]