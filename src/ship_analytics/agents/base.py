"""Base contracts for the multi-agent registry.

Every specialist in the system implements the :class:`Agent` protocol and
exposes one or more :class:`Capability` objects. Agents communicate only through
:class:`AgentMessage` envelopes, which carry a correlation id so that the whole
dispatch graph can be traced.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Protocol, runtime_checkable

logger = logging.getLogger("ship_analytics.agents")


class AgentStatus(str, Enum):
    """Lifecycle state of a registered agent."""

    REGISTERED = "registered"
    STARTING = "starting"
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    STOPPED = "stopped"
    FAILED = "failed"


class Capability:
    """A single, named capability an agent can perform.

    Capabilities are the unit of discovery: the orchestrator routes a request to
    the agent that declares the matching capability. A capability is described
    by a name, a free-form description (used for semantic routing), and a JSON
    schema for its input payload.
    """

    def __init__(
        self,
        name: str,
        description: str,
        input_schema: dict[str, Any] | None = None,
        safety_level: str = "safe",
        category: str = "general",
    ) -> None:
        self.name = name
        self.description = description
        self.input_schema = input_schema or {}
        self.safety_level = safety_level
        self.category = category

    def matches(self, requirement: str) -> bool:
        """Cheap lexical match used before semantic routing."""
        tokens = set(requirement.lower().split())
        desc_tokens = set(self.description.lower().split())
        return bool(tokens & desc_tokens) or self.name in requirement.lower()

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": self.input_schema,
            "safety_level": self.safety_level,
            "category": self.category,
        }


class AgentMessage:
    """Envelope exchanged between the orchestrator and agents.

    Carries a correlation id that survives the whole dispatch graph so the
    caller can trace every tool call, prediction, and query back to the
    original request.
    """

    def __init__(
        self,
        type: str,
        payload: dict[str, Any],
        correlation_id: str | None = None,
        source: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.type = type
        self.payload = payload
        self.correlation_id = correlation_id or uuid.uuid4().hex
        self.source = source
        self.metadata = metadata or {}
        self.timestamp = datetime.now(UTC).isoformat()

    def reply(self, type: str, payload: dict[str, Any]) -> AgentMessage:
        """Return a reply message that preserves the correlation id."""
        return AgentMessage(
            type=type,
            payload=payload,
            correlation_id=self.correlation_id,
            source=None,
            metadata={"in_reply_to": self.type},
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "payload": self.payload,
            "correlation_id": self.correlation_id,
            "source": self.source,
            "metadata": self.metadata,
            "timestamp": self.timestamp,
        }


@runtime_checkable
class Agent(Protocol):
    """Minimal protocol every specialist must satisfy.

    Implementations may be async or sync; the orchestrator handles both.
    """

    name: str
    capabilities: list[Capability]
    status: AgentStatus

    async def handle(self, message: AgentMessage) -> AgentMessage: ...

    async def health(self) -> dict[str, Any]: ...


class BaseAgent:
    """Convenience base class implementing the common bits of :class:`Agent`."""

    def __init__(self, name: str, description: str = "") -> None:
        self.name = name
        self.description = description
        self.capabilities: list[Capability] = []
        self.status = AgentStatus.REGISTERED
        self._started_at: str | None = None

    def register_capability(self, capability: Capability) -> None:
        """Register a capability with this agent."""
        if capability not in self.capabilities:
            self.capabilities.append(capability)

    def capabilities_for(self, requirement: str) -> list[Capability]:
        """Return capabilities that lexically match a requirement."""
        return [cap for cap in self.capabilities if cap.matches(requirement)]

    async def start(self) -> None:
        """Mark the agent as healthy. Override for real startup work."""
        self.status = AgentStatus.HEALTHY
        self._started_at = datetime.now(UTC).isoformat()
        logger.debug("agent %s healthy", self.name)

    async def stop(self) -> None:
        """Mark the agent as stopped. Override for real teardown."""
        self.status = AgentStatus.STOPPED
        logger.debug("agent %s stopped", self.name)

    async def health(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status.value,
            "capabilities": [c.name for c in self.capabilities],
            "started_at": self._started_at,
        }

    async def handle(self, message: AgentMessage) -> AgentMessage:  # pragma: no cover
        raise NotImplementedError(
            f"Agent {self.name} does not implement handle(); subclass BaseAgent"
        )


def run_handler(
    agent: Agent, message: AgentMessage
) -> Awaitable[AgentMessage]:
    """Wrap a sync or async agent handler into an awaitable."""
    result = agent.handle(message)
    if asyncio.iscoroutine(result):
        return result
    return _wrap_sync(result)


async def _wrap_sync(result: AgentMessage) -> AgentMessage:
    return result