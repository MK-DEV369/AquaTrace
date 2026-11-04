"""Agent registry and discovery.

The registry is the single source of truth for "who can do what". It owns the
lifecycle of every agent and answers three questions:

    * register(agent)  - add an agent and start it
    * discover(capability) - which agents offer a capability?
    * route(message)   - dispatch a message to the best agent
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from ship_analytics.agents.base import Agent, AgentMessage, AgentStatus

logger = logging.getLogger("ship_analytics.agents.registry")


class AgentRegistry:
    """Central registry for all specialist agents."""

    def __init__(self) -> None:
        self._agents: dict[str, Agent] = {}
        self._order: list[str] = []

    def register(self, agent: Agent) -> None:
        """Register and start an agent.

        Registration is idempotent by name: re-registering replaces the previous
        instance after stopping it.
        """
        existing = self._agents.get(agent.name)
        if existing is not None and existing is not agent:
            try:
                asyncio.get_event_loop().create_task(existing.stop())
            except RuntimeError:
                pass
        self._agents[agent.name] = agent
        if agent.name not in self._order:
            self._order.append(agent.name)
        logger.info("registered agent %s (%d capabilities)", agent.name, len(agent.capabilities))

    def unregister(self, name: str) -> bool:
        """Remove an agent from the registry."""
        if name in self._agents:
            del self._agents[name]
            self._order = [n for n in self._order if n != name]
            return True
        return False

    def get(self, name: str) -> Agent | None:
        return self._agents.get(name)

    def list_agents(self) -> list[dict[str, Any]]:
        """Return a serialisable description of every registered agent."""
        result = []
        for name in self._order:
            agent = self._agents[name]
            result.append({
                "name": agent.name,
                "status": getattr(agent, "status", AgentStatus.REGISTERED).value
                if hasattr(getattr(agent, "status", None), "value")
                else str(getattr(agent, "status", "registered")),
                "capabilities": [c.to_dict() for c in agent.capabilities],
            })
        return result

    def discover(self, capability_name: str) -> list[Agent]:
        """Return every agent that offers a capability by exact name."""
        matches: list[Agent] = []
        for name in self._order:
            agent = self._agents[name]
            if any(c.name == capability_name for c in agent.capabilities):
                matches.append(agent)
        return matches

    def discover_by_requirement(self, requirement: str) -> list[Agent]:
        """Return agents whose capabilities lexically match a free-form requirement."""
        matches: list[Agent] = []
        for name in self._order:
            agent = self._agents[name]
            if any(c.matches(requirement) for c in agent.capabilities):
                matches.append(agent)
        return matches

    def discover_capability(self, name: str) -> Agent | None:
        """Return the first agent that offers the named capability."""
        for agent in self.discover(name):
            return agent
        return None

    async def start_all(self) -> None:
        """Start every registered agent."""
        for name in list(self._order):
            agent = self._agents[name]
            try:
                await agent.start()
            except Exception as exc:  # pragma: no cover - defensive
                logger.error("failed to start agent %s: %s", name, exc)
                agent.status = AgentStatus.FAILED

    async def stop_all(self) -> None:
        """Stop every registered agent in reverse registration order."""
        for name in reversed(list(self._order)):
            agent = self._agents[name]
            try:
                await agent.stop()
            except Exception as exc:  # pragma: no cover - defensive
                logger.error("failed to stop agent %s: %s", name, exc)

    async def health_all(self) -> dict[str, Any]:
        result = {}
        for name in self._order:
            agent = self._agents[name]
            try:
                result[name] = await agent.health()
            except Exception as exc:  # pragma: no cover - defensive
                result[name] = {"name": name, "status": "error", "error": str(exc)}
        return result

    async def route(self, message: AgentMessage, capability_name: str | None = None) -> AgentMessage:
        """Dispatch a message to the agent best suited to handle it.

        If ``capability_name`` is given, the first agent offering it is used.
        Otherwise the orchestrator's caller is expected to pre-select the agent
        via :meth:`discover_by_requirement`.
        """
        if capability_name:
            agent = self.discover_capability(capability_name)
            if agent is None:
                return AgentMessage(
                    type="error",
                    payload={"error": f"no agent offers capability {capability_name}"},
                    correlation_id=message.correlation_id,
                )
            return await agent.handle(message)
        # Default: try each agent in registration order until one accepts.
        for name in self._order:
            agent = self._agents[name]
            try:
                return await agent.handle(message)
            except NotImplementedError:
                continue
            except Exception as exc:
                logger.warning("agent %s failed to handle message: %s", name, exc)
                continue
        return AgentMessage(
            type="error",
            payload={"error": "no agent could handle the message"},
            correlation_id=message.correlation_id,
        )

    def __len__(self) -> int:
        return len(self._agents)

    def __contains__(self, name: str) -> bool:
        return name in self._agents

    def snapshot(self) -> dict[str, Any]:
        """Serialisable snapshot for the /agents endpoint and traces."""
        return {
            "agent_count": len(self._agents),
            "agents": self.list_agents(),
        }