"""Orchestrator for the multi-agent system.

The orchestrator is the single entry point for higher-level operations. It:

    1. Accepts a request (a dict or an AgentMessage).
    2. Decomposes it into a list of capability requirements.
    3. Discovers agents for each requirement.
    4. Dispatches messages and aggregates the results.
    5. Records a trace that can be streamed live to the dashboard.

The trace is what makes the multi-agent processing *visible*.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from ship_analytics.agents.base import AgentMessage
from ship_analytics.agents.registry import AgentRegistry

logger = logging.getLogger("ship_analytics.agents.orchestrator")

# Cap on how many trace steps we keep in memory.
_TRACE_HISTORY = 500


@dataclass
class AgentTask:
    """A dependency-aware unit of work assigned to a registered agent."""

    task_id: str
    requirement: str
    payload: dict[str, Any]
    depends_on: list[str] = field(default_factory=list)
    status: str = "queued"
    result: dict[str, Any] | None = None
    error: str | None = None


class OrchestratorTrace:
    """A single trace event emitted by the orchestrator.

    Subscribers (WebSocket, dashboard, tests) consume these to render the live
    processing flow.
    """

    def __init__(
        self,
        step: str,
        agent: str | None = None,
        capability: str | None = None,
        detail: str = "",
        correlation_id: str | None = None,
        status: str = "ok",
        duration_ms: float | None = None,
    ) -> None:
        self.step = step
        self.agent = agent
        self.capability = capability
        self.detail = detail
        self.correlation_id = correlation_id
        self.status = status
        self.duration_ms = duration_ms
        self.timestamp = time.time()

    def to_dict(self) -> dict[str, Any]:
        return {
            "step": self.step,
            "agent": self.agent,
            "capability": self.capability,
            "detail": self.detail,
            "correlation_id": self.correlation_id,
            "status": self.status,
            "duration_ms": self.duration_ms,
            "timestamp": self.timestamp,
        }


class Orchestrator:
    """Coordinates the specialist agents and exposes a live trace."""

    def __init__(self, registry: AgentRegistry) -> None:
        self.registry = registry
        self._trace: deque[OrchestratorTrace] = deque(maxlen=_TRACE_HISTORY)
        self._subscribers: list[Callable[[OrchestratorTrace], None]] = []
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Trace plumbing
    # ------------------------------------------------------------------
    def subscribe(self, callback: Callable[[OrchestratorTrace], None]) -> None:
        """Register a callback invoked for every trace step."""
        self._subscribers.append(callback)

    def unsubscribe(self, callback: Callable[[OrchestratorTrace], None]) -> None:
        if callback in self._subscribers:
            self._subscribers.remove(callback)

    def _emit(self, trace: OrchestratorTrace) -> None:
        self._trace.append(trace)
        for cb in list(self._subscribers):
            try:
                cb(trace)
            except Exception:  # pragma: no cover - defensive
                pass

    def trace_history(self, limit: int = 100) -> list[dict[str, Any]]:
        """Return the most recent trace steps."""
        items = list(self._trace)[-limit:]
        return [t.to_dict() for t in items]

    def clear_trace(self) -> None:
        self._trace.clear()

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------
    async def run(
        self,
        request: dict[str, Any] | AgentMessage,
        requirements: list[str] | None = None,
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        """Run a request through the agent graph.

        ``requirements`` is an optional list of free-form capability hints; if
        omitted the orchestrator uses a single "investigate" requirement.
        """
        if isinstance(request, AgentMessage):
            correlation_id = correlation_id or request.correlation_id
            payload = request.payload
        else:
            payload = request
            correlation_id = correlation_id or uuid.uuid4().hex

        requirements = requirements or ["investigate"]
        started = time.perf_counter()

        self._emit(
            OrchestratorTrace(
                step="request_received",
                detail=f"requirements={requirements}",
                correlation_id=correlation_id,
            )
        )

        results: list[dict[str, Any]] = []
        for req in requirements:
            t0 = time.perf_counter()
            self._emit(
                OrchestratorTrace(
                    step="discover",
                    capability=req,
                    detail=f"looking for agents matching {req!r}",
                    correlation_id=correlation_id,
                )
            )
            agents = self.registry.discover_by_requirement(req)
            if not agents:
                self._emit(
                    OrchestratorTrace(
                        step="discover",
                        capability=req,
                        detail="no agent registered for this requirement",
                        correlation_id=correlation_id,
                        status="warning",
                    )
                )
                continue
            for agent in agents:
                self._emit(
                    OrchestratorTrace(
                        step="dispatch",
                        agent=agent.name,
                        capability=req,
                        detail=f"dispatching to {agent.name}",
                        correlation_id=correlation_id,
                    )
                )
                msg = AgentMessage(
                    type="handle",
                    payload=payload,
                    correlation_id=correlation_id,
                    source="orchestrator",
                )
                try:
                    reply = await agent.handle(msg)
                    duration = (time.perf_counter() - t0) * 1000.0
                    self._emit(
                        OrchestratorTrace(
                            step="reply",
                            agent=agent.name,
                            capability=req,
                            detail=f"{reply.type}",
                            correlation_id=correlation_id,
                            duration_ms=duration,
                        )
                    )
                    results.append(
                        {
                            "agent": agent.name,
                            "capability": req,
                            "reply": reply.to_dict(),
                        }
                    )
                except Exception as exc:
                    duration = (time.perf_counter() - t0) * 1000.0
                    self._emit(
                        OrchestratorTrace(
                            step="error",
                            agent=agent.name,
                            capability=req,
                            detail=str(exc),
                            correlation_id=correlation_id,
                            status="error",
                            duration_ms=duration,
                        )
                    )
                    results.append(
                        {
                            "agent": agent.name,
                            "capability": req,
                            "error": str(exc),
                        }
                    )

        total = (time.perf_counter() - started) * 1000.0
        self._emit(
            OrchestratorTrace(
                step="completed",
                detail=f"{len(results)} result(s)",
                correlation_id=correlation_id,
                duration_ms=total,
            )
        )

        return {
            "correlation_id": correlation_id,
            "requirements": requirements,
            "results": results,
            "duration_ms": total,
        }

    async def run_task_graph(
        self,
        tasks: list[AgentTask],
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        """Execute a dependency-aware graph and parallelize ready tasks."""
        correlation_id = correlation_id or uuid.uuid4().hex
        task_map = {task.task_id: task for task in tasks}
        if len(task_map) != len(tasks):
            raise ValueError("task IDs must be unique")
        for task in tasks:
            missing = set(task.depends_on) - set(task_map)
            if missing:
                raise ValueError(f"task {task.task_id} depends on unknown task(s): {sorted(missing)}")

        completed: set[str] = set()
        while len(completed) < len(tasks):
            ready = [
                task for task in tasks
                if task.status == "queued" and set(task.depends_on) <= completed
            ]
            if not ready:
                blocked = [task.task_id for task in tasks if task.status == "queued"]
                raise ValueError(f"task graph has a cycle or blocked dependency: {blocked}")

            async def execute(task: AgentTask) -> None:
                task.status = "running"
                self._emit(
                    OrchestratorTrace(
                        step="task_started",
                        capability=task.requirement,
                        detail=task.task_id,
                        correlation_id=correlation_id,
                    )
                )
                dependency_results = {
                    dependency: task_map[dependency].result
                    for dependency in task.depends_on
                }
                payload = {**task.payload, "dependency_results": dependency_results}
                outcome = await self.run(payload, [task.requirement], correlation_id)
                task.result = outcome
                task.status = "completed" if outcome["results"] else "failed"
                if task.status == "failed":
                    task.error = "no registered agent matched the requirement"
                self._emit(
                    OrchestratorTrace(
                        step="task_completed",
                        capability=task.requirement,
                        detail=task.task_id,
                        correlation_id=correlation_id,
                        status="ok" if task.status == "completed" else "error",
                    )
                )

            await asyncio.gather(*(execute(task) for task in ready))
            completed.update(task.task_id for task in ready if task.status == "completed")
            for task in ready:
                if task.status != "completed":
                    raise RuntimeError(f"agent task failed: {task.task_id}: {task.error}")

        return {
            "correlation_id": correlation_id,
            "tasks": [
                {
                    "task_id": task.task_id,
                    "requirement": task.requirement,
                    "depends_on": task.depends_on,
                    "status": task.status,
                    "result": task.result,
                    "error": task.error,
                }
                for task in tasks
            ],
        }

    async def health(self) -> dict[str, Any]:
        return {
            "registry": self.registry.snapshot(),
            "agent_health": await self.registry.health_all(),
        }