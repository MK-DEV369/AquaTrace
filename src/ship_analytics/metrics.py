"""Lightweight in-process metrics registry (Prometheus text format).

Kept dependency-free so it works in every environment. Counters and
histograms are stored in memory and exposed via /metrics.
"""

from __future__ import annotations

import threading

_lock = threading.Lock()
_counters: dict[str, float] = {}
_histograms: dict[str, list[float]] = {}


def counter(name: str, value: float = 1.0, labels: dict[str, str] | None = None) -> None:
    key = _labelled(name, labels)
    with _lock:
        _counters[key] = _counters.get(key, 0.0) + value


def histogram(name: str, value: float, labels: dict[str, str] | None = None) -> None:
    key = _labelled(name, labels)
    with _lock:
        _histograms.setdefault(key, []).append(value)


def _labelled(name: str, labels: dict[str, str] | None) -> str:
    if not labels:
        return name
    parts = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
    return f"{name}{{{parts}}}"


def render() -> str:
    lines: list[str] = []
    with _lock:
        for key, val in sorted(_counters.items()):
            lines.append(f"{key} {val}")
        for key, vals in sorted(_histograms.items()):
            if not vals:
                continue
            count = len(vals)
            total = sum(vals)
            lines.append(f"{key}_count {count}")
            lines.append(f"{key}_sum {total}")
            # A few quantiles for convenience.
            s = sorted(vals)
            for q in (0.5, 0.9, 0.99):
                idx = min(len(s) - 1, int(len(s) * q))
                lines.append(f"{key}_quantile{{q=\"{q}\"}} {s[idx]}")
    return "\n".join(lines) + "\n"


def reset() -> None:
    with _lock:
        _counters.clear()
        _histograms.clear()