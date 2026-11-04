"""Structured logging configuration for AquaTrace AI.

Replaces scattered print() calls with a consistent, correlation-id-aware logger.
"""

from __future__ import annotations

import contextvars
import logging
import sys

_correlation_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "correlation_id", default=None
)


def set_correlation_id(value: str | None) -> None:
    _correlation_id.set(value)


def get_correlation_id() -> str | None:
    return _correlation_id.get()


class CorrelationFilter(logging.Filter):
    """Inject the current correlation id into every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = _correlation_id.get() or "-"
        return True


def configure_logging(level: str = "INFO") -> None:
    """Configure the root logger with a structured format."""
    root = logging.getLogger()
    root.setLevel(level.upper())
    # Avoid duplicate handlers on re-init.
    for handler in list(root.handlers):
        root.removeHandler(handler)
    handler = logging.StreamHandler(sys.stdout)
    handler.addFilter(CorrelationFilter())
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] cid=%(correlation_id)s %(message)s"
    )
    handler.setFormatter(formatter)
    root.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)