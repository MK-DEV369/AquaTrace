"""WebSocket module for Ship Analytics API.

This module provides enhanced WebSocket functionality for real-time analytics
and communication between clients and the Ship Analytics system.
"""

from services.websocket.connection_manager import EnhancedConnectionManager
from services.websocket.handler import WebSocketHandler, get_websocket_handler
from services.websocket.schemas import (
    AnalysisMessage,
    AnalyticsUpdate,
    AuthenticationMessage,
    CommandMessage,
    ErrorMessage,
    HeartbeatMessage,
    MLModelUpdate,
    QueryMessage,
    ResponseMessage,
    RouteDiscoveryUpdate,
    SubscriptionMessage,
    SystemStatusMessage,
    UpdateMessage,
    WebSocketMessage,
    WebSocketMessageType,
)

__all__ = [
    "EnhancedConnectionManager",
    "WebSocketMessage",
    "UpdateMessage",
    "QueryMessage",
    "AnalysisMessage",
    "SubscriptionMessage",
    "CommandMessage",
    "ResponseMessage",
    "ErrorMessage",
    "HeartbeatMessage",
    "AnalyticsUpdate",
    "RouteDiscoveryUpdate",
    "MLModelUpdate",
    "SystemStatusMessage",
    "AuthenticationMessage",
    "WebSocketMessageType",
    "WebSocketHandler",
    "get_websocket_handler",
]