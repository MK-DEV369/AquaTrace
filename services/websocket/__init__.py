"""WebSocket module for Ship Analytics API.

This module provides enhanced WebSocket functionality for real-time analytics
and communication between clients and the Ship Analytics system.
"""

from services.websocket.connection_manager import EnhancedConnectionManager
from services.websocket.schemas import (
    WebSocketMessage, UpdateMessage, QueryMessage, AnalysisMessage,
    SubscriptionMessage, CommandMessage, ResponseMessage, ErrorMessage,
    HeartbeatMessage, AnalyticsUpdate, RouteDiscoveryUpdate,
    MLModelUpdate, SystemStatusMessage, AuthenticationMessage,
    WebSocketMessageType,
)
from services.websocket.handler import WebSocketHandler, get_websocket_handler

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