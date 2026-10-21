"""WebSocket message validation schemas."""

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field, validator


class WebSocketMessage(BaseModel):
    """Base WebSocket message schema with validation."""
    
    type: str = Field(..., pattern=r'^[a-zA-Z][a-zA-Z0-9_]*$')
    timestamp: datetime
    data: Dict[str, Any]
    user_id: Optional[str] = None
    session_id: str = Field(default_factory=lambda: str(UUID().hex[:32]))
    message_id: str = Field(default_factory=lambda: str(UUID().hex[:32]))
    
    @validator('type')
    def validate_message_type(cls, v):
        """Validate message type against allowed types."""
        allowed_types = {
            'update', 'query', 'analysis', 'subscription', 
            'heartbeat', 'command', 'response', 'error'
        }
        if v not in allowed_types:
            raise ValueError(f'Invalid message type: {v}. Allowed types: {allowed_types}')
        return v
    
    @validator('timestamp')
    def validate_timestamp(cls, v):
        """Ensure timestamp is not in the future."""
        if v > datetime.utcnow():
            raise ValueError('Timestamp cannot be in the future')
        return v
    
    @validator('data')
    def validate_data(cls, v, values):
        """Validate message data based on type."""
        message_type = values.get('type')
        if message_type == 'subscription':
            if 'channels' not in v:
                raise ValueError('Subscription messages must include "channels" field')
            if not isinstance(v['channels'], list):
                raise ValueError('Channels must be a list')
        
        elif message_type == 'query':
            if 'query' not in v:
                raise ValueError('Query messages must include "query" field')
        
        return v


class HeartbeatMessage(WebSocketMessage):
    """Heartbeat message for connection health monitoring."""
    
    type: Literal['heartbeat'] = 'heartbeat'
    data: Dict[str, Any] = Field(default_factory=lambda: {'status': 'alive'})


class UpdateMessage(WebSocketMessage):
    """Message for broadcasting updates to clients."""
    
    type: Literal['update'] = 'update'


class QueryMessage(WebSocketMessage):
    """Message for client queries to the server."""
    
    type: Literal['query'] = 'query'


class AnalysisMessage(WebSocketMessage):
    """Message for ML analysis results."""
    
    type: Literal['analysis'] = 'analysis'


class SubscriptionMessage(WebSocketMessage):
    """Message for channel subscriptions."""
    
    type: Literal['subscription'] = 'subscription'


class CommandMessage(WebSocketMessage):
    """Message for commands to the server."""
    
    type: Literal['command'] = 'command'


class ResponseMessage(WebSocketMessage):
    """Message for server responses."""
    
    type: Literal['response'] = 'response'


class ErrorMessage(WebSocketMessage):
    """Message for error reporting."""
    
    type: Literal['error'] = 'error'
    data: Dict[str, Any] = Field(default_factory=lambda: {'error_type': 'unknown'})


class AnalyticsUpdate(BaseModel):
    """Schema for analytics updates sent via WebSocket."""
    
    event_id: str
    mmsi: int
    event_time: datetime
    anomaly_score: float
    severity: Literal['low', 'medium', 'high']
    route_cluster: Optional[str] = None
    eta_prediction: Optional[datetime] = None
    confidence: float
    metadata: Dict[str, Any] = Field(default_factory=dict)


class RouteDiscoveryUpdate(BaseModel):
    """Schema for route discovery updates."""
    
    route_id: str
    mmsi: int
    cluster_id: str
    confidence: float
    total_distance: float
    average_speed: float
    num_points: int
    start_time: datetime
    end_time: datetime
    geometry: Dict[str, Any] = Field(default_factory=dict)


class MLModelUpdate(BaseModel):
    """Schema for ML model updates."""
    
    model_type: Literal['isolation_forest', 'dbscan', 'eta_baseline']
    model_id: str
    version: str
    accuracy: float
    last_trained: datetime
    parameters: Dict[str, Any]
    training_data_size: int


class SystemStatusMessage(BaseModel):
    """System status message for monitoring."""
    
    status: Literal['healthy', 'degraded', 'unhealthy']
    uptime: float
    active_connections: int
    messages_processed: int
    errors: int
    latency_ms: float
    memory_usage: Dict[str, float]
    cpu_usage: float
    timestamp: datetime


class AuthenticationMessage(BaseModel):
    """Message for WebSocket authentication."""
    
    type: Literal['authentication'] = 'authentication'
    token: str
    expires_at: datetime
    user_id: str
    permissions: List[str] = Field(default_factory=list)


# Type alias for all WebSocket message types
WebSocketMessageType = WebSocketMessage | HeartbeatMessage | UpdateMessage | QueryMessage | AnalysisMessage | SubscriptionMessage | CommandMessage | ResponseMessage | ErrorMessage