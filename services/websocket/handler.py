"""WebSocket handler with enhanced authentication and message processing."""

import asyncio
from datetime import datetime
from typing import Optional

from fastapi import Depends, HTTPException, WebSocket, WebSocketDisconnect

from services.websocket.connection_manager import EnhancedConnectionManager
from services.websocket.schemas import (
    AnalyticsUpdate,
    AuthenticationMessage,
    MLModelUpdate,
    QueryMessage,
    RouteDiscoveryUpdate,
    SystemStatusMessage,
    UpdateMessage,
    WebSocketMessage,
)
from ship_analytics.config import get_settings


class WebSocketHandler:
    """Enhanced WebSocket handler with authentication and routing."""
    
    def __init__(self, connection_manager: EnhancedConnectionManager):
        self.connection_manager = connection_manager
        self.message_handlers = {
            'update': self._handle_update_message,
            'query': self._handle_query_message,
            'analysis': self._handle_analysis_message,
            'subscription': self._handle_subscription_message,
            'heartbeat': self._handle_heartbeat_message,
        }
    
    async def handle_websocket(self, websocket: WebSocket, token: Optional[str] = None):
        """Handle WebSocket connection with authentication."""
        connection_id = None
        try:
            # Connect with authentication
            connection_id = await self.connection_manager.connect(websocket, token)
            user_id = self.connection_manager.connection_metadata[connection_id]["user_id"]
            
            # Register for system status updates
            await self.connection_manager.subscribe(connection_id, "system_status")
            
            # Welcome message
            await self.connection_manager.send_update(connection_id, {
                "type": "connection_established",
                "connection_id": connection_id,
                "user_id": user_id,
                "server_time": datetime.utcnow().isoformat(),
                "supported_channels": ["analytics", "routes", "ml_models", "system_status"],
            })
            
            # Send initial system status
            status_message = await self._get_system_status()
            await self.connection_manager.send_update(connection_id, status_message)
            
            # Handle incoming messages
            await self._handle_messages(connection_id, user_id)
            
        except WebSocketDisconnect:
            pass
        except Exception as e:
            # Send error message to client
            await websocket.send_json({
                "type": "error",
                "message": str(e),
                "timestamp": datetime.utcnow().isoformat(),
            })
        finally:
            if connection_id is not None:
                self.connection_manager.disconnect(connection_id)
    
    async def _handle_messages(self, connection_id: str, user_id: str):
        """Handle incoming WebSocket messages."""
        while True:
            try:
                # Receive message with timeout
                data = await self.connection_manager.active_connections[connection_id].receive_json()
                
                # Validate message type
                if not isinstance(data, dict):
                    raise ValueError("Message must be a JSON object")
                
                message_type = data.get('type')
                if not message_type:
                    raise ValueError("Message must include 'type' field")
                
                # Get handler
                handler = self.message_handlers.get(message_type)
                if not handler:
                    raise ValueError(f"Unknown message type: {message_type}")
                
                # Process message
                await handler(connection_id, user_id, data)
                
            except WebSocketDisconnect:
                break
            except Exception as e:
                # Send error response
                await self.connection_manager.send_update(connection_id, {
                    "type": "error",
                    "message": str(e),
                    "timestamp": datetime.utcnow().isoformat(),
                    "message_id": data.get('message_id', 'unknown'),
                })
    
    async def _handle_update_message(self, connection_id: str, user_id: str, data: dict):
        """Handle update messages (admin only)."""
        if "admin" not in self.connection_manager.connection_metadata[connection_id]["permissions"]:
            raise HTTPException(status_code=403, detail="Admin permissions required")
        
        # Process update based on content
        update_type = data.get('update_type')
        
        if update_type == 'analytics':
            # Send analytics update
            await self._broadcast_analytics_update(data.get('data'))
        
        elif update_type == 'route_discovery':
            # Send route discovery update
            await self._broadcast_route_discovery_update(data.get('data'))
        
        elif update_type == 'ml_model':
            # Send ML model update
            await self._broadcast_ml_model_update(data.get('data'))
        
        elif update_type == 'system_status':
            # Send system status update
            status_message = await self._get_system_status()
            await self.connection_manager.broadcast_update(status_message)
    
    async def _handle_query_message(self, connection_id: str, user_id: str, data: dict):
        """Handle query messages."""
        query_type = data.get('query_type')
        
        if query_type == 'analytics':
            # Get analytics data
            result = await self._get_analytics_data(user_id)
            await self.connection_manager.send_update(connection_id, {
                "type": "query_response",
                "data": result,
                "timestamp": datetime.utcnow().isoformat(),
            })
        
        elif query_type == 'routes':
            # Get route discovery data
            result = await self._get_route_data(user_id)
            await self.connection_manager.send_update(connection_id, {
                "type": "query_response",
                "data": result,
                "timestamp": datetime.utcnow().isoformat(),
            })
        
        elif query_type == 'ml_models':
            # Get ML model status
            result = await self._get_ml_model_status()
            await self.connection_manager.send_update(connection_id, {
                "type": "query_response",
                "data": result,
                "timestamp": datetime.utcnow().isoformat(),
            })
    
    async def _handle_analysis_message(self, connection_id: str, user_id: str, data: dict):
        """Handle analysis requests."""
        analysis_type = data.get('analysis_type')
        
        if analysis_type == 'anomaly_detection':
            # Trigger anomaly detection analysis
            result = await self._trigger_anomaly_detection()
            await self._broadcast_analytics_update(result)
        
        elif analysis_type == 'route_clustering':
            # Trigger route clustering analysis
            result = await self._trigger_route_clustering()
            await self._broadcast_route_discovery_update(result)
        
        elif analysis_type == 'model_training':
            # Trigger model training
            result = await self._trigger_model_training()
            await self._broadcast_ml_model_update(result)
    
    async def _handle_subscription_message(self, connection_id: str, user_id: str, data: dict):
        """Handle subscription requests."""
        channels = data.get('channels', [])
        
        for channel in channels:
            await self.connection_manager.subscribe(connection_id, channel)
        
        await self.connection_manager.send_update(connection_id, {
            "type": "subscription_confirmed",
            "channels": channels,
            "timestamp": datetime.utcnow().isoformat(),
        })
    
    async def _handle_heartbeat_message(self, connection_id: str, user_id: str, data: dict):
        """Handle heartbeat messages."""
        # Just acknowledge heartbeat, no action needed
        pass
    
    async def _broadcast_analytics_update(self, analytics_data: dict):
        """Broadcast analytics update to all subscribers."""
        message = {
            "type": "analytics_update",
            "data": analytics_data,
            "timestamp": datetime.utcnow().isoformat(),
        }
        await self.connection_manager.broadcast_update(message, user_id=None)
    
    async def _broadcast_route_discovery_update(self, route_data: dict):
        """Broadcast route discovery update to all subscribers."""
        message = {
            "type": "route_discovery_update",
            "data": route_data,
            "timestamp": datetime.utcnow().isoformat(),
        }
        await self.connection_manager.broadcast_update(message, user_id=None)
    
    async def _broadcast_ml_model_update(self, model_data: dict):
        """Broadcast ML model update to all subscribers."""
        message = {
            "type": "ml_model_update",
            "data": model_data,
            "timestamp": datetime.utcnow().isoformat(),
        }
        await self.connection_manager.broadcast_update(message, user_id=None)
    
    async def _get_system_status(self) -> dict:
        """Get current system status."""
        connection_stats = self.connection_manager.get_connection_stats()
        
        return {
            "type": "system_status",
            "data": {
                "status": "healthy",
                "uptime": 86400.0,  # Example uptime
                "active_connections": connection_stats["total_connections"],
                "messages_processed": 15000,
                "errors": 5,
                "latency_ms": 12.5,
                "memory_usage": {
                    "total": 8589934592,
                    "used": 4294967296,
                    "available": 4294967296,
                },
                "cpu_usage": 45.2,
                "timestamp": datetime.utcnow().isoformat(),
            },
        }
    
    async def _get_analytics_data(self, user_id: str) -> dict:
        """Get analytics data for a specific user."""
        # This would query the actual analytics data
        return {
            "analytics_data": [],
            "count": 0,
            "user_id": user_id,
        }
    
    async def _get_route_data(self, user_id: str) -> dict:
        """Get route data for a specific user."""
        # This would query the actual route data
        return {
            "route_data": [],
            "count": 0,
            "user_id": user_id,
        }
    
    async def _get_ml_model_status(self) -> dict:
        """Get ML model status."""
        # This would query the actual ML model status
        return {
            "models": [],
            "count": 0,
        }
    
    async def _trigger_anomaly_detection(self) -> dict:
        """Trigger anomaly detection analysis."""
        # This would trigger actual anomaly detection
        return {
            "anomalies_detected": 0,
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    async def _trigger_route_clustering(self) -> dict:
        """Trigger route clustering analysis."""
        # This would trigger actual route clustering
        return {
            "routes_discovered": 0,
            "clusters": 0,
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    async def _trigger_model_training(self) -> dict:
        """Trigger model training."""
        # This would trigger actual model training
        return {
            "training_started": False,
            "timestamp": datetime.utcnow().isoformat(),
        }


# WebSocket dependencies and endpoint registration
async def get_websocket_handler() -> WebSocketHandler:
    """Dependency injection for WebSocketHandler."""
    settings = get_settings()
    connection_manager = EnhancedConnectionManager(
        jwt_secret=settings.jwt_secret,
        rate_limit_per_minute=60,
    )
    return WebSocketHandler(connection_manager)


def get_websocket_token(websocket: WebSocket) -> Optional[str]:
    """Extract JWT token from WebSocket query parameters."""
    query_string = websocket.url.query
    if query_string:
        params = dict(param.split("=", 1) for param in query_string.split("&") if "=" in param)
        return params.get("token")
    return None