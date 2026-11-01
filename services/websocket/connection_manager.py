"""Enhanced WebSocket connection manager with authentication and rate limiting."""

import asyncio
import time
import uuid
from datetime import datetime, timedelta

from fastapi import WebSocket, WebSocketDisconnect

try:
    import jwt as pyjwt
    JWT_AVAILABLE = True
except ImportError:
    pyjwt = None
    JWT_AVAILABLE = False


class EnhancedConnectionManager:
    """Production-ready WebSocket connection manager with authentication and rate limiting."""
    
    def __init__(self, jwt_secret: str, rate_limit_per_minute: int = 60):
        self.active_connections: dict[str, WebSocket] = {}
        self.user_connections: dict[str, set[str]] = {}
        self.connection_metadata: dict[str, dict] = {}
        self.rate_limiters: dict[str, RateLimiter] = {}
        self.heartbeat_tasks: dict[str, asyncio.Task] = {}
        self.subscription_manager = SubscriptionManager()
        self.jwt_secret = jwt_secret
        self.rate_limit_per_minute = rate_limit_per_minute
        self.max_connections = 1000
        self.connection_ttl = timedelta(minutes=30)
    
    async def connect(self, websocket: WebSocket, user_id: str | None = None) -> str:
        """Connect a WebSocket with optional authentication.
        
        Args:
            websocket: FastAPI WebSocket connection
            user_id: Optional user ID for authentication
            
        Returns:
            Connection ID for this WebSocket
        """
        await websocket.accept()
        
        # Generate unique connection ID
        connection_id = str(uuid.uuid4())
        
        # Check connection limits
        if len(self.active_connections) >= self.max_connections:
            await websocket.close(code=1013, reason="Too many connections")
            raise ConnectionError("Connection limit exceeded")
        
        # Authenticate user
        authenticated_user = await self._authenticate_connection(websocket, user_id)
        
        # Add to active connections
        self.active_connections[connection_id] = websocket
        self.connection_metadata[connection_id] = {
            "user_id": authenticated_user["user_id"],
            "username": authenticated_user.get("username"),
            "joined_at": datetime.utcnow(),
            "last_activity": datetime.utcnow(),
            "ip_address": websocket.client.host if websocket.client else None,
            "permissions": authenticated_user.get("permissions", []),
        }
        
        # Track user connections
        if authenticated_user["user_id"] not in self.user_connections:
            self.user_connections[authenticated_user["user_id"]] = set()
        self.user_connections[authenticated_user["user_id"]].add(connection_id)
        
        # Initialize rate limiter
        self.rate_limiters[connection_id] = RateLimiter(self.rate_limit_per_minute)
        
        # Start heartbeat monitoring
        self.heartbeat_tasks[connection_id] = asyncio.create_task(
            self._monitor_heartbeat(connection_id)
        )
        
        return connection_id
    
    def disconnect(self, connection_id: str):
        """Disconnect and clean up a WebSocket connection."""
        # Cancel heartbeat task
        if connection_id in self.heartbeat_tasks:
            self.heartbeat_tasks[connection_id].cancel()
            del self.heartbeat_tasks[connection_id]
        
        # Remove from active connections
        if connection_id in self.active_connections:
            del self.active_connections[connection_id]
        
        # Remove from user connections
        user_id = self.connection_metadata.get(connection_id, {}).get("user_id")
        if user_id and user_id in self.user_connections:
            self.user_connections[user_id].discard(connection_id)
            if not self.user_connections[user_id]:
                del self.user_connections[user_id]
        
        # Clean up rate limiter
        if connection_id in self.rate_limiters:
            del self.rate_limiters[connection_id]
        
        # Remove metadata
        if connection_id in self.connection_metadata:
            del self.connection_metadata[connection_id]
    
    async def send_update(self, connection_id: str, message: dict) -> bool:
        """Send an update to a specific WebSocket connection with rate limiting."""
        # Check rate limiting
        if not self._check_rate_limit(connection_id):
            return False
        
        if connection_id not in self.active_connections:
            return False
        
        try:
            # Update last activity
            self.connection_metadata[connection_id]["last_activity"] = datetime.utcnow()
            
            # Send message
            await self.active_connections[connection_id].send_json(message)
            return True
        except (WebSocketDisconnect, RuntimeError):
            # Connection is broken, clean up
            self.disconnect(connection_id)
            return False
    
    async def broadcast_update(self, message: dict, user_id: str | None = None):
        """Broadcast an update to all connected clients, optionally filtered by user."""
        connections = self.active_connections.copy()
        
        if user_id:
            # Filter by user
            if user_id not in self.user_connections:
                return
            connections = {
                cid: conn for cid, conn in connections.items()
                if cid in self.user_connections.get(user_id, set())
            }
        
        # Send to all connections
        for connection_id, _websocket in connections.items():
            await self.send_update(connection_id, message)
    
    async def subscribe(self, connection_id: str, channel: str, filter_criteria: dict | None = None):
        """Subscribe a connection to a specific channel."""
        if connection_id not in self.active_connections:
            return False
        
        self.subscription_manager.subscribe(connection_id, channel, filter_criteria)
        return True
    
    async def unsubscribe(self, connection_id: str, channel: str):
        """Unsubscribe a connection from a specific channel."""
        self.subscription_manager.unsubscribe(connection_id, channel)
    
    def get_connection_stats(self) -> dict:
        """Get statistics about all connections."""
        return {
            "total_connections": len(self.active_connections),
            "unique_users": len(self.user_connections),
            "connections_per_user": {
                user: len(conns) for user, conns in self.user_connections.items()
            },
            "recent_connections": [
                metadata for metadata in self.connection_metadata.values()
                if datetime.utcnow() - metadata["joined_at"] < timedelta(hours=1)
            ],
        }
    
    async def cleanup_expired_connections(self):
        """Clean up expired connections and heartbeat tasks."""
        current_time = datetime.utcnow()
        expired_connections = []
        
        for connection_id, metadata in self.connection_metadata.items():
            if current_time - metadata["last_activity"] > self.connection_ttl:
                expired_connections.append(connection_id)
        
        for connection_id in expired_connections:
            self.disconnect(connection_id)
    
    async def _authenticate_connection(self, websocket: WebSocket, user_id: str | None) -> dict:
        """Authenticate a WebSocket connection."""
        # Extract token from query parameters or headers
        token = self._extract_token(websocket)
        
        if token and JWT_AVAILABLE:
            try:
                payload = pyjwt.decode(token, self.jwt_secret, algorithms=["HS256"])
                return {
                    "user_id": payload.get("sub", f"anonymous_{int(time.time())}"),
                    "username": payload.get("username"),
                    "permissions": payload.get("permissions", []),
                    "authenticated": True,
                }
            except (pyjwt.InvalidTokenError, AttributeError):
                pass
        
        await websocket.close(code=1008, reason="Authentication required")
        raise PermissionError("valid WebSocket authentication token required")
    
    def _extract_token(self, websocket: WebSocket) -> str | None:
        """Extract authentication token from WebSocket."""
        # Check query parameters
        query_string = websocket.url.query
        if query_string:
            params = dict(param.split("=", 1) for param in query_string.split("&") if "=" in param)
            token = params.get("token") or params.get("access_token")
            if token:
                return token
        
        # Check headers (if available)
        # This would need to be implemented based on your WebSocket setup
        
        return None
    
    def _check_rate_limit(self, connection_id: str) -> bool:
        """Check if a connection is within rate limits."""
        if connection_id not in self.rate_limiters:
            return True
        
        return self.rate_limiters[connection_id].is_allowed()
    
    async def _monitor_heartbeat(self, connection_id: str):
        """Monitor WebSocket connection health."""
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                if connection_id not in self.active_connections:
                    break
                
                # Send heartbeat to client
                metadata = self.connection_metadata.get(connection_id, {})
                last_activity = metadata.get("last_activity")
                
                if last_activity and datetime.utcnow() - last_activity > timedelta(minutes=5):
                    # Connection may be dead, attempt to send ping
                    try:
                        await self.active_connections[connection_id].send_json({
                            "type": "heartbeat",
                            "timestamp": datetime.utcnow().isoformat(),
                        })
                    except (WebSocketDisconnect, RuntimeError):
                        self.disconnect(connection_id)
                        break
                
            except asyncio.CancelledError:
                break
            except Exception:
                # Log error but continue monitoring
                break


class RateLimiter:
    """Simple rate limiter based on sliding window."""
    
    def __init__(self, max_requests: int):
        self.max_requests = max_requests
        self.requests: list[float] = []
        self.lock = asyncio.Lock()
    
    async def is_allowed(self) -> bool:
        """Check if a request is allowed within rate limit."""
        async with self.lock:
            current_time = time.time()
            
            # Remove old requests (older than 1 minute)
            self.requests = [req_time for req_time in self.requests 
                           if current_time - req_time < 60]
            
            if len(self.requests) >= self.max_requests:
                return False
            
            # Add current request
            self.requests.append(current_time)
            return True


class SubscriptionManager:
    """Manage WebSocket subscriptions to different channels."""
    
    def __init__(self):
        self.subscriptions: dict[str, dict[str, set[dict]]] = {}
        self.lock = asyncio.Lock()
    
    async def subscribe(self, connection_id: str, channel: str, filter_criteria: dict | None = None):
        """Subscribe a connection to a channel with optional filters."""
        async with self.lock:
            if channel not in self.subscriptions:
                self.subscriptions[channel] = {}
            
            if connection_id not in self.subscriptions[channel]:
                self.subscriptions[channel][connection_id] = set()
            
            if filter_criteria:
                self.subscriptions[channel][connection_id].add(frozenset(filter_criteria.items()))
    
    async def unsubscribe(self, connection_id: str, channel: str):
        """Unsubscribe a connection from a channel."""
        async with self.lock:
            if channel in self.subscriptions and connection_id in self.subscriptions[channel]:
                del self.subscriptions[channel][connection_id]
    
    async def get_subscribers(self, channel: str, filter_criteria: dict | None = None) -> set[str]:
        """Get all subscribers for a channel that match the filter criteria."""
        async with self.lock:
            if channel not in self.subscriptions:
                return set()
            
            subscribers = set()
            for connection_id, criteria in self.subscriptions[channel].items():
                if not filter_criteria or any(frozenset(filter_criteria.items()) == c for c in criteria):
                    subscribers.add(connection_id)
            
            return subscribers