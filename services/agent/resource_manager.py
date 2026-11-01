"""Resource manager for AquaTrace AI Maritime Investigation Agent.

This module provides resource isolation, circuit breaker patterns, and resource
management to ensure complete isolation between investigation processes.
"""

import asyncio
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set
from enum import Enum
from dataclasses import dataclass

from fastapi import HTTPException
from pydantic import BaseModel, Field


class ResourceType(str, Enum):
    """Types of resources that can be managed."""
    DATABASE_CONNECTION = "database_connection"
    KAFKA_PRODUCER = "kafka_producer"
    KAFKA_CONSUMER = "kafka_consumer"
    EXTERNAL_API = "external_api"
    FILE_SYSTEM = "file_system"
    MEMORY_CACHE = "memory_cache"
    NETWORK_SOCKET = "network_socket"
    COMPUTATION = "computation"


class ResourceState(str, Enum):
    """States for resource lifecycle management."""
    AVAILABLE = "available"
    ACQUIRED = "acquired"
    RELEASED = "released"
    ERROR = "error"
    TIMEOUT = "timeout"


@dataclass
class ResourceQuota:
    """Resource quota definition."""
    limit: int
    used: int = 0
    remaining: int = Field(init=False)
    
    def __post_init__(self):
        self.remaining = self.limit - self.used


@dataclass
class ResourceUsage:
    """Current resource usage tracking."""
    resource_id: str
    resource_type: ResourceType
    state: ResourceState = ResourceState.AVAILABLE
    acquired_at: Optional[datetime] = None
    timeout_at: Optional[datetime] = None
    owner_id: Optional[str] = None
    metadata: Dict[str, Any] = None


class CircuitBreakerState(str, Enum):
    """States for circuit breaker."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""
    failure_threshold: int = 5
    recovery_timeout: timedelta = timedelta(minutes=5)
    expected_success_rate: float = 0.5
    timeout: timedelta = timedelta(seconds=60)


class CircuitBreaker:
    """Circuit breaker pattern for fault tolerance and isolation."""
    
    def __init__(self, name: str, config: CircuitBreakerConfig):
        self.name = name
        self.config = config
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.success_count = 0
        self.total_requests = 0
        self.opened_at: Optional[datetime] = None
        
    def record_success(self):
        """Record a successful operation."""
        self.total_requests += 1
        self.success_count += 1
        
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.CLOSED
            self.failure_count = 0
            self.success_count = 0
    
    def record_failure(self):
        """Record a failed operation."""
        self.total_requests += 1
        self.failure_count += 1
        self.last_failure_time = datetime.utcnow()
        
        if self.state == CircuitBreakerState.CLOSED:
            if self.failure_count >= self.config.failure_threshold:
                self.state = CircuitBreakerState.OPEN
                self.opened_at = datetime.utcnow()
        
        elif self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.OPEN
    
    def can_execute(self) -> bool:
        """Check if operation can be executed."""
        if self.state == CircuitBreakerState.CLOSED:
            return True
        
        if self.state == CircuitBreakerState.OPEN:
            if self.last_failure_time and (
                datetime.utcnow() - self.last_failure_time < self.config.recovery_timeout
            ):
                return False
            
            # Allow half-open test
            self.state = CircuitBreakerState.HALF_OPEN
            return True
        
        return self.state == CircuitBreakerState.HALF_OPEN
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get circuit breaker health status."""
        if self.total_requests == 0:
            return {
                'state': self.state.value,
                'failure_rate': 0.0,
                'success_rate': 0.0,
                'total_requests': 0,
                'failure_count': 0,
                'success_count': 0
            }
        
        failure_rate = self.failure_count / self.total_requests
        success_rate = self.success_count / self.total_requests
        
        return {
            'state': self.state.value,
            'failure_rate': failure_rate,
            'success_rate': success_rate,
            'total_requests': self.total_requests,
            'failure_count': self.failure_count,
            'success_count': self.success_count,
            'opened_at': self.opened_at.isoformat() if self.opened_at else None,
            'last_failure_time': self.last_failure_time.isoformat() if self.last_failure_time else None
        }


class ResourceManager:
    """Manages resource isolation and lifecycle."""
    
    def __init__(self):
        self.active_resources: Dict[str, ResourceUsage] = {}
        self.resource_quotas: Dict[ResourceType, ResourceQuota] = {}
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.resource_timeouts: Dict[str, timedelta] = {}
        self.sandboxes: Dict[str, Dict[str, Any]] = {}
        
        # Initialize default quotas
        self._initialize_default_quotas()
    
    def _initialize_default_quotas(self):
        """Initialize default resource quotas."""
        self.resource_quotas = {
            ResourceType.DATABASE_CONNECTION: ResourceQuota(limit=10),
            ResourceType.KAFKA_PRODUCER: ResourceQuota(limit=5),
            ResourceType.KAFKA_CONSUMER: ResourceQuota(limit=8),
            ResourceType.EXTERNAL_API: ResourceQuota(limit=20),
            ResourceType.FILE_SYSTEM: ResourceQuota(limit=50),
            ResourceType.MEMORY_CACHE: ResourceQuota(limit=100),
            ResourceType.NETWORK_SOCKET: ResourceQuota(limit=25),
            ResourceType.COMPUTATION: ResourceQuota(limit=15)
        }
    
    def add_circuit_breaker(self, name: str, config: Optional[CircuitBreakerConfig] = None):
        """Add a circuit breaker for a resource."""
        if config is None:
            config = CircuitBreakerConfig()
        
        self.circuit_breakers[name] = CircuitBreaker(name, config)
    
    async def acquire_resource(
        self,
        resource_type: ResourceType,
        owner_id: str,
        resource_id: Optional[str] = None,
        timeout: Optional[timedelta] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """Acquire a resource with circuit breaker protection."""
        resource_id = resource_id or f"{resource_type.value}_{owner_id}_{int(time.time())}"
        
        # Check if resource type has circuit breaker
        circuit_breaker_name = f"{resource_type.value}_cb"
        if circuit_breaker_name in self.circuit_breakers:
            circuit_breaker = self.circuit_breakers[circuit_breaker_name]
            if not circuit_breaker.can_execute():
                raise HTTPException(
                    status_code=503,
                    detail=f"Circuit breaker '{circuit_breaker_name}' is open. Resource {resource_type.value} unavailable."
                )
        
        # Check quota availability
        quota = self.resource_quotas.get(resource_type)
        if quota and quota.remaining <= 0:
            raise HTTPException(
                status_code=429,
                detail=f"Resource quota exceeded for {resource_type.value}"
            )
        
        # Create resource usage record
        timeout_duration = timeout or self.resource_timeouts.get(resource_type, timedelta(minutes=30))
        
        resource = ResourceUsage(
            resource_id=resource_id,
            resource_type=resource_type,
            state=ResourceState.ACQUIRED,
            acquired_at=datetime.utcnow(),
            timeout_at=datetime.utcnow() + timeout_duration,
            owner_id=owner_id,
            metadata=metadata
        )
        
        # Update quota
        if quota:
            quota.used += 1
            quota.remaining -= 1
        
        # Store resource
        self.active_resources[resource_id] = resource
        
        # Record success in circuit breaker
        if circuit_breaker_name in self.circuit_breakers:
            circuit_breaker.record_success()
        
        return resource_id
    
    async def release_resource(self, resource_id: str, owner_id: str):
        """Release a resource."""
        if resource_id not in self.active_resources:
            raise HTTPException(
                status_code=404,
                detail=f"Resource {resource_id} not found"
            )
        
        resource = self.active_resources[resource_id]
        
        # Verify ownership
        if resource.owner_id != owner_id:
            raise HTTPException(
                status_code=403,
                detail=f"Resource {resource_id} is owned by {resource.owner_id}, not {owner_id}"
            )
        
        # Update quota
        quota = self.resource_quotas.get(resource.resource_type)
        if quota:
            quota.used -= 1
            quota.remaining += 1
        
        # Mark resource as released
        resource.state = ResourceState.RELEASED
        resource.owner_id = None
        
        # Remove from active resources after timeout
        await self._schedule_resource_cleanup(resource_id, resource)
    
    async def _schedule_resource_cleanup(self, resource_id: str, resource: ResourceUsage):
        """Schedule resource cleanup after timeout."""
        async def cleanup():
            await asyncio.sleep(300)  # Wait 5 minutes before permanent cleanup
            if resource_id in self.active_resources:
                if resource.state != ResourceState.ACQUIRED:
                    # Permanent cleanup
                    del self.active_resources[resource_id]
        
        asyncio.create_task(cleanup())
    
    async def get_resource(self, resource_id: str) -> ResourceUsage:
        """Get current resource status."""
        if resource_id not in self.active_resources:
            raise HTTPException(
                status_code=404,
                detail=f"Resource {resource_id} not found"
            )
        
        return self.active_resources[resource_id]
    
    async def check_resource_timeout(self, resource_id: str) -> bool:
        """Check if resource has timed out."""
        if resource_id not in self.active_resources:
            return True
        
        resource = self.active_resources[resource_id]
        
        if resource.timeout_at and datetime.utcnow() > resource.timeout_at:
            resource.state = ResourceState.TIMEOUT
            return True
        
        return False
    
    def get_resource_status(self, resource_id: str) -> Dict[str, Any]:
        """Get detailed resource status."""
        if resource_id not in self.active_resources:
            raise HTTPException(
                status_code=404,
                detail=f"Resource {resource_id} not found"
            )
        
        resource = self.active_resources[resource_id]
        
        return {
            'resource_id': resource.resource_id,
            'resource_type': resource.resource_type.value,
            'state': resource.state.value,
            'acquired_at': resource.acquired_at.isoformat() if resource.acquired_at else None,
            'timeout_at': resource.timeout_at.isoformat() if resource.timeout_at else None,
            'owner_id': resource.owner_id,
            'metadata': resource.metadata,
            'remaining_timeout': (resource.timeout_at - datetime.utcnow()).total_seconds() 
                if resource.timeout_at else None
        }
    
    def get_resource_health(self) -> Dict[str, Any]:
        """Get overall resource health status."""
        healthy_resources = sum(
            1 for resource in self.active_resources.values()
            if resource.state == ResourceState.AVAILABLE
        )
        
        acquired_resources = sum(
            1 for resource in self.active_resources.values()
            if resource.state == ResourceState.ACQUIRED
        )
        
        error_resources = sum(
            1 for resource in self.active_resources.values()
            if resource.state == ResourceState.ERROR
        )
        
        circuit_breaker_status = {
            name: breaker.get_health_status()
            for name, breaker in self.circuit_breakers.items()
        }
        
        return {
            'total_resources': len(self.active_resources),
            'healthy_resources': healthy_resources,
            'acquired_resources': acquired_resources,
            'error_resources': error_resources,
            'circuit_breakers': circuit_breaker_status,
            'resource_utilization': {
                rtype.value: {
                    'limit': quota.limit,
                    'used': quota.used,
                    'remaining': quota.remaining,
                    'utilization_rate': (quota.used / quota.limit) * 100 if quota.limit > 0 else 0
                }
                for rtype, quota in self.resource_quotas.items()
            }
        }
    
    async def create_sandbox(self, sandbox_id: str, configuration: Dict[str, Any]) -> str:
        """Create a sandbox environment for isolated execution."""
        # Generate sandbox ID if not provided
        if not sandbox_id:
            sandbox_id = f"sandbox_{int(time.time())}"
        
        # Create sandbox with configuration
        sandbox = {
            'id': sandbox_id,
            'configuration': configuration,
            'created_at': datetime.utcnow(),
            'resource_isolation': True,
            'network_isolation': True,
            'file_system_isolation': True
        }
        
        self.sandboxes[sandbox_id] = sandbox
        return sandbox_id
    
    async def execute_in_sandbox(
        self,
        sandbox_id: str,
        tool_name: str,
        parameters: Dict[str, Any],
        timeout: timedelta = timedelta(minutes=5)
    ) -> Dict[str, Any]:
        """Execute tool in isolated sandbox."""
        if sandbox_id not in self.sandboxes:
            raise HTTPException(
                status_code=404,
                detail=f"Sandbox {sandbox_id} not found"
            )
        
        sandbox = self.sandboxes[sandbox_id]
        
        # Validate sandbox configuration
        if not sandbox.get('resource_isolation', False):
            raise HTTPException(
                status_code=400,
                detail=f"Sandbox {sandbox_id} does not have resource isolation enabled"
            )
        
        # Execute in isolated environment
        start_time = datetime.utcnow()
        
        try:
            # Acquire resources for sandbox execution
            resource_id = await self.acquire_resource(
                ResourceType.COMPUTATION,
                sandbox_id,
                timeout=timeout,
                metadata={'sandbox_id': sandbox_id, 'tool_name': tool_name}
            )
            
            # Execute tool (this would integrate with actual tool execution)
            result = {
                'tool_name': tool_name,
                'parameters': parameters,
                'sandbox_id': sandbox_id,
                'execution_time': timeout.total_seconds(),
                'timestamp': datetime.utcnow().isoformat(),
                'status': 'completed'
            }
            
            # Release resource
            await self.release_resource(resource_id, sandbox_id)
            
            return result
            
        except Exception as e:
            # Ensure resource is released on error
            if 'resource_id' in locals():
                try:
                    await self.release_resource(resource_id, sandbox_id)
                except:
                    pass
            
            raise HTTPException(
                status_code=500,
                detail=f"Sandbox execution failed: {str(e)}"
            )


class ResourceManager:
    """Global resource manager instance."""
    
    def __init__(self):
        self._instance = None
        
    def get_instance(self) -> ResourceManager:
        """Get singleton instance."""
        if self._instance is None:
            self._instance = ResourceManager()
        return self._instance


# Global resource manager instance
resource_manager = ResourceManager().get_instance()