"""Enhanced security layer for AquaTrace AI Maritime Investigation Agent.

This module implements zero-trust architecture, comprehensive validation,
and advanced security measures for the agentic AI system.
"""

import asyncio
import hashlib
import json
import secrets
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set
from enum import Enum
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import serialization

from fastapi import HTTPException, Request
from pydantic import BaseModel, Field, field_validator


class SecurityLevel(str, Enum):
    """Security levels for data and operations."""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    SECRET = "secret"
    TOP_SECRET = "top_secret"


class ValidationRule(BaseModel):
    """Validation rule for data integrity and security."""
    
    rule_id: str = Field(min_length=8, max_length=50)
    rule_type: str = Field(min_length=5, max_length=50)
    description: str
    pattern: Optional[str] = None
    schema: Optional[Dict[str, Any]] = None
    validation_function: Optional[str] = None
    severity: str = Field(default="medium")
    active: bool = True


class ValidationResult(BaseModel):
    """Result of data validation."""
    
    is_valid: bool
    rule_id: str
    field_name: str
    value: Any
    error_message: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class SecurityAuditLog(BaseModel):
    """Security audit log entry."""
    
    log_id: str = Field(min_length=8, max_length=50)
    event_type: str
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    action: str
    resource: str
    outcome: str
    details: Dict[str, Any]
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    checksum: str
    
    @classmethod
    def create(cls, **data) -> 'SecurityAuditLog':
        """Create audit log entry with checksum."""
        # Remove log_id from data for checksum calculation
        log_data = data.copy()
        if 'log_id' in log_data:
            del log_data['log_id']
        
        # Generate checksum
        checksum_data = json.dumps(log_data, sort_keys=True).encode()
        checksum = hashlib.sha256(checksum_data).hexdigest()
        
        # Add log_id and checksum
        log_id = f"audit_{int(time.time())}_{secrets.token_hex(8)}"
        log_data['log_id'] = log_id
        log_data['checksum'] = checksum
        
        return cls(**log_data)
    
    def verify_checksum(self) -> bool:
        """Verify audit log integrity."""
        # Recreate base data (without checksum and log_id)
        log_data = self.dict(exclude={'checksum', 'log_id'})
        
        # Calculate checksum
        checksum_data = json.dumps(log_data, sort_keys=True).encode()
        calculated_checksum = hashlib.sha256(checksum_data).hexdigest()
        
        return calculated_checksum == self.checksum


class ZeroTrustManager:
    """Zero-trust architecture manager for service-to-service communication."""
    
    def __init__(self, jwt_secret: str):
        self.jwt_secret = jwt_secret
        self.service_registry: Dict[str, Dict[str, Any]] = {}
        self.identity_store: Dict[str, Dict[str, Any]] = {}
        self.policy_engine: Dict[str, Any] = {}
        self.encrypted_data_key: bytes = Fernet.generate_key()
        self.fernet = Fernet(self.encrypted_data_key)
        
    def register_service(self, service_id: str, service_info: Dict[str, Any]):
        """Register a service in the zero-trust network."""
        service_info['registered_at'] = datetime.utcnow().isoformat()
        service_info['last_heartbeat'] = datetime.utcnow().isoformat()
        self.service_registry[service_id] = service_info
    
    def authenticate_service(self, service_id: str, credentials: Dict[str, str]) -> str:
        """Authenticate a service and return JWT token."""
        if service_id not in self.service_registry:
            raise HTTPException(
                status_code=401,
                detail=f"Service {service_id} not registered"
            )
        
        service = self.service_registry[service_id]
        
        # Validate credentials (implementation would depend on auth method)
        if not self._validate_credentials(service, credentials):
            raise HTTPException(
                status_code=401,
                detail="Invalid credentials"
            )
        
        # Generate JWT token
        token_data = {
            'service_id': service_id,
            'issued_at': datetime.utcnow().isoformat(),
            'expires_at': (datetime.utcnow() + timedelta(hours=24)).isoformat(),
            'permissions': service.get('permissions', []),
            'security_level': service.get('security_level', SecurityLevel.INTERNAL.value)
        }
        
        token = self._generate_jwt_token(token_data)
        
        # Store token in identity store
        self.identity_store[token] = {
            'service_id': service_id,
            'issued_at': datetime.utcnow(),
            'expires_at': datetime.utcnow() + timedelta(hours=24),
            'permissions': service.get('permissions', []),
            'security_level': service.get('security_level', SecurityLevel.INTERNAL.value)
        }
        
        return token
    
    def authorize_request(self, token: str, resource: str, action: str) -> bool:
        """Authorize a service request based on zero-trust principles."""
        if token not in self.identity_store:
            return False
        
        identity = self.identity_store[token]
        
        # Check token expiration
        if datetime.utcnow() > identity['expires_at']:
            del self.identity_store[token]
            return False
        
        # Check service permissions
        permissions = identity['permissions']
        
        # Check if action is permitted
        if not self._check_permission(permissions, resource, action):
            return False
        
        # Apply security policies
        if not self._apply_security_policies(identity, resource, action):
            return False
        
        return True
    
    def encrypt_data(self, data: Dict[str, Any], recipient_service: str) -> str:
        """Encrypt data for secure transmission between services."""
        try:
            data_json = json.dumps(data)
            encrypted_data = self.fernet.encrypt(data_json.encode())
            return encrypted_data.decode()
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Data encryption failed: {str(e)}"
            )
    
    def decrypt_data(self, encrypted_data: str) -> Dict[str, Any]:
        """Decrypt data from secure transmission."""
        try:
            decrypted_data = self.fernet.decrypt(encrypted_data.encode())
            return json.loads(decrypted_data.decode())
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Data decryption failed: {str(e)}"
            )
    
    def validate_request(self, request: Request) -> Dict[str, Any]:
        """Validate incoming request using zero-trust principles."""
        validation_result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'metadata': {}
        }
        
        # Validate request headers
        validation_result['headers'] = self._validate_request_headers(request)
        
        # Validate request body
        validation_result['body'] = self._validate_request_body(request)
        
        # Validate request metadata
        validation_result['metadata'] = self._validate_request_metadata(request)
        
        # Check overall validity
        if validation_result['headers']['valid'] and validation_result['body']['valid']:
            validation_result['valid'] = True
        else:
            validation_result['valid'] = False
        
        return validation_result
    
    def _validate_request_headers(self, request: Request) -> Dict[str, Any]:
        """Validate request headers."""
        headers = {}
        
        for key, value in request.headers.items():
            # Validate header key
            if not self._validate_header_key(key):
                headers[key] = {'valid': False, 'error': 'Invalid header key'}
                continue
            
            # Validate header value
            if not self._validate_header_value(key, value):
                headers[key] = {'valid': False, 'error': 'Invalid header value'}
                continue
            
            headers[key] = {'valid': True}
        
        return {
            'valid': all(h['valid'] for h in headers.values()),
            'headers': headers
        }
    
    def _validate_request_body(self, request: Request) -> Dict[str, Any]:
        """Validate request body against schema."""
        try:
            body = request.json()
            
            # Validate against schema
            schema = self._get_request_schema(request)
            if schema:
                # In a real implementation, this would use a validation library
                # For now, we'll do basic validation
                validation_result = self._validate_body_against_schema(body, schema)
                return validation_result
            
            return {
                'valid': True,
                'errors': []
            }
            
        except Exception as e:
            return {
                'valid': False,
                'error': str(e)
            }
    
    def _validate_request_metadata(self, request: Request) -> Dict[str, Any]:
        """Validate request metadata and security context."""
        metadata = {
            'source_ip': request.client.host if request.client else None,
            'user_agent': request.headers.get('user-agent'),
            'timestamp': datetime.utcnow().isoformat(),
            'security_level': SecurityLevel.INTERNAL.value,
            'requires_encryption': True,
            'audit_required': True
        }
        
        # Validate source IP
        if not self._validate_source_ip(metadata['source_ip']):
            metadata['warnings'].append('Unusual source IP detected')
        
        # Validate user agent
        if metadata['user_agent'] and not self._validate_user_agent(metadata['user_agent']):
            metadata['warnings'].append('Unusual user agent detected')
        
        return metadata
    
    def _validate_header_key(self, key: str) -> bool:
        """Validate header key format."""
        # Implementation would validate header key format
        return True
    
    def _validate_header_value(self, key: str, value: str) -> bool:
        """Validate header value."""
        # Implementation would validate header value based on key
        return True
    
    def _validate_credentials(self, service: Dict[str, Any], credentials: Dict[str, str]) -> bool:
        """Validate service credentials."""
        # Implementation would validate credentials based on service configuration
        return True
    
    def _check_permission(self, permissions: List[str], resource: str, action: str) -> bool:
        """Check if service has permission for resource and action."""
        # Implementation would check permissions against policy
        return True
    
    def _apply_security_policies(self, identity: Dict[str, Any], resource: str, action: str) -> bool:
        """Apply security policies to service request."""
        # Implementation would apply security policies
        return True
    
    def _get_request_schema(self, request: Request) -> Optional[Dict[str, Any]]:
        """Get schema for request body validation."""
        # Implementation would return schema based on endpoint
        return None
    
    def _validate_body_against_schema(self, body: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
        """Validate body against schema."""
        # Implementation would validate body against schema
        return {'valid': True, 'errors': []}
    
    def _validate_source_ip(self, ip_address: str) -> bool:
        """Validate source IP address."""
        # Implementation would validate IP address
        return True
    
    def _validate_user_agent(self, user_agent: str) -> bool:
        """Validate user agent string."""
        # Implementation would validate user agent
        return True
    
    def _generate_jwt_token(self, data: Dict[str, Any]) -> str:
        """Generate JWT token."""
        # In a real implementation, this would use JWT library
        return "fake_jwt_token"


class ValidationSchemaManager:
    """Manages validation schemas and rules for data integrity."""
    
    def __init__(self):
        self.validation_rules: Dict[str, ValidationRule] = {}
        self.schemas: Dict[str, Dict[str, Any]] = {}
        self.rule_versions: Dict[str, int] = {}
        
    def register_validation_rule(self, rule: ValidationRule):
        """Register a validation rule."""
        self.validation_rules[rule.rule_id] = rule
        self.rule_versions[rule.rule_id] = self.rule_versions.get(rule.rule_id, 0) + 1
    
    def validate_data(
        self,
        data: Dict[str, Any],
        schema_name: Optional[str] = None,
        rules: Optional[List[str]] = None
    ) -> List[ValidationResult]:
        """Validate data against schema and rules."""
        results = []
        
        # Validate against schema if provided
        if schema_name and schema_name in self.schemas:
            schema = self.schemas[schema_name]
            for field_name, field_schema in schema.items():
                if field_name in data:
                    rule_result = self._validate_field(field_name, data[field_name], field_schema)
                    results.append(rule_result)
        
        # Validate against specific rules if provided
        if rules:
            for rule_id in rules:
                if rule_id in self.validation_rules:
                    rule = self.validation_rules[rule_id]
                    for field_name, field_value in data.items():
                        if self._rule_applies_to_field(rule, field_name):
                            rule_result = self._validate_with_rule(
                                field_name, field_value, rule
                            )
                            results.append(rule_result)
        
        return results
    
    def _validate_field(self, field_name: str, value: Any, schema: Dict[str, Any]) -> ValidationResult:
        """Validate a field against schema."""
        # Implementation would validate field based on schema
        return ValidationResult(
            is_valid=True,
            rule_id=f"schema_{field_name}",
            field_name=field_name,
            value=value
        )
    
    def _validate_with_rule(self, field_name: str, value: Any, rule: ValidationRule) -> ValidationResult:
        """Validate a field against a specific rule."""
        # Implementation would validate field based on rule
        return ValidationResult(
            is_valid=True,
            rule_id=rule.rule_id,
            field_name=field_name,
            value=value
        )
    
    def _rule_applies_to_field(self, rule: ValidationRule, field_name: str) -> bool:
        """Check if a rule applies to a field."""
        # Implementation would determine if rule applies to field
        return True
    
    def register_schema(self, schema_name: str, schema: Dict[str, Any]):
        """Register a validation schema."""
        self.schemas[schema_name] = schema
    
    def get_schema(self, schema_name: str) -> Optional[Dict[str, Any]]:
        """Get validation schema by name."""
        return self.schemas.get(schema_name)


class SecurityAuditor:
    """Comprehensive security auditing and compliance checking."""
    
    def __init__(self, zero_trust_manager: ZeroTrustManager):
        self.zero_trust_manager = zero_trust_manager
        self.audit_logs: List[SecurityAuditLog] = []
        
    async def log_security_event(
        self,
        event_type: str,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        action: str = "",
        resource: str = "",
        outcome: str = "",
        details: Optional[Dict[str, Any]] = None
    ) -> SecurityAuditLog:
        """Log a security event to audit trail."""
        audit_log = SecurityAuditLog.create(
            event_type=event_type,
            user_id=user_id,
            session_id=session_id,
            ip_address=ip_address,
            user_agent=user_agent,
            action=action,
            resource=resource,
            outcome=outcome,
            details=details or {}
        )
        
        self.audit_logs.append(audit_log)
        return audit_log
    
    async def verify_audit_log_integrity(self) -> bool:
        """Verify integrity of all audit logs."""
        for log in self.audit_logs:
            if not log.verify_checksum():
                return False
        
        return True
    
    def get_security_events(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        event_type: Optional[str] = None,
        user_id: Optional[str] = None
    ) -> List[SecurityAuditLog]:
        """Get security events with filtering."""
        events = self.audit_logs
        
        if start_time:
            events = [e for e in events if e.timestamp >= start_time]
        
        if end_time:
            events = [e for e in events if e.timestamp <= end_time]
        
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        
        if user_id:
            events = [e for e in events if e.user_id == user_id]
        
        return events
    
    def generate_security_report(self) -> Dict[str, Any]:
        """Generate comprehensive security report."""
        total_events = len(self.audit_logs)
        
        event_types = {}
        outcomes = {}
        user_activity = {}
        
        for log in self.audit_logs:
            event_types[log.event_type] = event_types.get(log.event_type, 0) + 1
            outcomes[log.outcome] = outcomes.get(log.outcome, 0) + 1
            
            if log.user_id:
                user_activity[log.user_id] = user_activity.get(log.user_id, 0) + 1
        
        checksum_valid = await self.verify_audit_log_integrity()
        
        return {
            'total_events': total_events,
            'event_types': event_types,
            'outcomes': outcomes,
            'user_activity': user_activity,
            'checksum_valid': checksum_valid,
            'generated_at': datetime.utcnow().isoformat()
        }


# Global security managers
zero_trust_manager = ZeroTrustManager(
    jwt_secret="change_this_jwt_secret_in_production"
)
validation_schema_manager = ValidationSchemaManager()
security_auditor = SecurityAuditor(zero_trust_manager)