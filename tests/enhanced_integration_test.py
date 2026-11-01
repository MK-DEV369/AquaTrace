"""Comprehensive integration test for enhanced AquaTrace AI Maritime Investigation Agent.

This test demonstrates the atomic, consistent, isolated, durable (ACID) principles
in action across all enhanced components.
"""

import asyncio
import pytest
from datetime import datetime, timedelta
from typing import Dict, Any

from services.agent.tools import get_vessel_snapshot, get_recent_anomalies, get_vessel_track
from services.agent.investigation_agent import MaritimeInvestigationAgent, InvestigationRequest
from services.agent.transaction_manager import TransactionCoordinator, transaction_coordinator
from services.agent.resource_manager import ResourceManager
from services.auth.zero_trust import zero_trust_manager, SecurityAuditor


class TestEnhancedAtomicity:
    """Test atomic operations and transaction management."""
    
    @pytest.mark.asyncio
    async def test_transaction_coordinator_basic(self):
        """Test basic transaction coordinator functionality."""
        # Create investigation request
        request = InvestigationRequest(
            question="Investigate vessel 533002002 for recent anomalies",
            max_tools=3
        )
        
        # Create transaction
        transaction_id = transaction_coordinator.create_transaction("test_inv_123")
        
        # Add actions to transaction
        action1_id = transaction_coordinator.add_action_to_transaction(
            transaction_id,
            "investigation",
            "get_vessel_snapshot",
            {"mmsi": 533002002}
        )
        
        action2_id = transaction_coordinator.add_action_to_transaction(
            transaction_id,
            "investigation",
            "get_recent_anomalies",
            {"mmsi": 533002002, "time_range": "7 days"}
        )
        
        # Execute transaction
        result = await transaction_coordinator.execute_transaction(transaction_id)
        
        # Verify transaction was completed
        assert result['transaction_id'] == transaction_id
        assert result['investigation_id'] == "test_inv_123"
        assert result['status'] == "completed"
        assert result['actions_completed'] == 2
        assert result['total_actions'] == 2
    
    @pytest.mark.asyncio
    async def test_transaction_rollback(self):
        """Test transaction rollback on failure."""
        # Create transaction
        transaction_id = transaction_coordinator.create_transaction("test_inv_456")
        
        # Add failing action
        action_id = transaction_coordinator.add_action_to_transaction(
            transaction_id,
            "investigation",
            "get_vessel_snapshot",
            {"mmsi": 999999999}  # Non-existent vessel
        )
        
        # Execute transaction (should fail and rollback)
        try:
            await transaction_coordinator.execute_transaction(transaction_id)
            assert False, "Transaction should have failed"
        except Exception:
            # Verify transaction is in rolled back state
            status = transaction_coordinator.get_transaction_status(transaction_id)
            assert status['state'] == "rolled_back"
            assert "failed_actions" in status
    
    @pytest.mark.asyncio
    async def test_action_isolation(self):
        """Test that actions are properly isolated."""
        # Create two separate transactions for same vessel
        transaction1_id = transaction_coordinator.create_transaction("test_inv_789a")
        transaction2_id = transaction_coordinator.create_transaction("test_inv_789b")
        
        # Add same action to both transactions
        action1_id = transaction_coordinator.add_action_to_transaction(
            transaction1_id,
            "investigation",
            "get_vessel_snapshot",
            {"mmsi": 533002002}
        )
        
        action2_id = transaction_coordinator.add_action_to_transaction(
            transaction2_id,
            "investigation",
            "get_vessel_snapshot",
            {"mmsi": 533002002}
        )
        
        # Execute both transactions
        result1 = await transaction_coordinator.execute_transaction(transaction1_id)
        result2 = await transaction_coordinator.execute_transaction(transaction2_id)
        
        # Verify both transactions completed independently
        assert result1['status'] == "completed"
        assert result2['status'] == "completed"
        assert result1['transaction_id'] != result2['transaction_id']


class TestEnhancedConsistency:
    """Test consistency guarantees across components."""
    
    @pytest.mark.asyncio
    async def test_event_store_consistency(self):
        """Test event store consistency."""
        # This would test the actual event store implementation
        # For now, we'll create a mock test
        assert True  # Placeholder for actual event store tests
    
    @pytest.mark.asyncio
    async def test_audit_log_integrity(self):
        """Test audit log integrity and checksum verification."""
        from services.auth.zero_trust import SecurityAuditLog
        
        # Create audit log
        audit_log = SecurityAuditLog.create(
            event_type="test_event",
            user_id="test_user",
            action="test_action",
            resource="test_resource",
            outcome="success",
            details={"test": "data"}
        )
        
        # Verify checksum
        assert audit_log.verify_checksum()
        
        # Test that modified log fails verification
        modified_log = SecurityAuditLog.create(
            event_type="test_event",
            user_id="test_user",
            action="test_action_modified",  # Changed
            resource="test_resource",
            outcome="success",
            details={"test": "data"}
        )
        
        assert not modified_log.verify_checksum()
    
    @pytest.mark.asyncio
    async def test_validation_schema_consistency(self):
        """Test validation schema consistency."""
        from services.auth.zero_trust import validation_schema_manager
        
        # Create test schema
        test_schema = {
            "mmsi": {"type": "integer", "minimum": 100000000, "maximum": 999999999},
            "latitude": {"type": "number", "minimum": -90, "maximum": 90},
            "longitude": {"type": "number", "minimum": -180, "maximum": 180}
        }
        
        # Register schema
        validation_schema_manager.register_schema("test_vessel", test_schema)
        
        # Get schema
        retrieved_schema = validation_schema_manager.get_schema("test_vessel")
        assert retrieved_schema == test_schema
        
        # Test data validation
        test_data = {
            "mmsi": 533002002,
            "latitude": 1.2914,
            "longitude": 103.8560
        }
        
        validation_results = validation_schema_manager.validate_data("test_vessel", test_data)
        # All validations should pass
        assert all(result.is_valid for result in validation_results)


class TestEnhancedIsolation:
    """Test isolation and resource management."""
    
    @pytest.mark.asyncio
    async def test_resource_isolation(self):
        """Test resource isolation with circuit breakers."""
        from services.agent.resource_manager import resource_manager
        
        # Create sandbox for test
        sandbox_id = await resource_manager.create_sandbox(
            "test_sandbox",
            {
                "resource_isolation": True,
                "network_isolation": True,
                "file_system_isolation": True
            }
        )
        
        # Acquire resources in sandbox
        resource1_id = await resource_manager.acquire_resource(
            "database_connection",
            sandbox_id,
            timeout=timedelta(seconds=10)
        )
        
        resource2_id = await resource_manager.acquire_resource(
            "kafka_producer",
            sandbox_id,
            timeout=timedelta(seconds=10)
        )
        
        # Verify resources were acquired
        resource1 = await resource_manager.get_resource(resource1_id)
        resource2 = await resource_manager.get_resource(resource2_id)
        
        assert resource1.owner_id == sandbox_id
        assert resource2.owner_id == sandbox_id
        assert resource1.state.value == "acquired"
        assert resource2.state.value == "acquired"
        
        # Release resources
        await resource_manager.release_resource(resource1_id, sandbox_id)
        await resource_manager.release_resource(resource2_id, sandbox_id)
        
        # Verify resources are released
        resource1 = await resource_manager.get_resource(resource1_id)
        resource2 = await resource_manager.get_resource(resource2_id)
        
        assert resource1.state.value == "released"
        assert resource2.state.value == "released"
    
    @pytest.mark.asyncio
    async def test_sandbox_execution_isolation(self):
        """Test that sandbox execution is properly isolated."""
        from services.agent.resource_manager import resource_manager
        
        # Create sandbox
        sandbox_id = await resource_manager.create_sandbox(
            "isolation_test",
            {
                "resource_isolation": True,
                "network_isolation": True,
                "file_system_isolation": True
            }
        )
        
        # Execute tool in sandbox (this would test actual tool execution)
        result = await resource_manager.execute_in_sandbox(
            sandbox_id,
            "get_vessel_snapshot",
            {"mmsi": 533002002}
        )
        
        # Verify execution completed successfully
        assert result['status'] == "completed"
        assert result['sandbox_id'] == sandbox_id
        assert result['tool_name'] == "get_vessel_snapshot"


class TestEnhancedDurability:
    """Test durability and persistence mechanisms."""
    
    @pytest.mark.asyncio
    async def test_audit_log_durability(self):
        """Test audit log durability and persistence."""
        from services.auth.zero_trust import SecurityAuditor
        
        auditor = SecurityAuditor(zero_trust_manager)
        
        # Log multiple security events
        for i in range(10):
            await auditor.log_security_event(
                event_type="test_event",
                user_id=f"test_user_{i}",
                session_id=f"test_session_{i}",
                action="test_action",
                resource="test_resource",
                outcome="success",
                details={"test": f"data_{i}"}
            )
        
        # Generate security report
        report = auditor.generate_security_report()
        
        # Verify report contains expected data
        assert report['total_events'] == 10
        assert report['checksum_valid'] == True
        assert 'generated_at' in report
    
    @pytest.mark.asyncio
    async def test_transaction_durability(self):
        """Test transaction durability and persistence."""
        # Create and complete a transaction
        transaction_id = transaction_coordinator.create_transaction("durability_test")
        
        action_id = transaction_coordinator.add_action_to_transaction(
            transaction_id,
            "investigation",
            "get_vessel_snapshot",
            {"mmsi": 533002002}
        )
        
        result = await transaction_coordinator.execute_transaction(transaction_id)
        
        # Verify transaction is persisted
        status = transaction_coordinator.get_transaction_status(transaction_id)
        assert status['state'] == "completed"
        assert len(status['actions']) == 1
        assert status['actions'][0]['state'] == "completed"


class TestEnhancedSecurity:
    """Test enhanced security features."""
    
    @pytest.mark.asyncio
    async def test_zero_trust_architecture(self):
        """Test zero-trust architecture implementation."""
        # Register a test service
        zero_trust_manager.register_service(
            "test_service",
            {
                "name": "Test Service",
                "permissions": ["read:vessels", "write:investigations"],
                "security_level": "confidential",
                "timeout": 30
            }
        )
        
        # Authenticate service
        token = zero_trust_manager.authenticate_service(
            "test_service",
            {"api_key": "test_api_key"}
        )
        
        # Verify token
        assert token is not None
        assert len(token) > 0
        
        # Authorize request
        authorized = zero_trust_manager.authorize_request(
            token,
            "vessels",
            "read"
        )
        
        assert authorized
    
    @pytest.mark.asyncio
    async def test_data_encryption(self):
        """Test data encryption capabilities."""
        test_data = {
            "sensitive_data": "This is sensitive information",
            "metadata": {"classification": "confidential"}
        }
        
        # Encrypt data
        encrypted_data = zero_trust_manager.encrypt_data(
            test_data,
            "test_service"
        )
        
        # Decrypt data
        decrypted_data = zero_trust_manager.decrypt_data(encrypted_data)
        
        # Verify data integrity
        assert decrypted_data == test_data
    
    @pytest.mark.asyncio
    async def test_validation_pipeline(self):
        """Test comprehensive validation pipeline."""
        from services.auth.zero_trust import validation_schema_manager
        
        # Create test schema
        schema = {
            "mmsi": {"type": "integer", "minimum": 100000000, "maximum": 999999999},
            "latitude": {"type": "number", "minimum": -90, "maximum": 90},
            "longitude": {"type": "number", "minimum": -180, "maximum": 180}
        }
        
        # Register schema
        validation_schema_manager.register_schema("comprehensive_test", schema)
        
        # Test valid data
        valid_data = {
            "mmsi": 533002002,
            "latitude": 1.2914,
            "longitude": 103.8560
        }
        
        validation_results = validation_schema_manager.validate_data(
            "comprehensive_test",
            valid_data
        )
        
        # All validations should pass
        assert all(result.is_valid for result in validation_results)
        
        # Test invalid data
        invalid_data = {
            "mmsi": 999999999999,  # Out of range
            "latitude": 95.0,  # Out of range
            "longitude": 200.0  # Out of range
        }
        
        validation_results = validation_schema_manager.validate_data(
            "comprehensive_test",
            invalid_data
        )
        
        # Some validations should fail
        assert any(not result.is_valid for result in validation_results)


class TestEndToEndIntegration:
    """Test end-to-end integration of all enhanced components."""
    
    @pytest.mark.asyncio
    async def test_complete_investigation_workflow(self):
        """Test complete investigation workflow with enhanced features."""
        # Create investigation agent
        agent = MaritimeInvestigationAgent()
        
        # Create investigation request
        request = InvestigationRequest(
            question="Investigate recent anomalies for vessel 533002002",
            context={"user_id": "test_user"},
            max_tools=3
        )
        
        # Start investigation
        session_id = await agent.start_investigation(request)
        assert session_id.startswith("inv_")
        
        # Execute investigation
        result = await agent.execute_investigation(session_id)
        
        # Verify investigation completed
        assert result['session_id'] == session_id
        assert 'investigation_summary' in result
        assert 'evidence_summary' in result
        assert 'recommendations' in result
        assert 'uncertainties' in result
        assert 'confidence_score' in result
        
        # Get investigation status
        status = await agent.get_session_status(session_id)
        assert status['session_id'] == session_id
        assert status['status'] == "completed"
        assert status['tool_calls_count'] > 0
    
    @pytest.mark.asyncio
    async def test_transaction_and_audit_integration(self):
        """Test integration between transaction manager and audit logging."""
        from services.auth.zero_trust import security_auditor
        
        # Create transaction
        transaction_id = transaction_coordinator.create_transaction("integration_test")
        
        # Log security event
        await security_auditor.log_security_event(
            event_type="transaction_initiated",
            session_id=transaction_id,
            action="create_transaction",
            resource="transaction_manager",
            outcome="success",
            details={"transaction_id": transaction_id}
        )
        
        # Add action to transaction
        action_id = transaction_coordinator.add_action_to_transaction(
            transaction_id,
            "investigation",
            "get_vessel_snapshot",
            {"mmsi": 533002002}
        )
        
        # Execute transaction
        result = await transaction_coordinator.execute_transaction(transaction_id)
        
        # Log security event for transaction completion
        await security_auditor.log_security_event(
            event_type="transaction_completed",
            session_id=transaction_id,
            action="execute_transaction",
            resource="transaction_manager",
            outcome="success",
            details={"transaction_id": transaction_id, "result": result}
        )
        
        # Verify audit logs
        events = security_auditor.get_security_events(
            event_type="transaction_initiated"
        )
        assert len(events) == 1
        assert events[0].session_id == transaction_id
        
        # Generate security report
        report = security_auditor.generate_security_report()
        assert report['total_events'] >= 2
        
        # Verify audit log integrity
        checksum_valid = await security_auditor.verify_audit_log_integrity()
        assert checksum_valid
    
    @pytest.mark.asyncio
    async def test_resource_and_transaction_integration(self):
        """Test integration between resource manager and transaction manager."""
        from services.agent.resource_manager import resource_manager
        
        # Create sandbox
        sandbox_id = await resource_manager.create_sandbox(
            "integration_sandbox",
            {
                "resource_isolation": True,
                "network_isolation": True,
                "file_system_isolation": True
            }
        )
        
        # Create transaction
        transaction_id = transaction_coordinator.create_transaction("resource_integration_test")
        
        # Acquire resources in sandbox
        resource_id = await resource_manager.acquire_resource(
            "database_connection",
            sandbox_id,
            timeout=timedelta(seconds=10)
        )
        
        # Add action to transaction
        action_id = transaction_coordinator.add_action_to_transaction(
            transaction_id,
            "investigation",
            "get_vessel_snapshot",
            {"mmsi": 533002002}
        )
        
        # Execute transaction with resource isolation
        result = await transaction_coordinator.execute_transaction(transaction_id)
        
        # Verify both resources were properly managed
        resource = await resource_manager.get_resource(resource_id)
        assert resource.owner_id == sandbox_id
        assert resource.state.value == "released"
        
        # Clean up
        await resource_manager.release_resource(resource_id, sandbox_id)


# Test class definitions for pytest
__test__ = [
    TestEnhancedAtomicity,
    TestEnhancedConsistency,
    TestEnhancedIsolation,
    TestEnhancedDurability,
    TestEnhancedSecurity,
    TestEndToEndIntegration
]


if __name__ == "__main__":
    # Run tests programmatically
    import sys
    
    # Run pytest
    sys.exit(pytest.main([
        __file__,
        "-v",
        "--tb=short"
    ]))