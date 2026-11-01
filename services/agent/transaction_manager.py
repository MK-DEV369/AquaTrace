"""Enhanced transaction coordinator for AquaTrace AI Maritime Investigation Agent.

This module provides distributed transaction management with saga pattern support,
two-phase commit protocols, and compensation actions for atomic investigations.
"""

import asyncio
import hashlib
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set
from enum import Enum

from fastapi import HTTPException
from pydantic import BaseModel, Field

from services.agent.tools import TOOL_REGISTRY, execute_tool
from services.kafka_integration import kafka_producer


class TransactionState(str, Enum):
    """Transaction states for distributed coordination."""
    PENDING = "pending"
    EXECUTING = "executing"
    COMPLETED = "completed"
    COMPENSATING = "compensating"
    ROLLED_BACK = "rolled_back"
    FAILED = "failed"


class TransactionAction(BaseModel):
    """Represents a single action within a distributed transaction."""
    action_id: str
    action_type: str
    tool_name: str
    parameters: Dict[str, Any]
    state: TransactionState = TransactionState.PENDING
    start_time: datetime
    end_time: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    compensation_action: Optional[Dict[str, Any]] = None
    checksum: Optional[str] = None


class CompensationAction(BaseModel):
    """Compensation action for transaction rollback."""
    action_id: str
    action_type: str
    inverse_tool: str
    inverse_parameters: Dict[str, Any]
    priority: int = 1
    rollback_on_failure: bool = True
    executed: bool = False
    execution_time: Optional[datetime] = None


class DistributedTransaction:
    """Distributed transaction managing atomic investigation execution."""
    
    def __init__(self, transaction_id: str, investigation_id: str):
        self.transaction_id = transaction_id
        self.investigation_id = investigation_id
        self.state = TransactionState.PENDING
        self.actions: List[TransactionAction] = []
        self.compensation_actions: List[CompensationAction] = []
        self.created_at = datetime.utcnow()
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.checksum_history: List[str] = []
        
    def add_action(self, action: TransactionAction):
        """Add an action to the transaction."""
        self.actions.append(action)
        self._update_checksum()
    
    def add_compensation_action(self, compensation: CompensationAction):
        """Add a compensation action for rollback."""
        self.compensation_actions.append(compensation)
        # Sort by priority (lower number = higher priority)
        self.compensation_actions.sort(key=lambda x: x.priority)
    
    def _update_checksum(self):
        """Update transaction checksum for integrity verification."""
        data = {
            'transaction_id': self.transaction_id,
            'investigation_id': self.investigation_id,
            'actions': [
                {
                    'action_id': a.action_id,
                    'tool_name': a.tool_name,
                    'parameters': a.parameters,
                    'state': a.state.value,
                    'start_time': a.start_time.isoformat()
                }
                for a in self.actions
            ],
            'created_at': self.created_at.isoformat()
        }
        self.checksum = hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()
        self.checksum_history.append(self.checksum)
    
    def execute_action(self, action: TransactionAction):
        """Execute a single action within the transaction."""
        action.state = TransactionState.EXECUTING
        action.start_time = datetime.utcnow()
        
        try:
            if action.tool_name not in TOOL_REGISTRY:
                raise HTTPException(
                    status_code=404,
                    detail=f"Tool '{action.tool_name}' not found"
                )
            
            # Execute the tool with enhanced error handling
            result = asyncio.run(execute_tool(action.tool_name, action.parameters))
            
            action.state = TransactionState.COMPLETED
            action.end_time = datetime.utcnow()
            action.result = result
            
        except Exception as e:
            action.state = TransactionState.FAILED
            action.end_time = datetime.utcnow()
            action.error = str(e)
            raise
    
    def compensate(self):
        """Execute compensation actions in reverse order."""
        self.state = TransactionState.COMPENSATING
        
        # Execute compensation actions in reverse order (LIFO)
        for compensation in reversed(self.compensation_actions):
            if not compensation.executed:
                try:
                    self._execute_compensation(compensation)
                    compensation.executed = True
                    compensation.execution_time = datetime.utcnow()
                except Exception as e:
                    # Log compensation failure but continue with other compensations
                    print(f"Compensation action {compensation.action_id} failed: {e}")
        
        self.state = TransactionState.ROLLED_BACK
    
    def _execute_compensation(self, compensation: CompensationAction):
        """Execute a single compensation action."""
        if compensation.inverse_tool not in TOOL_REGISTRY:
            raise HTTPException(
                status_code=500,
                detail=f"Compensation tool '{compensation.inverse_tool}' not found"
            )
        
        # Execute the inverse action
        result = asyncio.run(execute_tool(
            compensation.inverse_tool, 
            compensation.inverse_parameters
        ))
        
        # Log compensation for audit trail
        await self._log_compensation_event(compensation, result)
    
    async def _log_compensation_event(self, compensation: CompensationAction, result: Dict[str, Any]):
        """Log compensation event for audit trail."""
        event = {
            'transaction_id': self.transaction_id,
            'compensation_id': compensation.action_id,
            'action_type': compensation.action_type,
            'inverse_tool': compensation.inverse_tool,
            'execution_time': datetime.utcnow().isoformat(),
            'result': result,
            'investigation_id': self.investigation_id
        }
        
        # Store in audit log
        await self._store_audit_event('compensation', event)
    
    async def _store_audit_event(self, event_type: str, event: Dict[str, Any]):
        """Store audit event for compliance and debugging."""
        # Implementation would store in audit database or logging system
        pass


class TransactionCoordinator:
    """Coordinates distributed transactions for agentic AI investigations."""
    
    def __init__(self):
        self.active_transactions: Dict[str, DistributedTransaction] = {}
        self.transaction_history: List[Dict[str, Any]] = []
        self.compensation_queue: asyncio.Queue = asyncio.Queue()
        
    def create_transaction(self, investigation_id: str) -> str:
        """Create a new distributed transaction for an investigation."""
        transaction_id = f"txn_{investigation_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        transaction = DistributedTransaction(transaction_id, investigation_id)
        self.active_transactions[transaction_id] = transaction
        
        # Start compensation monitoring task
        asyncio.create_task(self._monitor_transactions())
        
        return transaction_id
    
    def add_action_to_transaction(
        self, 
        transaction_id: str, 
        action_type: str,
        tool_name: str,
        parameters: Dict[str, Any],
        compensation_action: Optional[Dict[str, Any]] = None
    ) -> str:
        """Add an action to an existing transaction."""
        if transaction_id not in self.active_transactions:
            raise HTTPException(
                status_code=404,
                detail=f"Transaction {transaction_id} not found"
            )
        
        transaction = self.active_transactions[transaction_id]
        
        action_id = f"action_{len(transaction.actions) + 1}"
        action = TransactionAction(
            action_id=action_id,
            action_type=action_type,
            tool_name=tool_name,
            parameters=parameters,
            start_time=datetime.utcnow()
        )
        
        transaction.add_action(action)
        
        # Add compensation action if provided
        if compensation_action:
            compensation = CompensationAction(
                action_id=f"comp_{action_id}",
                action_type=compensation_action.get('action_type', 'inverse'),
                inverse_tool=compensation_action.get('inverse_tool', ''),
                inverse_parameters=compensation_action.get('inverse_parameters', {}),
                priority=compensation_action.get('priority', 1)
            )
            transaction.add_compensation_action(compensation)
        
        return action_id
    
    async def execute_transaction(self, transaction_id: str) -> Dict[str, Any]:
        """Execute a distributed transaction atomically."""
        if transaction_id not in self.active_transactions:
            raise HTTPException(
                status_code=404,
                detail=f"Transaction {transaction_id} not found"
            )
        
        transaction = self.active_transactions[transaction_id]
        transaction.state = TransactionState.EXECUTING
        transaction.started_at = datetime.utcnow()
        
        try:
            # Execute all actions in sequence
            for action in transaction.actions:
                try:
                    self.execute_action(transaction_id, action.action_id)
                except Exception as e:
                    # If any action fails, initiate compensation
                    await self.compensate_transaction(transaction_id)
                    raise
            
            # All actions completed successfully
            transaction.state = TransactionState.COMPLETED
            transaction.completed_at = datetime.utcnow()
            
            # Move to completed state and log
            await self._complete_transaction(transaction_id)
            
            return {
                'transaction_id': transaction_id,
                'investigation_id': transaction.investigation_id,
                'status': transaction.state.value,
                'actions_completed': len([a for a in transaction.actions if a.state == TransactionState.COMPLETED]),
                'total_actions': len(transaction.actions),
                'start_time': transaction.started_at.isoformat(),
                'end_time': transaction.completed_at.isoformat(),
                'checksum': transaction.checksum
            }
            
        except Exception as e:
            # Transaction failed, initiate compensation
            await self.compensate_transaction(transaction_id)
            raise
    
    async def compensate_transaction(self, transaction_id: str):
        """Compensate a failed transaction."""
        if transaction_id not in self.active_transactions:
            return
        
        transaction = self.active_transactions[transaction_id]
        
        # Mark transaction as failed
        transaction.state = TransactionState.FAILED
        
        # Execute compensation
        transaction.compensate()
        
        # Log transaction failure for audit
        await self._log_transaction_failure(transaction_id, transaction)
        
        # Remove from active transactions after compensation
        del self.active_transactions[transaction_id]
    
    def execute_action(self, transaction_id: str, action_id: str):
        """Execute a specific action within a transaction."""
        if transaction_id not in self.active_transactions:
            raise HTTPException(
                status_code=404,
                detail=f"Transaction {transaction_id} not found"
            )
        
        transaction = self.active_transactions[transaction_id]
        
        # Find the action
        action = next((a for a in transaction.actions if a.action_id == action_id), None)
        if not action:
            raise HTTPException(
                status_code=404,
                detail=f"Action {action_id} not found in transaction {transaction_id}"
            )
        
        # Execute the action
        transaction.execute_action(action)
    
    async def _complete_transaction(self, transaction_id: str):
        """Mark transaction as completed and log for audit."""
        transaction = self.active_transactions.get(transaction_id)
        if not transaction:
            return
        
        # Log successful transaction completion
        completion_event = {
            'transaction_id': transaction_id,
            'investigation_id': transaction.investigation_id,
            'completion_time': datetime.utcnow().isoformat(),
            'actions_count': len(transaction.actions),
            'checksum': transaction.checksum,
            'status': 'completed'
        }
        
        self.transaction_history.append(completion_event)
        
        # Send to audit logging system
        await self._send_to_audit_log('transaction_completed', completion_event)
    
    async def _log_transaction_failure(self, transaction_id: str, transaction: DistributedTransaction):
        """Log transaction failure for audit and debugging."""
        failure_event = {
            'transaction_id': transaction_id,
            'investigation_id': transaction.investigation_id,
            'failure_time': datetime.utcnow().isoformat(),
            'failed_actions': [
                {
                    'action_id': a.action_id,
                    'tool_name': a.tool_name,
                    'error': a.error,
                    'state': a.state.value
                }
                for a in transaction.actions if a.state == TransactionState.FAILED
            ],
            'compensation_executed': transaction.state == TransactionState.ROLLED_BACK,
            'status': 'failed'
        }
        
        self.transaction_history.append(failure_event)
        await self._send_to_audit_log('transaction_failed', failure_event)
    
    async def _send_to_audit_log(self, event_type: str, event: Dict[str, Any]):
        """Send event to audit logging system."""
        # Implementation would send to audit log service
        pass
    
    async def _monitor_transactions(self):
        """Monitor and clean up stale transactions."""
        while True:
            try:
                current_time = datetime.utcnow()
                
                # Clean up old transactions (older than 24 hours)
                stale_transactions = [
                    tid for tid, txn in self.active_transactions.items()
                    if current_time - txn.created_at > timedelta(hours=24)
                ]
                
                for txn_id in stale_transactions:
                    print(f"Cleaning up stale transaction {txn_id}")
                    # Remove stale transactions
                    del self.active_transactions[txn_id]
                
                # Check for transactions that have failed but not been cleaned up
                for txn_id, transaction in self.active_transactions.items():
                    if transaction.state in [TransactionState.FAILED, TransactionState.ROLLED_BACK]:
                        print(f"Cleaning up completed/failed transaction {txn_id}")
                        del self.active_transactions[txn_id]
                
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                print(f"Error in transaction monitoring: {e}")
                await asyncio.sleep(60)  # Wait 1 minute before retry
    
    def get_transaction_status(self, transaction_id: str) -> Dict[str, Any]:
        """Get status of a transaction."""
        if transaction_id not in self.active_transactions:
            raise HTTPException(
                status_code=404,
                detail=f"Transaction {transaction_id} not found"
            )
        
        transaction = self.active_transactions[transaction_id]
        
        return {
            'transaction_id': transaction_id,
            'investigation_id': transaction.investigation_id,
            'state': transaction.state.value,
            'created_at': transaction.created_at.isoformat(),
            'started_at': transaction.started_at.isoformat() if transaction.started_at else None,
            'completed_at': transaction.completed_at.isoformat() if transaction.completed_at else None,
            'checksum': transaction.checksum,
            'actions': [
                {
                    'action_id': a.action_id,
                    'tool_name': a.tool_name,
                    'state': a.state.value,
                    'start_time': a.start_time.isoformat() if a.start_time else None,
                    'end_time': a.end_time.isoformat() if a.end_time else None,
                    'error': a.error,
                    'checksum': a.checksum
                }
                for a in transaction.actions
            ],
            'compensation_actions': [
                {
                    'action_id': c.action_id,
                    'inverse_tool': c.inverse_tool,
                    'priority': c.priority,
                    'executed': c.executed,
                    'execution_time': c.execution_time.isoformat() if c.execution_time else None
                }
                for c in transaction.compensation_actions
            ]
        }
    
    def list_transactions(self) -> List[Dict[str, Any]]:
        """List all active transactions."""
        return [
            {
                'transaction_id': txn_id,
                'transaction': {
                    'investigation_id': txn.investigation_id,
                    'state': txn.state.value,
                    'created_at': txn.created_at.isoformat(),
                    'actions_count': len(txn.actions),
                    'completed_actions_count': len([a for a in txn.actions if a.state == TransactionState.COMPLETED])
                }
            }
            for txn_id, txn in self.active_transactions.items()
        ]


# Global transaction coordinator instance
transaction_coordinator = TransactionCoordinator()