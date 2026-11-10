"""Event store for AquaTrace AI Maritime Investigation Agent.

This module provides immutable event logging with checksum verification,
replay capabilities, and audit trail for all investigation activities.
"""

import hashlib
import json
from datetime import datetime
from enum import StrEnum
from typing import Any

from fastapi import HTTPException
from pydantic import BaseModel, Field


class EventStoreType(StrEnum):
    """Supported event store backends."""
    POSTGRESQL = "postgresql"
    REDIS = "redis"
    MEMORY = "memory"


class EventRecord(BaseModel):
    """Immutable record of an event in the event store."""
    
    event_id: str = Field(min_length=8, max_length=200)
    event_type: str
    investigation_id: str
    timestamp: datetime
    data: dict[str, Any]
    checksum: str
    version: int = 1
    sequence_number: int
    metadata: dict[str, Any] | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    @classmethod
    def create(cls, event_id: str, event_type: str, investigation_id: str, 
               data: dict[str, Any], sequence_number: int, metadata: dict[str, Any] | None = None) -> 'EventRecord':
        """Create a new event record with checksum."""
        # Create base event data
        event_data = {
            'event_id': event_id,
            'event_type': event_type,
            'investigation_id': investigation_id,
            'timestamp': data.get('timestamp', datetime.utcnow()).isoformat(),
            'data': data,
            'version': 1,
            'sequence_number': sequence_number,
            'metadata': metadata or {}
        }
        
        # Generate checksum for integrity verification
        checksum_data = json.dumps(event_data, sort_keys=True).encode()
        checksum = hashlib.sha256(checksum_data).hexdigest()
        
        # Update event data with checksum
        event_data['checksum'] = checksum
        
        return cls(**event_data)


class EventStore:
    """Immutable event store with replay capabilities."""
    
    def __init__(self, store_type: EventStoreType = EventStoreType.POSTGRESQL):
        self.store_type = store_type
        self.events: dict[str, list[EventRecord]] = {}  # investigation_id -> events
        self.sequence_numbers: dict[str, int] = {}  # investigation_id -> next sequence
        self.checksum_index: dict[str, dict[str, str]] = {}  # event_id -> checksum
        self.committed_events: set[str] = set()  # event_ids that have been committed
        
    def _ensure_investigation_exists(self, investigation_id: str):
        """Ensure investigation has an event store initialized."""
        if investigation_id not in self.events:
            self.events[investigation_id] = []
            self.sequence_numbers[investigation_id] = 0
            self.checksum_index[investigation_id] = {}
    
    async def append_event(
        self, 
        investigation_id: str, 
        event_type: str, 
        data: dict[str, Any],
        metadata: dict[str, Any] | None = None
    ) -> EventRecord:
        """Append an event to the store with checksum verification."""
        self._ensure_investigation_exists(investigation_id)
        
        # Generate event ID
        event_id = f"{investigation_id}_{len(self.events[investigation_id])}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        # Get next sequence number
        sequence_number = self.sequence_numbers[investigation_id]
        
        # Create event record
        event = EventRecord.create(
            event_id=event_id,
            event_type=event_type,
            investigation_id=investigation_id,
            data=data,
            sequence_number=sequence_number,
            metadata=metadata
        )
        
        # Validate checksum
        if not self._verify_checksum(event):
            raise HTTPException(
                status_code=400,
                detail=f"Checksum verification failed for event {event_id}"
            )
        
        # Store the event
        self.events[investigation_id].append(event)
        self.checksum_index[investigation_id][event_id] = event.checksum
        self.sequence_numbers[investigation_id] += 1
        
        # Store to persistence backend
        await self._persist_event(event)
        
        return event
    
    async def get_events(
        self, 
        investigation_id: str, 
        from_sequence: int = 0,
        to_sequence: int | None = None,
        event_types: list[str] | None = None
    ) -> list[EventRecord]:
        """Retrieve events for an investigation with filtering."""
        self._ensure_investigation_exists(investigation_id)
        
        events = self.events[investigation_id]
        
        # Filter by sequence number
        filtered_events = events[from_sequence:to_sequence] if to_sequence else events[from_sequence:]
        
        # Filter by event type
        if event_types:
            filtered_events = [
                e for e in filtered_events if e.event_type in event_types
            ]
        
        return filtered_events
    
    async def get_event(self, investigation_id: str, event_id: str) -> EventRecord | None:
        """Retrieve a specific event by ID."""
        self._ensure_investigation_exists(investigation_id)
        
        if event_id not in self.checksum_index[investigation_id]:
            return None
        
        # Find event by ID
        for event in self.events[investigation_id]:
            if event.event_id == event_id:
                return event
        
        return None
    
    async def replay_events(
        self, 
        investigation_id: str, 
        from_event: str = None,
        from_sequence: int = 0,
        to_event: str = None,
        to_sequence: int = None
    ) -> list[EventRecord]:
        """Replay events with filtering for specific ranges."""
        self._ensure_investigation_exists(investigation_id)
        
        events = self.events[investigation_id]
        
        # Filter by from_event (event_id or sequence)
        if from_event and from_event.startswith(f"{investigation_id}_"):
            # Find event by ID
            start_index = next(
                (i for i, e in enumerate(events) if e.event_id == from_event),
                len(events)
            )
            events = events[start_index + 1:]
        elif from_sequence is not None:
            events = events[from_sequence:]
        
        # Filter by to_event (event_id or sequence)
        if to_event and to_event.startswith(f"{investigation_id}_"):
            # Find event by ID
            end_index = next(
                (i for i, e in enumerate(events) if e.event_id == to_event),
                len(events)
            )
            events = events[:end_index]
        elif to_sequence is not None:
            events = events[:to_sequence]
        
        return events
    
    def _verify_checksum(self, event: EventRecord) -> bool:
        """Verify event checksum integrity."""
        # Recreate the base event data (without checksum)
        event_data = {
            'event_id': event.event_id,
            'event_type': event.event_type,
            'investigation_id': event.investigation_id,
            'timestamp': event.timestamp.isoformat(),
            'data': event.data,
            'version': event.version,
            'sequence_number': event.sequence_number,
            'metadata': event.metadata or {}
        }
        
        # Calculate checksum
        checksum_data = json.dumps(event_data, sort_keys=True).encode()
        calculated_checksum = hashlib.sha256(checksum_data).hexdigest()
        
        return calculated_checksum == event.checksum
    
    async def _persist_event(self, event: EventRecord):
        """Finalize the in-memory append used by the memory backend."""
        self.committed_events.add(event.event_id)
    
    async def commit_event_batch(self, events: list[EventRecord]):
        """Commit multiple events atomically."""
        try:
            # Validate all events before committing
            for event in events:
                if not self._verify_checksum(event):
                    raise HTTPException(
                        status_code=400,
                        detail=f"Checksum verification failed for event {event.event_id}"
                    )
            
            # Persist all events
            for event in events:
                await self._persist_event(event)
            
            # Mark as committed
            for event in events:
                self.committed_events.add(event.event_id)
                
        except Exception:
            # If any event fails, rollback all
            for event in events:
                if event.event_id in self.committed_events:
                    await self._undo_persistence(event)
            
            raise
    
    async def _undo_persistence(self, event: EventRecord):
        """Remove an event from the memory backend during rollback."""
        self.committed_events.discard(event.event_id)
        events = self.events.get(event.investigation_id, [])
        self.events[event.investigation_id] = [
            stored for stored in events if stored.event_id != event.event_id
        ]
        self.checksum_index.get(event.investigation_id, {}).pop(event.event_id, None)
    
    async def get_event_history(self, investigation_id: str) -> dict[str, Any]:
        """Get complete event history for an investigation."""
        self._ensure_investigation_exists(investigation_id)
        
        events = self.events[investigation_id]
        
        return {
            'investigation_id': investigation_id,
            'total_events': len(events),
            'sequence_range': {
                'start': events[0].sequence_number if events else 0,
                'end': events[-1].sequence_number if events else 0
            },
            'event_types': list(set(e.event_type for e in events)),
            'checksum_valid': all(self._verify_checksum(e) for e in events),
            'committed_events_count': len(set(e.event_id for e in events) & self.committed_events),
            'latest_event_time': max(e.timestamp for e in events).isoformat() if events else None,
            'oldest_event_time': min(e.timestamp for e in events).isoformat() if events else None
        }
    
    def is_event_committed(self, event_id: str) -> bool:
        """Check if an event has been committed."""
        return event_id in self.committed_events
    
    def get_checksum_index(self, investigation_id: str) -> dict[str, str]:
        """Get checksum index for an investigation."""
        self._ensure_investigation_exists(investigation_id)
        return self.checksum_index[investigation_id].copy()


class EventStoreManager:
    """Manages multiple event stores with routing and consistency."""
    
    def __init__(self):
        self.event_stores: dict[str, EventStore] = {}
        self.default_store_type = EventStoreType.POSTGRESQL
        
    def get_store(self, investigation_id: str, store_type: EventStoreType | None = None) -> EventStore:
        """Get or create an event store for an investigation."""
        store_type = store_type or self.default_store_type
        store_key = f"{investigation_id}_{store_type.value}"
        
        if store_key not in self.event_stores:
            self.event_stores[store_key] = EventStore(store_type)
        
        return self.event_stores[store_key]
    
    async def append_event_with_consistency(
        self,
        investigation_id: str,
        event_type: str,
        data: dict[str, Any],
        metadata: dict[str, Any] | None = None
    ) -> EventRecord:
        """Append event with consistency guarantees across stores."""
        # Use primary store for the event
        primary_store = self.get_store(investigation_id)
        
        # Create event with checksum
        event = await primary_store.append_event(investigation_id, event_type, data, metadata)
        
        # Optionally replicate to secondary stores
        await self._replicate_to_secondary_stores(investigation_id, event)
        
        return event
    
    async def _replicate_to_secondary_stores(self, investigation_id: str, event: EventRecord):
        """Replicate event to secondary stores for high availability."""
        for key, store in self.event_stores.items():
            if key != investigation_id and store is not self.get_store(investigation_id):
                store._ensure_investigation_exists(investigation_id)
                if not any(item.event_id == event.event_id for item in store.events[investigation_id]):
                    store.events[investigation_id].append(event)
                    store.checksum_index[investigation_id][event.event_id] = event.checksum
    
    async def get_consistent_events(
        self,
        investigation_id: str,
        from_sequence: int = 0,
        to_sequence: int | None = None,
        event_types: list[str] | None = None
    ) -> list[EventRecord]:
        """Get events with consistency guarantees."""
        primary_store = self.get_store(investigation_id)
        return await primary_store.get_events(from_sequence, to_sequence, event_types)
    
    async def verify_event_integrity(self, investigation_id: str, event_id: str) -> bool:
        """Verify event integrity across all stores."""
        # Get from all stores and verify consistency
        for store_key, store in self.event_stores.items():
            if store_key.startswith(investigation_id):
                event = await store.get_event(investigation_id, event_id)
                if event and not store._verify_checksum(event):
                    return False
        
        return True


# Global event store manager
event_store_manager = EventStoreManager()