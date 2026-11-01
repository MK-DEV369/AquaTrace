"""Kafka integration for AquaTrace AI Maritime Investigation Agent.

This module integrates the agentic AI system with the existing Kafka infrastructure,
providing real-time data consumption and investigation capabilities.
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from confluent_kafka import Consumer, KafkaError
from fastapi import HTTPException

from ship_analytics.config import get_settings
from ship_analytics.schemas import AisEvent

from services.agent.tools import execute_tool, TOOL_METADATA


class KafkaInvestigationConsumer:
    """Kafka consumer for real-time vessel investigation data."""
    
    def __init__(self, topic: str = "ais.raw", group_id: str = "agent-investigation"):
        self.topic = topic
        self.group_id = group_id
        self.consumer = None
        self.settings = get_settings()
        self.active_investigations: Dict[str, Dict[str, Any]] = {}
        
    async def start_consumption(self):
        """Start consuming messages from Kafka."""
        config = {
            'bootstrap.servers': self.settings.kafka_brokers,
            'group.id': self.group_id,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000,
            'session.timeout.ms': 300000,
            'isolation.level': 'read_committed'
        }
        
        self.consumer = Consumer(config)
        self.consumer.subscribe([self.topic])
        
        print(f"🔄 Started consuming from topic '{self.topic}' with group ID '{self.group_id}'")
        
        while True:
            try:
                message = self.consumer.poll(timeout=1.0)
                if message is None:
                    continue
                
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"❌ Kafka error: {message.error()}")
                        continue
                
                await self.process_message(message)
                
            except Exception as e:
                print(f"❌ Error in Kafka consumption: {e}")
                await asyncio.sleep(1)
    
    async def process_message(self, message):
        """Process a Kafka message and trigger relevant investigations."""
        try:
            # Decode the message
            raw_value = message.value().decode('utf-8')
            event_data = json.loads(raw_value)
            
            # Convert to AisEvent
            event = AisEvent(**event_data)
            
            # Check if this event triggers any investigations
            investigation_triggers = await self.check_investigation_triggers(event)
            
            if investigation_triggers:
                print(f"🔍 Investigation triggered for vessel {event.mmsi}")
                await self.trigger_investigation(event, investigation_triggers)
            
            # Store event for analysis
            await self.store_event_for_analysis(event)
            
        except Exception as e:
            print(f"❌ Error processing message: {e}")
    
    async def check_investigation_triggers(self, event: AisEvent) -> List[str]:
        """Check if an event should trigger an investigation."""
        triggers = []
        
        # Example trigger conditions
        if event.sog_knots is not None and event.sog_knots > 50:
            triggers.append("high_speed_event")
        
        if event.cog_degrees is not None and event.cog_degrees > 180:
            triggers.append("sharp_course_change")
        
        # Check against active investigations
        for session_id, session_info in self.active_investigations.items():
            if session_info.get('vessel_mmsi') == event.mmsi:
                triggers.append(f"investigation_session:{session_id}")
        
        return triggers
    
    async def trigger_investigation(self, event: AisEvent, triggers: List[str]):
        """Trigger an investigation based on event triggers."""
        for trigger in triggers:
            if trigger.startswith("investigation_session:"):
                # This event is part of an ongoing investigation
                session_id = trigger.split(":")[2]
                session_info = self.active_investigations.get(session_id)
                if session_info:
                    await self.update_investigation_session(session_id, event)
            
            elif trigger == "high_speed_event":
                # Trigger investigation for high speed event
                await self.auto_investigate_event(event, "high_speed_anomaly")
            
            elif trigger == "sharp_course_change":
                # Trigger investigation for sharp course change
                await self.auto_investigate_event(event, "sharp_course_change")
    
    async def update_investigation_session(self, session_id: str, event: AisEvent):
        """Update an investigation session with new event data."""
        session_info = self.active_investigations.get(session_id)
        if session_info:
            session_info['events'].append(event)
            session_info['last_updated'] = datetime.utcnow()
            
            # Check if session has enough data for analysis
            if len(session_info['events']) >= session_info.get('min_events', 5):
                await self.analyze_investigation_session(session_id)
    
    async def auto_investigate_event(self, event: AisEvent, anomaly_type: str):
        """Automatically investigate an event that triggered a rule."""
        try:
            # Create investigation request for this event
            from services.agent.investigation_agent import InvestigationRequest
            
            question = f"Investigate why vessel {event.mmsi} triggered a {anomaly_type}. " \
                      f"Event occurred at {event.event_time}, position {event.latitude}, {event.longitude}, " \
                      f"speed {event.sog_knots} knots, course {event.cog_degrees} degrees."
            
            request = InvestigationRequest(
                question=question,
                context={
                    "user_id": "auto_detector",
                    "anomaly_type": anomaly_type,
                    "event_source": "kafka_consumer"
                },
                max_tools=5,
                reasoning_depth="standard"
            )
            
            # Start investigation session
            session_id = await investigation_agent.start_investigation(request)
            
            # Store investigation info
            self.active_investigations[session_id] = {
                "vessel_mmsi": event.mmsi,
                "anomaly_type": anomaly_type,
                "start_time": datetime.utcnow(),
                "events": [event],
                "min_events": 3,
                "auto_triggered": True
            }
            
            # Execute investigation
            result = await investigation_agent.execute_investigation(session_id)
            
            print(f"🔍 Auto-investigation completed for vessel {event.mmsi}, session {session_id}")
            
        except Exception as e:
            print(f"❌ Error in auto-investigation: {e}")
    
    async def store_event_for_analysis(self, event: AisEvent):
        """Store event for analysis and future investigations."""
        # Implementation would store events in database or cache
        # This is a placeholder for actual storage logic
        pass
    
    async def analyze_investigation_session(self, session_id: str):
        """Analyze a completed investigation session."""
        session_info = self.active_investigations.get(session_id)
        if not session_info:
            return
        
        try:
            # Get vessel snapshot for the vessel being investigated
            vessel_snapshot = await execute_tool(
                "get_vessel_snapshot", 
                {"mmsi": session_info['vessel_mmsi']}
            )
            
            # Get recent anomalies
            recent_anomalies = await execute_tool(
                "get_recent_anomalies",
                {"mmsi": session_info['vessel_mmsi'], "time_range": "24 hours"}
            )
            
            # Analyze investigation results
            analysis = {
                "session_id": session_id,
                "vessel_mmsi": session_info['vessel_mmsi'],
                "anomaly_type": session_info['anomaly_type'],
                "event_count": len(session_info['events']),
                "vessel_snapshot": vessel_snapshot,
                "relevant_anomalies": recent_anomalies,
                "analysis_timestamp": datetime.utcnow().isoformat()
            }
            
            print(f"📊 Investigation analysis completed for session {session_id}")
            
        except Exception as e:
            print(f"❌ Error analyzing investigation session: {e}")
    
    async def get_active_sessions(self) -> Dict[str, Any]:
        """Get all active investigation sessions."""
        return self.active_investigations.copy()
    
    async def stop(self):
        """Stop the Kafka consumer."""
        if self.consumer:
            self.consumer.close()
            print("🛑 Kafka consumer stopped")


class KafkaInvestigationProducer:
    """Kafka producer for investigation results and alerts."""
    
    def __init__(self):
        self.producer = None
        self.settings = get_settings()
    
    async def start(self):
        """Initialize the Kafka producer."""
        config = {
            'bootstrap.servers': self.settings.kafka_brokers,
            'client.id': 'agent-producer'
        }
        
        self.producer = Producer(config)
        print("🔄 Kafka producer initialized")
    
    async def publish_investigation_result(self, session_id: str, investigation_result: Dict[str, Any]):
        """Publish investigation results to Kafka."""
        try:
            message = {
                "type": "investigation_result",
                "session_id": session_id,
                "timestamp": datetime.utcnow().isoformat(),
                "result": investigation_result
            }
            
            self.producer.produce(
                "agent.investigations",
                key=session_id,
                value=json.dumps(message)
            )
            self.producer.poll(0)
            print(f"📤 Investigation result published for session {session_id}")
            
        except Exception as e:
            print(f"❌ Error publishing investigation result: {e}")
    
    async def publish_investigation_alert(self, alert: Dict[str, Any]):
        """Publish investigation alert to Kafka."""
        try:
            message = {
                "type": "investigation_alert",
                "timestamp": datetime.utcnow().isoformat(),
                "alert": alert
            }
            
            self.producer.produce(
                "agent.alerts",
                key=alert.get('session_id', 'unknown'),
                value=json.dumps(message)
            )
            self.producer.poll(0)
            print(f"📤 Investigation alert published: {alert.get('message', 'Unknown alert')}")
            
        except Exception as e:
            print(f"❌ Error publishing investigation alert: {e}")
    
    async def stop(self):
        """Stop the Kafka producer."""
        if self.producer:
            self.producer.flush()
            print("🛑 Kafka producer stopped")


# Global instances
kafka_consumer = KafkaInvestigationConsumer()
kafka_producer = KafkaInvestigationProducer()

# Global investigation agent instance (from investigation_agent.py)
from services.agent.investigation_agent import MaritimeInvestigationAgent

investigation_agent = MaritimeInvestigationAgent()


async def setup_kafka_integration():
    """Setup and initialize Kafka integration for agentic AI."""
    print("🔧 Setting up Kafka integration for AquaTrace AI...")
    
    # Start the consumer
    consumer_task = asyncio.create_task(kafka_consumer.start_consumption())
    
    # Start the producer
    await kafka_producer.start()
    
    print("✅ Kafka integration setup completed")
    print(f"   • Consumer topic: {kafka_consumer.topic}")
    print(f"   • Consumer group: {kafka_consumer.group_id}")
    print(f"   • Producer topic: agent.investigations")
    print(f"   • Alert topic: agent.alerts")
    
    return consumer_task


async def stop_kafka_integration():
    """Stop Kafka integration."""
    print("🛑 Stopping Kafka integration...")
    
    await kafka_producer.stop()
    await kafka_consumer.stop()
    
    print("✅ Kafka integration stopped")


async def get_kafka_status() -> Dict[str, Any]:
    """Get Kafka integration status."""
    return {
        "consumer_active": kafka_consumer.consumer is not None,
        "producer_active": kafka_producer.producer is not None,
        "active_investigations": len(kafka_consumer.active_investigations),
        "topic": kafka_consumer.topic,
        "group_id": kafka_consumer.group_id
    }
