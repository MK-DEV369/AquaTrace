"""FastAPI router for AquaTrace AI Maritime Investigation Agent.

This module provides API endpoints for the agentic AI system that can
plan investigations, select tools, query data, and produce evidence-based
explanations for maritime vessel behavior analysis.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from services.agent.investigation_agent import (
    InvestigationRequest,
    MaritimeInvestigationAgent,
    InvestigationSession
)
from services.kafka_integration import (
    KafkaInvestigationConsumer,
    KafkaInvestigationProducer,
    investigation_agent,
    setup_kafka_integration,
    stop_kafka_integration,
    get_kafka_status
)

# Create router for agent endpoints
router = APIRouter(prefix="/api/v1/agent", tags=["maritime-agent"])

# Initialize the investigation agent
investigation_agent = MaritimeInvestigationAgent()

# WebSocket connection manager for real-time investigation updates
class AgentWebSocketManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.investigation_sessions: Dict[str, Dict[str, Any]] = {}
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
    
    async def send_to_all(self, message: Dict[str, Any]):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                pass
    
    async def send_to_session(self, session_id: str, message: Dict[str, Any]):
        if session_id in self.investigation_sessions:
            for connection in self.active_connections:
                # Send update to all connected clients
                await connection.send_json({
                    **message,
                    "session_id": session_id,
                    "timestamp": datetime.utcnow().isoformat()
                })

# Initialize WebSocket manager
websocket_manager = AgentWebSocketManager()

# API endpoint models
class InvestigationStartResponse(BaseModel):
    session_id: str
    status: str
    question: str
    start_time: datetime
    message: str

class InvestigationStatusResponse(BaseModel):
    session_id: str
    status: str
    question: str
    start_time: datetime
    tool_calls_count: int
    evidence_count: int
    errors_count: int
    reasoning_steps_count: int

class InvestigationExecuteResponse(BaseModel):
    session_id: str
    status: str
    investigation_summary: str
    evidence_summary: Dict[str, Any]
    recommendations: List[str]
    uncertainties: List[str]
    confidence_score: float
    completion_timestamp: datetime

class InvestigationCancelResponse(BaseModel):
    session_id: str
    status: str
    message: str

class AgentToolInfo(BaseModel):
    name: str
    description: str
    parameters: List[Dict[str, str]]
    returns: str
    safety_level: str
    category: str

class ToolListResponse(BaseModel):
    tools: List[AgentToolInfo]

# API endpoint implementations

@router.post("/investigate/start", response_model=InvestigationStartResponse)
async def start_investigation(request: InvestigationRequest) -> Dict[str, Any]:
    """Start a new maritime investigation.
    
    Args:
        request: Investigation request containing question and parameters
        
    Returns:
        Investigation session information
    """
    try:
        session_id = await investigation_agent.start_investigation(request)
        
        # Update WebSocket manager with session info
        websocket_manager.investigation_sessions[session_id] = {
            "status": "in_progress",
            "question": request.question,
            "start_time": datetime.utcnow(),
            "user_id": request.user_id,
            "reasoning_depth": request.reasoning_depth,
            "max_tools": request.max_tools
        }
        
        # Send update to all connected WebSocket clients
        await websocket_manager.send_to_session(session_id, {
            "type": "investigation_started",
            "status": "in_progress",
            "message": f"Investigation started: {request.question}",
            "session_id": session_id,
            "start_time": datetime.utcnow().isoformat()
        })
        
        return InvestigationStartResponse(
            session_id=session_id,
            status="in_progress",
            question=request.question,
            start_time=datetime.utcnow(),
            message="Investigation session started successfully"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start investigation: {str(e)}")

@router.post("/investigate/{session_id}/execute", response_model=InvestigationExecuteResponse)
async def execute_investigation(session_id: str, request: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Execute an investigation session.
    
    Args:
        session_id: Investigation session identifier
        request: Optional additional parameters (for future extensibility)
        
    Returns:
        Investigation results and final report
    """
    try:
        # Update WebSocket manager
        if session_id in websocket_manager.investigation_sessions:
            websocket_manager.investigation_sessions[session_id]["status"] = "executing"
        
        # Send execution start update
        await websocket_manager.send_to_session(session_id, {
            "type": "investigation_executing",
            "status": "executing",
            "message": "Investigation tools are being executed...",
            "session_id": session_id
        })
        
        # Execute the investigation
        result = await investigation_agent.execute_investigation(session_id)
        
        # Update WebSocket manager
        if session_id in websocket_manager.investigation_sessions:
            websocket_manager.investigation_sessions[session_id]["status"] = "completed"
        
        # Send completion update
        await websocket_manager.send_to_session(session_id, {
            "type": "investigation_completed",
            "status": "completed",
            "message": "Investigation completed successfully",
            "session_id": session_id,
            "confidence_score": result.get("confidence_score", 0),
            "recommendations_count": len(result.get("recommendations", [])),
            "completion_timestamp": result.get("completion_timestamp")
        })
        
        return InvestigationExecuteResponse(**result)
        
    except Exception as e:
        # Update WebSocket manager
        if session_id in websocket_manager.investigation_sessions:
            websocket_manager.investigation_sessions[session_id]["status"] = "error"
        
        # Send error update
        await websocket_manager.send_to_session(session_id, {
            "type": "investigation_error",
            "status": "error",
            "message": f"Investigation failed: {str(e)}",
            "session_id": session_id,
            "error": str(e)
        })
        
        raise HTTPException(status_code=500, detail=f"Failed to execute investigation: {str(e)}")

@router.get("/investigate/{session_id}/status", response_model=InvestigationStatusResponse)
async def get_investigation_status(session_id: str) -> Dict[str, Any]:
    """Get the status of an investigation session.
    
    Args:
        session_id: Investigation session identifier
        
    Returns:
        Investigation session status information
    """
    try:
        status_info = await investigation_agent.get_session_status(session_id)
        return InvestigationStatusResponse(**status_info)
        
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Session {session_id} not found: {str(e)}")

@router.post("/investigate/{session_id}/cancel", response_model=InvestigationCancelResponse)
async def cancel_investigation(session_id: str) -> Dict[str, Any]:
    """Cancel an investigation session.
    
    Args:
        session_id: Investigation session identifier
        
    Returns:
        Cancellation confirmation
    """
    try:
        result = await investigation_agent.cancel_investigation(session_id)
        
        # Update WebSocket manager
        if session_id in websocket_manager.investigation_sessions:
            del websocket_manager.investigation_sessions[session_id]
        
        # Send cancellation update
        await websocket_manager.send_to_session(session_id, {
            "type": "investigation_cancelled",
            "status": "cancelled",
            "message": "Investigation cancelled by user",
            "session_id": session_id,
            "cancellation_timestamp": datetime.utcnow().isoformat()
        })
        
        return InvestigationCancelResponse(**result)
        
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Failed to cancel session {session_id}: {str(e)}")

@router.get("/investigate/{session_id}/report", response_model=Dict[str, Any])
async def get_investigation_report(session_id: str) -> Dict[str, Any]:
    """Get detailed investigation report.
    
    Args:
        session_id: Investigation session identifier
        
    Returns:
        Complete investigation report
    """
    try:
        report = await investigation_agent.get_session_report(session_id)
        return report
        
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Report for session {session_id} not found: {str(e)}")

@router.get("/tools", response_model=ToolListResponse)
async def list_approved_tools() -> Dict[str, Any]:
    """List all approved investigation tools with metadata.
    
    Returns:
        List of approved tools and their metadata
    """
    try:
        from services.agent.tools import list_tools
        
        tools_info = await list_tools()
        
        return ToolListResponse(tools=tools_info)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list tools: {str(e)}")

@router.post("/execute-tool")
async def execute_single_tool(request: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a single tool (for testing and direct tool execution).
    
    Args:
        request: Tool execution request containing tool name and parameters
        
    Returns:
        Tool execution result
    """
    try:
        from services.agent.tools import execute_tool
        
        tool_name = request.get("tool_name")
        parameters = request.get("parameters", {})
        
        if not tool_name:
            raise HTTPException(status_code=400, detail="tool_name is required")
        
        result = await execute_tool(tool_name, parameters)
        
        return {
            "tool_name": tool_name,
            "parameters": parameters,
            "result": result,
            "success": True,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Tool execution failed: {str(e)}")

@router.websocket("/investigate")
async def agent_websocket_endpoint(websocket: WebSocket, session_id: Optional[str] = None):
    """WebSocket endpoint for real-time investigation updates.
    
    Args:
        websocket: WebSocket connection
        session_id: Optional investigation session ID
    """
    await websocket_manager.connect(websocket)
    
    try:
        # Send connection confirmation
        await websocket.send_json({
            "type": "connection_established",
            "timestamp": datetime.utcnow().isoformat(),
            "session_id": session_id,
            "message": "Connected to AquaTrace AI Maritime Agent"
        })
        
        while True:
            # Receive messages from client
            data = await websocket.receive_json()
            
            # Handle different message types
            if data.get("type") == "start_investigation":
                question = data.get("question", "")
                if not question:
                    await websocket.send_json({
                        "type": "error",
                        "message": "Question is required to start investigation"
                    })
                    continue
                
                try:
                    # Create investigation request
                    from services.agent.investigation_agent import InvestigationRequest
                    request = InvestigationRequest(question=question)
                    
                    # Start investigation
                    new_session_id = await investigation_agent.start_investigation(request)
                    
                    # Send success response
                    await websocket.send_json({
                        "type": "investigation_started",
                        "session_id": new_session_id,
                        "status": "in_progress",
                        "message": f"Investigation started for: {question}",
                        "timestamp": datetime.utcnow().isoformat()
                    })
                    
                except Exception as e:
                    await websocket.send_json({
                        "type": "error",
                        "message": f"Failed to start investigation: {str(e)}"
                    })
            
            elif data.get("type") == "get_session_status":
                session_id = data.get("session_id")
                if not session_id:
                    await websocket.send_json({
                        "type": "error",
                        "message": "Session ID is required to get status"
                    })
                    continue
                
                try:
                    status = await investigation_agent.get_session_status(session_id)
                    await websocket.send_json({
                        "type": "session_status",
                        "data": status,
                        "timestamp": datetime.utcnow().isoformat()
                    })
                except Exception as e:
                    await websocket.send_json({
                        "type": "error",
                        "message": f"Failed to get session status: {str(e)}"
                    })
            
            elif data.get("type") == "cancel_investigation":
                session_id = data.get("session_id")
                if not session_id:
                    await websocket.send_json({
                        "type": "error",
                        "message": "Session ID is required to cancel investigation"
                    })
                    continue
                
                try:
                    result = await investigation_agent.cancel_investigation(session_id)
                    await websocket.send_json({
                        "type": "investigation_cancelled",
                        "data": result,
                        "timestamp": datetime.utcnow().isoformat()
                    })
                except Exception as e:
                    await websocket.send_json({
                        "type": "error",
                        "message": f"Failed to cancel investigation: {str(e)}"
                    })
            
            else:
                await websocket.send_json({
                    "type": "error",
                    "message": f"Unknown message type: {data.get('type')}"
                })
                
    except WebSocketDisconnect:
        websocket_manager.disconnect(websocket)
        if session_id:
            # Clean up session
            if session_id in investigation_agent.active_sessions:
                await investigation_agent.cancel_investigation(session_id)
    except Exception as e:
        websocket_manager.disconnect(websocket)
        # Clean up any associated sessions
        for session_id, session_data in list(websocket_manager.investigation_sessions.items()):
            if session_data.get("websocket_id") == websocket.client_state:
                del websocket_manager.investigation_sessions[session_id]
                await investigation_agent.cancel_investigation(session_id)

@router.get("/kafka/status")
async def get_kafka_integration_status() -> Dict[str, Any]:
    """Get Kafka integration status.
    
    Returns:
        Kafka integration status information
    """
    try:
        status = await get_kafka_status()
        return {
            "status": "active",
            "timestamp": datetime.utcnow().isoformat(),
            "kafka": status,
            "agent": {
                "active_sessions": len(investigation_agent.active_sessions),
                "tools_available": len(investigation_agent.tool_metadata)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get Kafka status: {str(e)}")


@router.post("/kafka/start")
async def start_kafka_integration() -> Dict[str, Any]:
    """Start Kafka integration for agentic AI.
    
    Returns:
        Kafka integration startup result
    """
    try:
        task = await setup_kafka_integration()
        return {
            "status": "started",
            "timestamp": datetime.utcnow().isoformat(),
            "consumer_task": task,
            "message": "Kafka integration started successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start Kafka integration: {str(e)}")


@router.post("/kafka/stop")
async def stop_kafka_integration_endpoint() -> Dict[str, Any]:
    """Stop Kafka integration.
    
    Returns:
        Kafka integration shutdown result
    """
    try:
        await stop_kafka_integration()
        return {
            "status": "stopped",
            "timestamp": datetime.utcnow().isoformat(),n            "message": "Kafka integration stopped successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to stop Kafka integration: {str(e)}")


@router.get("/kafka/consumer-sessions")
async def get_kafka_consumer_sessions() -> Dict[str, Any]:
    """Get Kafka consumer investigation sessions.
    
    Returns:
        List of active Kafka consumer sessions
    """
    try:
        from services.kafka_integration import kafka_consumer
        
        sessions = await kafka_consumer.get_active_sessions()
        return {
            "sessions": sessions,
            "count": len(sessions),
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get Kafka sessions: {str(e)}")


@router.post("/kafka/publish-investigation")
async def publish_investigation_result(request: Dict[str, Any]) -> Dict[str, Any]:
    """Publish investigation result to Kafka.
    
    Args:
        request: Investigation result to publish
        
    Returns:
        Publication result
    """
    try:
        from services.kafka_integration import kafka_producer
        
        session_id = request.get("session_id")
        investigation_result = request.get("result")
        
        if not session_id:
            raise HTTPException(status_code=400, detail="session_id is required")
        
        await kafka_producer.publish_investigation_result(session_id, investigation_result)
        
        return {
            "status": "published",
            "session_id": session_id,
            "timestamp": datetime.utcnow().isoformat(),
            "message": "Investigation result published successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to publish investigation result: {str(e)}")


@router.post("/kafka/publish-alert")
async def publish_investigation_alert(request: Dict[str, Any]) -> Dict[str, Any]:
    """Publish investigation alert to Kafka.
    
    Args:
        request: Investigation alert to publish
        
    Returns:
        Publication result
    """
    try:
        from services.kafka_integration import kafka_producer
        
        alert = request.get("alert")
        if not alert:
            raise HTTPException(status_code=400, detail="alert is required")
        
        await kafka_producer.publish_investigation_alert(alert)
        
        return {
            "status": "published",
            "timestamp": datetime.utcnow().isoformat(),
            "message": "Investigation alert published successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to publish investigation alert: {str(e)}")
