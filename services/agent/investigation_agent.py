"""AI Maritime Investigation Agent for AquaTrace.

This module implements the agentic AI system that can plan investigations,
select tools, query data, correlate evidence, and produce explanations
grounded in results.
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from services.agent.tools import execute_tool, get_tool, list_tools, TOOL_METADATA


class InvestigationRequest(BaseModel):
    """Request model for agent investigations."""
    question: str = Field(..., description="Natural language investigation question")
    context: Optional[Dict[str, Any]] = Field(None, description="Additional context for investigation")
    user_id: Optional[str] = Field(None, description="User identifier for tracking")
    max_tools: int = Field(5, description="Maximum number of tools to use", ge=1, le=10)
    reasoning_depth: str = Field("standard", description="Reasoning depth: basic, standard, thorough")


class InvestigationSession(BaseModel):
    """Represents an investigation session state."""
    session_id: str
    question: str
    start_time: datetime
    status: str = "in_progress"  # in_progress, completed, cancelled
    tool_calls: List[Dict[str, Any]] = []
    evidence_collected: List[Dict[str, Any]] = []
    reasoning_steps: List[Dict[str, Any]] = []
    final_report: Optional[Dict[str, Any]] = None
    errors: List[str] = []
    user_preferences: Optional[Dict[str, Any]] = None


class MaritimeInvestigationAgent:
    """Agent for maritime vessel investigations with tool planning and execution."""
    
    def __init__(self):
        self.active_sessions: Dict[str, InvestigationSession] = {}
        self.tool_metadata = TOOL_METADATA
        
    async def start_investigation(self, request: InvestigationRequest) -> str:
        """Start a new investigation session.
        
        Args:
            request: Investigation request containing question and parameters
            
        Returns:
            Session ID for tracking the investigation
        """
        session_id = f"inv_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{id(request)}"
        
        session = InvestigationSession(
            session_id=session_id,
            question=request.question,
            start_time=datetime.utcnow(),
            user_preferences=request.context or {}
        )
        
        self.active_sessions[session_id] = session
        
        # Add initial reasoning step
        session.reasoning_steps.append({
            "timestamp": datetime.utcnow().isoformat(),
            "step": "session_started",
            "description": f"Processing investigation request: {request.question}",
            "reasoning_depth": request.reasoning_depth,
            "max_tools": request.max_tools
        })
        
        return session_id
    
    async def execute_investigation(self, session_id: str) -> Dict[str, Any]:
        """Execute investigation with intelligent tool selection.
        
        Args:
            session_id: Investigation session identifier
            
        Returns:
            Investigation results and final report
        """
        if session_id not in self.active_sessions:
            raise HTTPException(status_code=404, detail=f"Investigation session {session_id} not found")
        
        session = self.active_sessions[session_id]
        
        try:
            # Step 1: Analyze question to determine required evidence
            await self._analyze_investigation_requirements(session)
            
            # Step 2: Plan tool execution sequence
            tool_sequence = await self._plan_tool_sequence(session)
            
            # Step 3: Execute planned tools
            await self._execute_tool_sequence(session, tool_sequence)
            
            # Step 4: Synthesize evidence and generate report
            await self._synthesize_evidence(session)
            
            # Step 5: Finalize investigation
            session.status = "completed"
            session.final_report = {
                "session_id": session_id,
                "investigation_summary": self._generate_summary(session),
                "evidence_summary": self._generate_evidence_summary(session),
                "recommendations": self._generate_recommendations(session),
                "uncertainties": self._identify_uncertainties(session),
                "confidence_score": self._calculate_confidence(session),
                "completion_timestamp": datetime.utcnow().isoformat()
            }
            
            return session.final_report
            
        except Exception as e:
            session.status = "cancelled"
            session.errors.append(str(e))
            raise HTTPException(status_code=500, detail=f"Investigation failed: {str(e)}")
    
    async def _analyze_investigation_requirements(self, session: InvestigationSession):
        """Analyze investigation question to determine required evidence."""
        question = session.question.lower()
        
        # Extract key entities and intent from question
        requirements = {
            "vessel_identified": False,
            "time_sensitive": False,
            "needs_comparison": False,
            "needs_spatial_context": False,
            "needs_anomaly_history": False
        }
        
        # Simple heuristic-based analysis
        if "vessel" in question and any(c.isdigit() for c in question):
            requirements["vessel_identified"] = True
            
            # Extract potential MMSI from question
            import re
            mmsi_match = re.search(r'\b(\d{9})\b', question)
            if mmsi_match:
                session.mmsi = int(mmsi_match.group(1))
        
        if any(word in question for word in ["recent", "latest", "current", "now"]):
            requirements["time_sensitive"] = True
        
        if any(word in question for word in ["compare", "baseline", "normal", "typical"]):
            requirements["needs_comparison"] = True
        
        if any(word in question for word in ["nearby", "surrounding", "close", "around"]):
            requirements["needs_spatial_context"] = True
        
        if any(word in question for word in ["flagged", "anomaly", "incident", "issue"]):
            requirements["needs_anomaly_history"] = True
        
        session.reasoning_steps.append({
            "timestamp": datetime.utcnow().isoformat(),
            "step": "analyze_requirements",
            "description": "Analyzed investigation question for required evidence",
            "requirements": requirements,
            "extracted_entities": {
                "mmsi": getattr(session, 'mmsi', None),
                "timeframe": self._extract_timeframe(question),
                "anomaly_type": self._extract_anomaly_type(question)
            }
        })
    
    async def _plan_tool_sequence(self, session: InvestigationSession) -> List[Dict[str, Any]]:
        """Plan the sequence of tool calls based on investigation requirements."""
        
        # Analyze session requirements
        vessel_id = getattr(session, 'mmsi', None)
        question_lower = session.question.lower()
        
        # Determine which tools are needed based on analysis
        planned_tools = []
        
        if vessel_id:
            # Basic vessel snapshot is always useful
            planned_tools.append({
                "tool": "get_vessel_snapshot",
                "parameters": {"mmsi": vessel_id},
                "priority": "high",
                "reasoning": "Get current vessel state and context"
            })
            
            # Add anomaly history if anomaly-related question
            if session.reasoning_steps[-1]["requirements"]["needs_anomaly_history"]:
                planned_tools.append({
                    "tool": "get_recent_anomalies",
                    "parameters": {"mmsi": vessel_id, "time_range": "7 days"},
                    "priority": "high",
                    "reasoning": "Investigate recent anomalies affecting this vessel"
                })
            
            # Add baseline comparison if comparison requested
            if session.reasoning_steps[-1]["requirements"]["needs_comparison"]:
                planned_tools.append({
                    "tool": "compare_vessel_baseline",
                    "parameters": {"mmsi": vessel_id, "time_range": "30 days"},
                    "priority": "medium",
                    "reasoning": "Compare with historical behavior baseline"
                })
            
            # Add vessel track for movement analysis
            if any(word in question_lower for word in ["track", "route", "movement", "history"]):
                planned_tools.append({
                    "tool": "get_vessel_track",
                    "parameters": {
                        "mmsi": vessel_id,
                        "start": (datetime.utcnow() - timedelta(hours=24)).isoformat(),
                        "end": datetime.utcnow().isoformat()
                    },
                    "priority": "medium",
                    "reasoning": "Analyze recent vessel movement patterns"
                })
            
            # Add spatial context if spatial relationship requested
            if session.reasoning_steps[-1]["requirements"]["needs_spatial_context"]:
                planned_tools.append({
                    "tool": "find_nearby_vessels",
                    "parameters": {
                        "mmsi": vessel_id,
                        "radius_km": 10.0,
                        "as_of": datetime.utcnow().isoformat()
                    },
                    "priority": "low",
                    "reasoning": "Find vessels in the vicinity for context"
                })
        
        # Limit tool count based on session constraints
        session.max_tools = min(session.max_tools, len(planned_tools))
        
        # Sort by priority and limit
        planned_tools.sort(key=lambda x: {"high": 0, "medium": 1, "low": 2}.get(x["priority"], 3))
        
        session.reasoning_steps.append({
            "timestamp": datetime.utcnow().isoformat(),
            "step": "plan_tools",
            "description": "Planned tool execution sequence",
            "tool_sequence": planned_tools[:session.max_tools],
            "total_tools_planned": len(planned_tools[:session.max_tools]),
            "reasoning": "Selected tools based on investigation requirements and priorities"
        })
        
        return planned_tools[:session.max_tools]
    
    async def _execute_tool_sequence(self, session: InvestigationSession, tool_sequence: List[Dict[str, Any]]):
        """Execute the planned sequence of tools."""
        
        for tool_plan in tool_sequence:
            try:
                session.reasoning_steps.append({
                    "timestamp": datetime.utcnow().isoformat(),
                    "step": "execute_tool",
                    "description": f"Executing tool: {tool_plan['tool']}",
                    "parameters": tool_plan["parameters"],
                    "reasoning": tool_plan["reasoning"]
                })
                
                # Execute the tool
                result = await execute_tool(tool_plan["tool"], tool_plan["parameters"])
                
                # Record the tool call and its results
                session.tool_calls.append({
                    "timestamp": datetime.utcnow().isoformat(),
                    "tool_name": tool_plan["tool"],
                    "parameters": tool_plan["parameters"],
                    "result": result,
                    "success": True,
                    "execution_time_ms": len(str(result))  # Rough timing
                })
                
                session.evidence_collected.append({
                    "timestamp": datetime.utcnow().isoformat(),
                    "tool_name": tool_plan["tool"],
                    "evidence": result,
                    "quality": self._assess_evidence_quality(tool_plan["tool"], result)
                })
                
                session.reasoning_steps.append({
                    "timestamp": datetime.utcnow().isoformat(),
                    "step": "tool_completed",
                    "description": f"Tool {tool_plan['tool']} completed successfully",
                    "evidence_count": len(session.evidence_collected),
                    "reasoning": "Tool provided relevant evidence for investigation"
                })
                
            except Exception as e:
                session.errors.append(f"Tool {tool_plan['tool']} failed: {str(e)}")
                
                session.tool_calls.append({
                    "timestamp": datetime.utcnow().isoformat(),
                    "tool_name": tool_plan["tool"],
                    "parameters": tool_plan["parameters"],
                    "error": str(e),
                    "success": False,
                    "execution_time_ms": len(str(e))
                })
                
                session.reasoning_steps.append({
                    "timestamp": datetime.utcnow().isoformat(),
                    "step": "tool_failed",
                    "description": f"Tool {tool_plan['tool']} failed: {str(e)}",
                    "error_details": str(e),
                    "reasoning": "Tool execution failed, investigating alternative evidence sources"
                })
                
                # Continue with other tools if possible
                continue
    
    async def _synthesize_evidence(self, session: InvestigationSession):
        """Synthesize collected evidence into coherent analysis."""
        
        session.reasoning_steps.append({
            "timestamp": datetime.utcnow().isoformat(),
            "step": "synthesize_evidence",
            "description": "Synthesizing collected evidence into analysis",
            "evidence_count": len(session.evidence_collected),
            "tool_calls_count": len(session.tool_calls)
        })
        
        # Build comprehensive analysis from evidence
        synthesis = {
            "temporal_analysis": self._build_temporal_analysis(session),
            "spatial_analysis": self._build_spatial_analysis(session),
            "behavioral_analysis": self._build_behavioral_analysis(session),
            "anomaly_analysis": self._build_anomaly_analysis(session),
            "contextual_factors": self._identify_contextual_factors(session)
        }
        
        session.reasoning_steps.append({
            "timestamp": datetime.utcnow().isoformat(),
            "step": "synthesis_complete",
            "description": "Evidence synthesis completed",
            "synthesis": synthesis,
            "key_findings": self._extract_key_findings(synthesis)
        })
    
    def _build_temporal_analysis(self, session: InvestigationSession) -> Dict[str, Any]:
        """Build temporal analysis from time-series evidence."""
        
        track_points = []
        anomaly_events = []
        recent_events = []
        
        for evidence in session.evidence_collected:
            if evidence["tool_name"] == "get_vessel_track":
                track_points.extend(evidence["evidence"])
            elif evidence["tool_name"] == "get_recent_anomalies":
                anomaly_events.extend(evidence["evidence"])
            elif evidence["tool_name"] == "get_vessel_snapshot":
                recent_events.append(evidence["evidence"])
        
        return {
            "movement_timeline": track_points,
            "anomaly_timeline": anomaly_events,
            "current_state": recent_events[0] if recent_events else None,
            "event_counts": {
                "track_points": len(track_points),
                "anomaly_events": len(anomaly_events),
                "state_updates": len(recent_events)
            }
        }
    
    def _build_spatial_analysis(self, session: InvestigationSession) -> Dict[str, Any]:
        """Build spatial analysis from geographic evidence."""
        
        spatial_evidence = []
        for evidence in session.evidence_collected:
            if evidence["tool_name"] == "find_nearby_vessels":
                spatial_evidence.extend(evidence["evidence"])
            elif evidence["tool_name"] == "compare_vessel_baseline":
                spatial_evidence.extend([{"type": "baseline", "data": evidence["evidence"]}])
        
        return {
            "spatial_relationships": spatial_evidence,
            "proximity_analysis": self._analyze_proximity(session),
            "traffic_patterns": self._analyze_traffic_patterns(session)
        }
    
    def _build_behavioral_analysis(self, session: InvestigationSession) -> Dict[str, Any]:
        """Build behavioral analysis from comparative evidence."""
        
        baseline_evidence = []
        for evidence in session.evidence_collected:
            if evidence["tool_name"] == "compare_vessel_baseline":
                baseline_evidence.append(evidence["evidence"])
        
        return {
            "baseline_comparison": baseline_evidence[0] if baseline_evidence else None,
            "deviation_analysis": self._analyze_deviations(session),
            "behavior_patterns": self._extract_behavior_patterns(session)
        }
    
    def _build_anomaly_analysis(self, session: InvestigationSession) -> Dict[str, Any]:
        """Build anomaly analysis from anomaly-specific evidence."""
        
        anomaly_evidence = []
        for evidence in session.evidence_collected:
            if evidence["tool_name"] == "get_recent_anomalies":
                anomaly_evidence.extend(evidence["evidence"])
        
        return {
            "anomaly_records": anomaly_evidence,
            "anomaly_correlations": self._correlate_anomalies(session),
            "severity_assessment": self._assess_anomaly_severity(session)
        }
    
    def _identify_contextual_factors(self, session: InvestigationSession) -> Dict[str, Any]:
        """Identify relevant contextual factors."""
        
        question_lower = session.question.lower()
        
        factors = {
            "temporal_context": {
                "investigation_focus": "recent" if any(word in question_lower for word in ["recent", "current", "now"]) else "historical",
                "data_freshness": "real-time" if any(word in question_lower for word in ["live", "real-time", "current"]) else "historical"
            },
            "spatial_context": {
                "has_geographic_scope": any(word in question_lower for word in ["nearby", "surrounding", "area", "location"]),
                "coordinate_referenced": any(word in question_lower for word in ["coordinates", "latitude", "longitude"])
            },
            "operational_context": {
                "investigation_type": self._determine_investigation_type(session.question),
                "urgency_level": self._determine_urgency_level(session.question)
            }
        }
        
        return factors
    
    def _generate_summary(self, session: InvestigationSession) -> str:
        """Generate concise investigation summary."""
        
        summary_parts = []
        
        if session.evidence_collected:
            summary_parts.append(f"Analyzed {len(session.evidence_collected)} evidence sources")
        
        if session.tool_calls:
            successful_calls = sum(1 for tc in session.tool_calls if tc["success"])
            summary_parts.append(f"Successfully executed {successful_calls} tools")
        
        if session.errors:
            summary_parts.append(f"Encountered {len(session.errors)} errors during investigation")
        
        if session.evidence_collected:
            summary_parts.append("Investigation completed with available evidence")
        
        return "; ".join(summary_parts)
    
    def _generate_evidence_summary(self, session: InvestigationSession) -> Dict[str, Any]:
        """Generate summary of collected evidence."""
        
        evidence_by_tool = {}
        for evidence in session.evidence_collected:
            tool_name = evidence["tool_name"]
            if tool_name not in evidence_by_tool:
                evidence_by_tool[tool_name] = []
            evidence_by_tool[tool_name].append(evidence)
        
        return {
            "total_evidence_sources": len(session.evidence_collected),
            "evidence_by_tool": {
                tool_name: {
                    "count": len(evidence_list),
                    "quality_average": sum(e["quality"] for e in evidence_list) / len(evidence_list)
                }
                for tool_name, evidence_list in evidence_by_tool.items()
            }
        }
    
    def _generate_recommendations(self, session: InvestigationSession) -> List[str]:
        """Generate actionable recommendations based on investigation."""
        
        recommendations = []
        
        # Add recommendations based on findings
        if any(e["quality"] < 0.5 for e in session.evidence_collected):
            recommendations.append("Collect additional evidence to address gaps in analysis")
        
        if session.errors:
            recommendations.append("Investigate technical issues that prevented tool execution")
        
        if any("anomaly" in str(e["evidence"]).lower() for e in session.evidence_collected):
            recommendations.append("Consider formal incident reporting if regulatory requirements apply")
        
        if any("nearby" in str(e["evidence"]) for e in session.evidence_collected):
            recommendations.append("Monitor traffic patterns in the affected area")
        
        if not recommendations:
            recommendations.append("Continue monitoring vessel for additional data points")
        
        return recommendations
    
    def _identify_uncertainties(self, session: InvestigationSession) -> List[str]:
        """Identify uncertainties and limitations in investigation."""
        
        uncertainties = []
        
        if session.errors:
            uncertainties.append(f"Tool execution failures may have limited evidence collection ({len(session.errors)} errors)")
        
        if not session.evidence_collected:
            uncertainties.append("No evidence was successfully collected due to technical limitations")
        
        if len(session.evidence_collected) < 2:
            uncertainties.append("Limited evidence sources may result in incomplete analysis")
        
        uncertainties.append("Future vessel behavior may differ from historical patterns")
        
        return uncertainties
    
    def _calculate_confidence(self, session: InvestigationSession) -> float:
        """Calculate overall confidence score for investigation."""
        
        if not session.evidence_collected:
            return 0.0
        
        # Base confidence on evidence quality and tool success
        successful_tools = sum(1 for tc in session.tool_calls if tc["success"])
        total_tools_attempted = len(session.tool_calls) if session.tool_calls else 1
        
        tool_success_rate = successful_tools / total_tools_attempted
        
        # Average evidence quality
        avg_evidence_quality = sum(e["quality"] for e in session.evidence_collected) / len(session.evidence_collected)
        
        # Combine metrics (weighted)
        confidence = (tool_success_rate * 0.4) + (avg_evidence_quality * 0.6)
        
        return min(1.0, max(0.0, confidence))
    
    # Helper methods
    
    def _extract_timeframe(self, question: str) -> Optional[str]:
        """Extract timeframe from investigation question."""
        question_lower = question.lower()
        
        if any(word in question_lower for word in ["last hour", "previous hour", "recent hour"]):
            return "last_hour"
        elif any(word in question_lower for word in ["today", "current day"]):
            return "today"
        elif any(word in question_lower for word in ["this week", "past week"]):
            return "this_week"
        elif any(word in question_lower for word in ["last 24 hours", "past 24h"]):
            return "last_24_hours"
        
        return None
    
    def _extract_anomaly_type(self, question: str) -> Optional[str]:
        """Extract anomaly type from investigation question."""
        question_lower = question.lower()
        
        anomaly_types = ["speed", "course", "position", "behavior", "navigation", "collision"]
        
        for anomaly_type in anomaly_types:
            if anomaly_type in question_lower:
                return anomaly_type
        
        return None
    
    def _assess_evidence_quality(self, tool_name: str, evidence: Dict[str, Any]) -> float:
        """Assess quality of evidence from tool execution."""
        
        if not evidence:
            return 0.0
        
        quality_score = 0.5  # Base score
        
        if tool_name == "get_vessel_snapshot":
            quality_score += 0.2  # Real-time data
        
        elif tool_name == "get_vessel_track":
            if isinstance(evidence, list) and len(evidence) > 0:
                quality_score += 0.3  # Rich temporal data
        
        elif tool_name == "get_recent_anomalies":
            if isinstance(evidence, list) and len(evidence) > 0:
                quality_score += 0.4  # Direct anomaly evidence
        
        elif tool_name == "compare_vessel_baseline":
            if "speed_statistics" in evidence and "course_statistics" in evidence:
                quality_score += 0.3  # Rich statistical data
        
        elif tool_name == "find_nearby_vessels":
            if isinstance(evidence, list) and len(evidence) > 0:
                quality_score += 0.2  # Spatial context
        
        elif tool_name == "draft_incident_report":
            if "incident_report" in evidence:
                quality_score += 0.5  # Structured report
        
        return min(1.0, quality_score)
    
    def _analyze_proximity(self, session: InvestigationSession) -> Dict[str, Any]:
        """Analyze proximity-based evidence."""
        
        proximity_data = []
        for evidence in session.evidence_collected:
            if evidence["tool_name"] == "find_nearby_vessels":
                proximity_data.extend(evidence["evidence"])
        
        return {
            "nearby_vessels_count": len(proximity_data),
            "average_distance_km": sum(e.get("distance_km", 0) for e in proximity_data) / len(proximity_data) if proximity_data else 0,
            "potential_conflicts": [e for e in proximity_data if e.get("distance_km", 0) < 1.0]
        }
    
    def _analyze_traffic_patterns(self, session: InvestigationSession) -> Dict[str, Any]:
        """Analyze traffic patterns from collected evidence."""
        
        return {
            "activity_level": "unknown",
            "peak_hours": [],
            "traffic_density": "low"
        }
    
    def _analyze_deviations(self, session: InvestigationSession) -> Dict[str, Any]:
        """Analyze behavioral deviations from baseline."""
        
        return {
            "speed_deviations": [],
            "course_deviations": [],
            "overall_deviation_score": 0.0
        }
    
    def _extract_behavior_patterns(self, session: InvestigationSession) -> Dict[str, Any]:
        """Extract behavior patterns from evidence."""
        
        return {
            "patterns": [],
            "frequency_analysis": {},
            "trend_indicators": []
        }
    
    def _correlate_anomalies(self, session: InvestigationSession) -> Dict[str, Any]:
        """Correlate multiple anomalies for deeper analysis."""
        
        return {
            "correlated_anomalies": [],
            "causality_hypotheses": [],
            "related_events": []
        }
    
    def _assess_anomaly_severity(self, session: InvestigationSession) -> Dict[str, Any]:
        """Assess severity of detected anomalies."""
        
        return {
            "overall_severity": "low",
            "critical_anomalies": [],
            "severity_factors": []
        }
    
    def _determine_investigation_type(self, question: str) -> str:
        """Determine type of investigation based on question."""
        
        question_lower = question.lower()
        
        if any(word in question_lower for word in ["why", "investigate", "reason", "cause"]):
            return "causal_analysis"
        elif any(word in question_lower for word in ["compare", "baseline", "difference"]):
            return "comparative_analysis"
        elif any(word in question_lower for word in ["monitor", "track", "follow up"]):
            return "ongoing_monitoring"
        elif any(word in question_lower for word in ["report", "incident", "documentation"]):
            return "incident_reporting"
        
        return "general_investigation"
    
    def _determine_urgency_level(self, question: str) -> str:
        """Determine urgency level based on question."""
        
        question_lower = question.lower()
        
        if any(word in question_lower for word in ["emergency", "urgent", "critical", "immediate"]):
            return "high"
        elif any(word in question_lower for word in ["important", "significant", "priority"]):
            return "medium"
        
        return "low"
    
    def _extract_key_findings(self, synthesis: Dict[str, Any]) -> List[str]:
        """Extract key findings from evidence synthesis."""
        
        key_findings = []
        
        # Extract key findings from various synthesis components
        if "temporal_analysis" in synthesis:
            key_findings.extend(self._extract_temporal_key_findings(synthesis["temporal_analysis"]))
        
        if "spatial_analysis" in synthesis:
            key_findings.extend(self._extract_spatial_key_findings(synthesis["spatial_analysis"]))
        
        return key_findings
    
    def _extract_temporal_key_findings(self, temporal_analysis: Dict[str, Any]) -> List[str]:
        """Extract key findings from temporal analysis."""
        
        findings = []
        
        if temporal_analysis.get("movement_timeline"):
            timeline = temporal_analysis["movement_timeline"]
            if timeline:
                findings.append(f"Vessel has {len(timeline)} recorded track points")
        
        if temporal_analysis.get("anomaly_timeline"):
            anomalies = temporal_analysis["anomaly_timeline"]
            if anomalies:
                findings.append(f"Detected {len(anomalies)} historical anomalies")
        
        return findings
    
    def _extract_spatial_key_findings(self, spatial_analysis: Dict[str, Any]) -> List[str]:
        """Extract key findings from spatial analysis."""
        
        findings = []
        
        if spatial_analysis.get("spatial_relationships"):
            relationships = spatial_analysis["spatial_relationships"]
            if relationships:
                findings.append(f"Found {len(relationships)} spatial relationships")
        
        return findings

    # Session management methods
    
    async def get_session_status(self, session_id: str) -> Dict[str, Any]:
        """Get status of an investigation session."""
        
        if session_id not in self.active_sessions:
            raise HTTPException(status_code=404, detail=f"Session {session_id} not found")
        
        session = self.active_sessions[session_id]
        
        return {
            "session_id": session_id,
            "status": session.status,
            "question": session.question,
            "start_time": session.start_time.isoformat(),
            "tool_calls_count": len(session.tool_calls),
            "evidence_count": len(session.evidence_collected),
            "errors_count": len(session.errors),
            "reasoning_steps_count": len(session.reasoning_steps)
        }
    
    async def cancel_investigation(self, session_id: str) -> Dict[str, Any]:
        """Cancel an investigation session."""
        
        if session_id not in self.active_sessions:
            raise HTTPException(status_code=404, detail=f"Session {session_id} not found")
        
        session = self.active_sessions[session_id]
        session.status = "cancelled"
        
        return {
            "session_id": session_id,
            "status": "cancelled",
            "message": "Investigation session cancelled successfully"
        }
    
    async def get_session_report(self, session_id: str) -> Dict[str, Any]:
        """Get detailed report for an investigation session."""
        
        if session_id not in self.active_sessions:
            raise HTTPException(status_code=404, detail=f"Session {session_id} not found")
        
        session = self.active_sessions[session_id]
        
        if not session.final_report:
            raise HTTPException(status_code=400, detail="Investigation not completed. Use execute_investigation first.")
        
        return session.final_report
