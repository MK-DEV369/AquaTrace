"""Approved investigation tools for AquaTrace AI Maritime Agent.

These tools provide safe, read-only access to vessel data and analytics
for the AI investigation agent. All functions validate inputs and return
structured evidence for agent reasoning.
"""

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import HTTPException
from pydantic import BaseModel, Field, field_validator

from ship_analytics.schemas import AisEvent
from ship_analytics.anomalies import AnomalyDetector

# Import existing database connection utilities
from services.api.main import connection

# Validation models for tool inputs
class VesselQuery(BaseModel):
    mmsi: int = Field(ge=100_000_000, le=999_999_999)

class TimeRange(BaseModel):
    start_time: datetime
    end_time: datetime
    
    @field_validator('end_time')
    @classmethod
    def validate_time_range(cls, v, info):
        start = info.data.get('start_time')
        if v <= start:
            raise ValueError('end_time must be after start_time')
        return v

class SpatialSearch(BaseModel):
    mmsi: int = Field(ge=100_000_000, le=999_999_999)
    radius_km: float = Field(ge=0.1, le=100.0)
    as_of: datetime

# Investigation tool implementations
async def get_vessel_snapshot(mmsi: int) -> Dict[str, Any]:
    """Retrieve the latest stored position, speed, course and timestamp.
    
    Args:
        mmsi: Vessel identifier (9 digits)
        
    Returns:
        Dictionary containing latest vessel state with metadata
        
    Raises:
        HTTPException: If vessel not found or database error
    """
    # Validate input
    query = VesselQuery(mmsi=mmsi)
    
    try:
        async with await connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                """
                SELECT event_time, ST_Y(geom) AS latitude, ST_X(geom) AS longitude,
                       sog_knots, cog_degrees, vessel_type, heading_degrees, 
                       nav_status, source, event_id
                FROM vessel_latest 
                WHERE mmsi = %s 
                ORDER BY event_time DESC 
                LIMIT 1
                """,
                (query.mmsi,)
            )
            result = await cursor.fetchone()
            
            if not result:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Vessel {query.mmsi} not found in database"
                )
                
            return {
                "mmsi": query.mmsi,
                "event_time": result["event_time"],
                "latitude": result["latitude"],
                "longitude": result["longitude"],
                "sog_knots": result["sog_knots"],
                "cog_degrees": result["cog_degrees"],
                "heading_degrees": result["heading_degrees"],
                "nav_status": result["nav_status"],
                "vessel_type": result["vessel_type"],
                "source": result["source"],
                "event_id": result["event_id"],
                "query_timestamp": datetime.utcnow().isoformat(),
                "data_source": "vessel_latest"
            }
            
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(
            status_code=500,
            detail=f"Database error retrieving vessel snapshot: {str(e)}"
        )
async def get_recent_anomalies(mmsi: int, time_range: timedelta = timedelta(hours=24)) -> List[Dict[str, Any]]:
    """Retrieve recorded anomalies and their rule-based reasons.
    
    Args:
        mmsi: Vessel identifier
        time_range: Time window for anomalies (default: 24 hours)
        
    Returns:
        List of anomaly records with detailed evidence
        
    Raises:
        HTTPException: If database error occurs
    """
    query = VesselQuery(mmsi=mmsi)
    
    try:
        async with await connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                """
                SELECT id, event_time, anomaly_type, severity, explanation,
                       ST_Y(geom) AS latitude, ST_X(geom) AS longitude,
                       evidence_json, source
                FROM anomaly
                WHERE mmsi = %s 
                AND event_time >= %s
                ORDER BY event_time DESC
                """,
                (query.mmsi, datetime.utcnow() - time_range)
            )
            
            anomalies = await cursor.fetchall()
            
            return [{
                "anomaly_id": anomaly["id"],
                "mmsi": query.mmsi,
                "event_time": anomaly["event_time"],
                "anomaly_type": anomaly["anomaly_type"],
                "severity": anomaly["severity"],
                "explanation": anomaly["explanation"],
                "latitude": anomaly["latitude"],
                "longitude": anomaly["longitude"],
                "evidence_json": anomaly["evidence_json"],
                "source": anomaly["source"],
                "query_timestamp": datetime.utcnow().isoformat(),
                "time_range_hours": time_range.total_seconds() / 3600
            } for anomaly in anomalies]
            
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(
            status_code=500,
            detail=f"Database error retrieving anomalies: {str(e)}"
        )
async def get_vessel_track(mmsi: int, start: datetime, end: datetime) -> List[Dict[str, Any]]:
    """Retrieve ordered historical positions for a specific vessel.
    
    Args:
        mmsi: Vessel identifier
        start: Start time for track data
        end: End time for track data
        
    Returns:
        List of vessel track points in chronological order
        
    Raises:
        HTTPException: If validation fails or database error
    """
    # Validate inputs using Pydantic
    vessel_query = VesselQuery(mmsi=mmsi)
    time_range_query = TimeRange(start_time=start, end_time=end)
    
    try:
        async with await connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                """
                SELECT event_time, ST_X(geom) AS longitude, ST_Y(geom) AS latitude,
                       sog_knots, cog_degrees, vessel_type, source
                FROM vessel_track_point
                WHERE mmsi = %s 
                AND event_time >= %s
                AND event_time <= %s
                ORDER BY event_time ASC
                """,
                (vessel_query.mmsi, time_range_query.start_time, time_range_query.end_time)
            )
            
            track_points = await cursor.fetchall()
            
            return [{
                "mmsi": vessel_query.mmsi,
                "event_time": point["event_time"],
                "latitude": point["latitude"],
                "longitude": point["longitude"],
                "sog_knots": point["sog_knots"],
                "cog_degrees": point["cog_degrees"],
                "vessel_type": point["vessel_type"],
                "source": point["source"],
                "query_timestamp": datetime.utcnow().isoformat(),
                "time_range": {
                    "start": time_range_query.start_time.isoformat(),
                    "end": time_range_query.end_time.isoformat(),
                    "duration_hours": (time_range_query.end_time - time_range_query.start_time).total_seconds() / 3600
                }
            } for point in track_points]
            
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(
            status_code=500,
            detail=f"Database error retrieving vessel track: {str(e)}"
        )
async def compare_vessel_baseline(mmsi: int, time_range: timedelta = timedelta(days=30)) -> Dict[str, Any]:
    """Compare observations with a baseline calculated from adequate historical data.
    
    Args:
        mmsi: Vessel identifier
        time_range: Historical period for baseline calculation (default: 30 days)
        
    Returns:
        Statistical comparison including mean, std, percentiles, and anomaly flags
        
    Raises:
        HTTPException: If database error or insufficient data
    """
    query = VesselQuery(mmsi=mmsi)
    
    try:
        async with await connection() as conn, conn.cursor() as cursor:
            # Get vessel events for baseline calculation
            await cursor.execute(
                """
                SELECT sog_knots, cog_degrees, event_time, vessel_type
                FROM ais_event_clean
                WHERE mmsi = %s 
                AND event_time >= %s
                ORDER BY event_time ASC
                """,
                (query.mmsi, datetime.utcnow() - time_range)
            )
            
            events = await cursor.fetchall()
            
            if not events:
                raise HTTPException(
                    status_code=404,
                    detail=f"Insufficient historical data for vessel {query.mmsi}"
                )
            
            # Calculate statistics
            speeds = [e["sog_knots"] or 0 for e in events]
            courses = [e["cog_degrees"] or 0 for e in events if e["cog_degrees"] is not None]
            
            speed_stats = {
                "count": len(speeds),
                "mean": sum(speeds) / len(speeds) if speeds else 0,
                "std": (sum((x - sum(speeds)/len(speeds))**2 for x in speeds) / len(speeds))**0.5 if len(speeds) > 1 else 0,
                "min": min(speeds) if speeds else 0,
                "max": max(speeds) if speeds else 0,
                "median": sorted(speeds)[len(speeds)//2] if speeds else 0,
                "p95": sorted(speeds)[int(len(speeds)*0.95)] if len(speeds) >= 20 else sorted(speeds)[-1]
            }
            
            course_stats = {
                "count": len(courses),
                "mean": sum(courses) / len(courses) if courses else 0,
                "std": (sum((x - sum(courses)/len(courses))**2 for x in courses) / len(courses))**0.5 if len(courses) > 1 else 0,
            }
            
            # Determine if current behavior is unusual using historical patterns
            detector = AnomalyDetector()
            
            # Get recent events for comparison
            recent_cutoff = datetime.utcnow() - timedelta(hours=1)
            await cursor.execute(
                """
                SELECT sog_knots, cog_degrees, event_time
                FROM ais_event_clean
                WHERE mmsi = %s 
                AND event_time >= %s
                ORDER BY event_time DESC
                LIMIT 10
                """,
                (query.mmsi, recent_cutoff)
            )
            
            recent_events = await cursor.fetchall()
            
            # Calculate deviation scores
            if recent_events:
                recent_speeds = [e["sog_knots"] or 0 for e in recent_events]
                recent_courses = [e["cog_degrees"] or 0 for e in recent_events if e["cog_degrees"] is not None]
                
                speed_deviation = abs(recent_speeds[0] - speed_stats["mean"]) / (speed_stats["std"] or 1)
                course_deviation = abs(recent_courses[0] - course_stats["mean"]) / (course_stats["std"] or 1) if recent_courses else 0
                
                is_speed_anomaly = speed_deviation > 2.0  # More than 2 standard deviations
                is_course_anomaly = course_deviation > 1.5  # More than 1.5 standard deviations
            else:
                speed_deviation = 0
                course_deviation = 0
                is_speed_anomaly = False
                is_course_anomaly = False
            
            return {
                "mmsi": query.mmsi,
                "baseline_period": {
                    "start": (datetime.utcnow() - time_range).isoformat(),
                    "end": datetime.utcnow().isoformat(),
                    "duration_days": time_range.total_seconds() / 86400
                },
                "speed_statistics": speed_stats,
                "course_statistics": course_stats,
                "recent_behavior_deviation": {
                    "speed_deviation_std": speed_deviation,
                    "course_deviation_std": course_deviation,
                    "is_speed_anomaly": is_speed_anomaly,
                    "is_course_anomaly": is_course_anomaly,
                    "recent_speed": recent_events[0]["sog_knots"] if recent_events else None,
                    "recent_course": recent_events[0]["cog_degrees"] if recent_events else None,
                    "recent_time": recent_events[0]["event_time"].isoformat() if recent_events else None
                },
                "query_timestamp": datetime.utcnow().isoformat(),
                "data_source": "ais_event_clean"
            }
            
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(
            status_code=500,
            detail=f"Database error calculating baseline comparison: {str(e)}"
        )
async def find_nearby_vessels(mmsi: int, radius_km: float = 10.0, as_of: datetime = None) -> List[Dict[str, Any]]:
    """Use PostGIS to find nearby vessels at a relevant time.
    
    Args:
        mmsi: Reference vessel identifier
        radius_km: Search radius in kilometers (default: 10km)
        as_of: Reference time for search (default: latest available)
        
    Returns:
        List of nearby vessels with spatial relationships
        
    Raises:
        HTTPException: If validation fails or database error
    """
    # Use latest available data if as_of not provided
    if as_of is None:
        as_of = datetime.utcnow() - timedelta(hours=1)
    
    # Validate input
    search_query = SpatialSearch(mmsi=mmsi, radius_km=radius_km, as_of=as_of)
    
    try:
        async with await connection() as conn, conn.cursor() as cursor:
            # First get the reference vessel position
            await cursor.execute(
                """
                SELECT ST_X(geom) AS longitude, ST_Y(geom) AS latitude, event_time
                FROM vessel_latest
                WHERE mmsi = %s
                ORDER BY event_time DESC
                LIMIT 1
                """,
                (search_query.mmsi,)
            )
            
            reference = await cursor.fetchone()
            if not reference:
                raise HTTPException(
                    status_code=404,
                    detail=f"Reference vessel {search_query.mmsi} not found"
                )
            
            # Find nearby vessels using PostGIS
            await cursor.execute(
                """
                SELECT v.mmsi, ST_X(v.geom) AS longitude, ST_Y(v.geom) AS latitude,
                       v.event_time, ST_Distance(v.geom, ST_Point(%s, %s), true) AS distance_km,
                       v.sog_knots, v.cog_degrees, v.vessel_type
                FROM vessel_latest v
                WHERE v.mmsi != %s 
                AND ST_DWithin(v.geom, ST_Point(%s, %s), %s)
                AND v.event_time <= %s
                ORDER BY distance_km ASC
                LIMIT 50
                """,
                (
                    reference["longitude"], reference["latitude"],
                    search_query.mmsi,
                    reference["longitude"], reference["latitude"],
                    search_query.radius_km * 1000,  # Convert km to meters for PostGIS
                    search_query.as_of
                )
            )
            
            nearby_vessels = await cursor.fetchall()
            
            return [{
                "mmsi": vessel["mmsi"],
                "latitude": vessel["latitude"],
                "longitude": vessel["longitude"],
                "event_time": vessel["event_time"],
                "distance_km": vessel["distance_km"],
                "sog_knots": vessel["sog_knots"],
                "cog_degrees": vessel["cog_degrees"],
                "vessel_type": vessel["vessel_type"],
                "query_timestamp": datetime.utcnow().isoformat(),
                "reference_vessel": search_query.mmsi,
                "search_radius_km": search_query.radius_km,
                "as_of": search_query.as_of.isoformat()
            } for vessel in nearby_vessels]
            
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(
            status_code=500,
            detail=f"Database error finding nearby vessels: {str(e)}"
        )
async def draft_incident_report(mmsi: int, anomaly_id: Optional[int] = None) -> Dict[str, Any]:
    """Assemble a structured draft from retrieved evidence.
    
    Args:
        mmsi: Vessel identifier for the incident
        anomaly_id: Specific anomaly record to include (optional)
        
    Returns:
        Structured incident report draft with timeline and recommendations
        
    Raises:
        HTTPException: If data retrieval fails
    """
    query = VesselQuery(mmsi=mmsi)
    
    try:
        async with await connection() as conn, conn.cursor() as cursor:
            # Get incident overview
            await cursor.execute(
                """
                SELECT a.id, a.mmsi, a.event_time, a.anomaly_type, a.severity, 
                       a.explanation, ST_Y(a.geom) AS latitude, ST_X(a.geom) AS longitude,
                       a.evidence_json, v.event_time as vessel_event_time,
                       v.sog_knots, v.cog_degrees, v.vessel_type
                FROM anomaly a
                JOIN vessel_latest v ON a.mmsi = v.mmsi AND a.event_time <= v.event_time
                WHERE a.mmsi = %s 
                AND a.event_time >= %s
                AND (%s IS NULL OR a.id = %s)
                ORDER BY a.event_time DESC
                LIMIT 10
                """,
                (
                    query.mmsi,
                    datetime.utcnow() - timedelta(days=7),  # Last 7 days
                    anomaly_id,
                    anomaly_id
                )
            )
            
            anomalies = await cursor.fetchall()
            
            if not anomalies:
                raise HTTPException(
                    status_code=404,
                    detail=f"No incidents found for vessel {query.mmsi}"
                )
            
            # Build structured report
            incident = anomalies[0] if anomaly_id else anomalies[0]
            
            # Get vessel recent behavior for context
            await cursor.execute(
                """
                SELECT event_time, ST_Y(geom) AS latitude, ST_X(geom) AS longitude,
                       sog_knots, cog_degrees, vessel_type
                FROM vessel_latest
                WHERE mmsi = %s
                ORDER BY event_time DESC
                LIMIT 5
                """,
                (query.mmsi,)
            )
            
            recent_positions = await cursor.fetchall()
            
            # Get nearby vessels for context
            await cursor.execute(
                """
                SELECT v.mmsi, ST_Y(v.geom) AS latitude, ST_X(v.geom) AS longitude,
                       v.event_time, v.sog_knots, v.vessel_type,
                       ST_Distance(v.geom, (SELECT geom FROM vessel_latest WHERE mmsi = %s), true) AS distance_km
                FROM vessel_latest v
                WHERE v.mmsi != %s 
                AND v.event_time >= %s
                AND ST_Distance(v.geom, (SELECT geom FROM vessel_latest WHERE mmsi = %s), true) < 10000
                ORDER BY distance_km ASC
                LIMIT 5
                """,
                (query.mmsi, query.mmsi, datetime.utcnow() - timedelta(hours=1), query.mmsi)
            )
            
            nearby_vessels = await cursor.fetchall()
            
            # Build comprehensive incident report
            report = {
                "incident_report": {
                    "vessel_mmsi": query.mmsi,
                    "report_generated": datetime.utcnow().isoformat(),
                    "time_period": {
                        "start": (datetime.utcnow() - timedelta(days=7)).isoformat(),
                        "end": datetime.utcnow().isoformat()
                    }
                },
                "primary_incident": {
                    "anomaly_id": incident["id"],
                    "event_time": incident["event_time"],
                    "anomaly_type": incident["anomaly_type"],
                    "severity": incident["severity"],
                    "explanation": incident["explanation"],
                    "location": {
                        "latitude": incident["latitude"],
                        "longitude": incident["longitude"]
                    },
                    "vessel_context": {
                        "current_speed_knots": incident["sog_knots"],
                        "current_course_degrees": incident["cog_degrees"],
                        "vessel_type": incident["vessel_type"],
                        "last_position_time": incident["vessel_event_time"].isoformat()
                    },
                    "evidence": incident["evidence_json"],
                    "recent_positions": [
                        {
                            "event_time": pos["event_time"].isoformat(),
                            "latitude": pos["latitude"],
                            "longitude": pos["longitude"],
                            "sog_knots": pos["sog_knots"],
                            "cog_degrees": pos["cog_degrees"]
                        } for pos in recent_positions
                    ]
                },
                "traffic_context": {
                    "nearby_vessels": [
                        {
                            "mmsi": v["mmsi"],
                            "latitude": v["latitude"],
                            "longitude": v["longitude"],
                            "distance_km": v["distance_km"],
                            "speed_knots": v["sog_knots"],
                            "vessel_type": v["vessel_type"],
                            "last_seen": v["event_time"].isoformat()
                        } for v in nearby_vessels
                    ],
                    "total_nearby_count": len(nearby_vessels)
                },
                "timeline": [
                    {
                        "timestamp": a["event_time"].isoformat(),
                        "anomaly_type": a["anomaly_type"],
                        "severity": a["severity"],
                        "explanation": a["explanation"],
                        "location": {"lat": a["latitude"], "lon": a["longitude"]},
                        "is_primary": a["id"] == incident["id"]
                    } for a in anomalies
                ],
                "recommended_actions": [
                    "Review vessel's recent track for unusual patterns",
                    "Check communication logs for any distress signals",
                    "Monitor nearby vessel traffic for potential interactions",
                    "Generate formal incident report if required by regulations"
                ],
                "uncertainties": [
                    "Missing historical data for complete baseline comparison",
                    "Potential data latency in real-time position updates",
                    "Limited visibility of underwater or obscured vessels"
                ],
                "query_timestamp": datetime.utcnow().isoformat()
            }
            
            return report
            
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(
            status_code=500,
            detail=f"Error drafting incident report: {str(e)}"
        )
# Public tool registry - maps tool names to functions
TOOL_REGISTRY = {
    "get_vessel_snapshot": get_vessel_snapshot,
    "get_recent_anomalies": get_recent_anomalies,
    "get_vessel_track": get_vessel_track,
    "compare_vessel_baseline": compare_vessel_baseline,
    "find_nearby_vessels": find_nearby_vessels,
    "draft_incident_report": draft_incident_report,
}

# Tool metadata for API documentation
TOOL_METADATA = {
    "get_vessel_snapshot": {
        "description": "Retrieve the latest stored position, speed, course and timestamp",
        "parameters": [{"name": "mmsi", "type": "integer", "required": True}],
        "returns": "vessel snapshot data",
        "safety_level": "safe",
        "category": "vessel_tracking"
    },
    "get_recent_anomalies": {
        "description": "Retrieve recorded anomalies and their rule-based reasons",
        "parameters": [
            {"name": "mmsi", "type": "integer", "required": True},
            {"name": "time_range", "type": "timedelta", "required": False}
        ],
        "returns": "list of anomaly records",
        "safety_level": "safe",
        "category": "anomaly_detection"
    },
    "get_vessel_track": {
        "description": "Retrieve ordered historical positions for a specific vessel",
        "parameters": [
            {"name": "mmsi", "type": "integer", "required": True},
            {"name": "start", "type": "datetime", "required": True},
            {"name": "end", "type": "datetime", "required": True}
        ],
        "returns": "vessel track points in chronological order",
        "safety_level": "safe",
        "category": "vessel_tracking"
    },
    "compare_vessel_baseline": {
        "description": "Compare observations with a baseline calculated from historical data",
        "parameters": [
            {"name": "mmsi", "type": "integer", "required": True},
            {"name": "time_range", "type": "timedelta", "required": False}
        ],
        "returns": "statistical baseline comparison",
        "safety_level": "safe",
        "category": "analytics"
    },
    "find_nearby_vessels": {
        "description": "Use PostGIS to find nearby vessels at a relevant time",
        "parameters": [
            {"name": "mmsi", "type": "integer", "required": True},
            {"name": "radius_km", "type": "float", "required": False},
            {"name": "as_of", "type": "datetime", "required": False}
        ],
        "returns": "list of nearby vessels with spatial relationships",
        "safety_level": "safe",
        "category": "spatial"
    },
    "draft_incident_report": {
        "description": "Assemble a structured draft from retrieved evidence",
        "parameters": [
            {"name": "mmsi", "type": "integer", "required": True},
            {"name": "anomaly_id", "type": "integer", "required": False}
        ],
        "returns": "structured incident report draft",
        "safety_level": "safe",
        "category": "reporting"
    }
}

async def get_tool(name: str):
    """Get a tool function by name.
    
    Args:
        name: Tool function name
        
    Returns:
        Tool function or None if not found
        
    Raises:
        HTTPException: If tool not found
    """
    if name not in TOOL_REGISTRY:
        raise HTTPException(
            status_code=404,
            detail=f"Tool '{name}' not found. Available tools: {list(TOOL_REGISTRY.keys())}"
        )
    return TOOL_REGISTRY[name]

async def list_tools() -> List[Dict[str, Any]]:
    """List all available investigation tools with metadata.
    
    Returns:
        List of tool metadata dictionaries
    """
    return [{
        "name": name,
        "metadata": metadata
    } for name, metadata in TOOL_METADATA.items()]

async def execute_tool(tool_name: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a tool with given parameters.
    
    Args:
        tool_name: Name of tool to execute
        parameters: Tool parameters
        
    Returns:
        Tool execution result
        
    Raises:
        HTTPException: If tool execution fails
    """
    tool = await get_tool(tool_name)
    
    # Convert parameter values based on expected types
    converted_params = {}
    
    if tool_name == "get_vessel_snapshot":
        converted_params["mmsi"] = int(parameters.get("mmsi"))
    
    elif tool_name == "get_recent_anomalies":
        converted_params["mmsi"] = int(parameters.get("mmsi"))
        if "time_range" in parameters:
            # Parse time_range string if provided
            from dateutil.parser import parse
            time_range_hours = float(parameters.get("time_range", "24"))
            converted_params["time_range"] = timedelta(hours=time_range_hours)
    
    elif tool_name == "get_vessel_track":
        converted_params["mmsi"] = int(parameters.get("mmsi"))
        converted_params["start"] = datetime.fromisoformat(parameters.get("start"))
        converted_params["end"] = datetime.fromisoformat(parameters.get("end"))
    
    elif tool_name == "compare_vessel_baseline":
        converted_params["mmsi"] = int(parameters.get("mmsi"))
        if "time_range" in parameters:
            time_range_days = float(parameters.get("time_range", "30"))
            converted_params["time_range"] = timedelta(days=time_range_days)
    
    elif tool_name == "find_nearby_vessels":
        converted_params["mmsi"] = int(parameters.get("mmsi"))
        converted_params["radius_km"] = float(parameters.get("radius_km", "10.0"))
        if "as_of" in parameters:
            converted_params["as_of"] = datetime.fromisoformat(parameters.get("as_of"))
    
    elif tool_name == "draft_incident_report":
        converted_params["mmsi"] = int(parameters.get("mmsi"))
        if "anomaly_id" in parameters and parameters.get("anomaly_id"):
            converted_params["anomaly_id"] = int(parameters.get("anomaly_id"))
    
    # Execute the tool
    try:
        return await tool(**converted_params)
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(
            status_code=500,
            detail=f"Tool execution failed: {str(e)}"
        )