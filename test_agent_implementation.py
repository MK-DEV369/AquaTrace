"""Test script for AquaTrace AI Maritime Investigation Agent.

This script tests the core functionality of the agentic AI system
including tool execution, investigation workflows, and API endpoints.
"""

import asyncio
import json
from datetime import datetime, timedelta

# Test imports and basic functionality
async def test_imports():
    """Test that all imports work correctly."""
    print("Testing imports...")
    
    try:
        from services.agent.tools import (
            get_vessel_snapshot,
            get_recent_anomalies,
            get_vessel_track,
            compare_vessel_baseline,
            find_nearby_vessels,
            draft_incident_report,
            execute_tool,
            get_tool,
            list_tools,
            TOOL_METADATA
        )
        print("✓ All imports successful")
        
        # Test tool registry
        print(f"✓ Available tools: {list(TOOL_METADATA.keys())}")
        
        return True
    except Exception as e:
        print(f"✗ Import failed: {e}")
        return False

async def test_tool_metadata():
    """Test tool metadata structure."""
    print("\nTesting tool metadata...")
    
    try:
        from services.agent.tools import TOOL_METADATA
        
        # Check that all expected tools are present
        expected_tools = [
            "get_vessel_snapshot",
            "get_recent_anomalies", 
            "get_vessel_track",
            "compare_vessel_baseline",
            "find_nearby_vessels",
            "draft_incident_report"
        ]
        
        for tool_name in expected_tools:
            if tool_name not in TOOL_METADATA:
                print(f"✗ Missing tool: {tool_name}")
                return False
            
            # Check tool metadata structure
            metadata = TOOL_METADATA[tool_name]
            required_fields = ["description", "parameters", "returns", "safety_level", "category"]
            
            for field in required_fields:
                if field not in metadata:
                    print(f"✗ Tool {tool_name} missing field: {field}")
                    return False
        
        print("✓ All tools have valid metadata structure")
        return True
        
    except Exception as e:
        print(f"✗ Tool metadata test failed: {e}")
        return False

async def test_investigation_agent():
    """Test basic investigation agent functionality."""
    print("\nTesting investigation agent...")
    
    try:
        from services.agent.investigation_agent import (
            MaritimeInvestigationAgent,
            InvestigationRequest
        )
        
        # Create agent instance
        agent = MaritimeInvestigationAgent()
        print("✓ Investigation agent created successfully")
        
        # Test investigation request model
        test_request = InvestigationRequest(
            question="Investigate vessel 533002002 for recent anomalies",
            context={"user_id": "test_user"},
            max_tools=3
        )
        print("✓ Investigation request model created successfully")
        
        # Test agent session creation
        session_id = await agent.start_investigation(test_request)
        print(f"✓ Investigation session created: {session_id}")
        
        return True
        
    except Exception as e:
        print(f"✗ Investigation agent test failed: {e}")
        return False

async def test_fastapi_router():
    """Test FastAPI router structure."""
    print("\nTesting FastAPI router...")
    
    try:
        from services.api.agent_router import router
        
        # Check that router has expected routes
        routes = []
        for route in router.routes:
            if hasattr(route, 'path'):
                routes.append(route.path)
        
        expected_routes = [
            "/api/v1/agent/investigate/start",
            "/api/v1/agent/investigate/{session_id}/execute",
            "/api/v1/agent/investigate/{session_id}/status",
            "/api/v1/agent/investigate/{session_id}/cancel",
            "/api/v1/agent/investigate/{session_id}/report",
            "/api/v1/agent/tools",
            "/api/v1/agent/execute-tool",
            "/api/v1/agent/health"
        ]
        
        for expected_route in expected_routes:
            if not any(expected_route in route for route in routes):
                print(f"✗ Missing route: {expected_route}")
                return False
        
        print("✓ All expected API routes are present")
        return True
        
    except Exception as e:
        print(f"✗ FastAPI router test failed: {e}")
        return False

async def test_integration():
    """Test integration between components."""
    print("\nTesting integration...")
    
    try:
        # Test tool execution function
        from services.agent.tools import execute_tool
        
        # Mock test - just test that the function exists and can be called
        # (we won't actually execute tools that require database)
        print("✓ Tool execution function available")
        
        # Test list tools function
        from services.agent.tools import list_tools
        tools_list = await list_tools()
        print(f"✓ Tool list function works, found {len(tools_list)} tools")
        
        return True
        
    except Exception as e:
        print(f"✗ Integration test failed: {e}")
        return False

async def test_code_quality():
    """Test code quality and structure."""
    print("\nTesting code quality...")
    
    try:
        # Check that investigation agent has expected methods
        from services.agent.investigation_agent import MaritimeInvestigationAgent
        
        expected_methods = [
            "start_investigation",
            "execute_investigation",
            "get_session_status",
            "cancel_investigation",
            "get_session_report"
        ]
        
        agent = MaritimeInvestigationAgent()
        
        for method_name in expected_methods:
            if not hasattr(agent, method_name):
                print(f"✗ Missing method: {method_name}")
                return False
            
            # Check that method is callable
            if not callable(getattr(agent, method_name)):
                print(f"✗ Method not callable: {method_name}")
                return False
        
        print("✓ All expected methods are present and callable")
        return True
        
    except Exception as e:
        print(f"✗ Code quality test failed: {e}")
        return False

async def main():
    """Run all tests."""
    print("=" * 60)
    print("AQUATRACE AI MARITIME INVESTIGATION AGENT - TEST SUITE")
    print("=" * 60)
    
    tests = [
        test_imports,
        test_tool_metadata,
        test_investigation_agent,
        test_fastapi_router,
        test_integration,
        test_code_quality
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if await test():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"✗ Test {test.__name__} crashed: {e}")
            failed += 1
    
    print("\n" + "=" * 60)
    print("TEST RESULTS")
    print("=" * 60)
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Total: {passed + failed}")
    
    if failed == 0:
        print("\n🎉 All tests passed! The implementation is ready.")
    else:
        print(f"\n⚠️  {failed} test(s) failed. Please review the implementation.")
    
    return failed == 0

if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)
