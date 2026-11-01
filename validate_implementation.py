#!/usr/bin/env python3
"""
Simple test to validate the AquaTrace AI Maritime Investigation Agent implementation.
"""

import sys
import traceback
from datetime import datetime, timedelta

def test_basic_imports():
    """Test basic imports."""
    print("Testing basic imports...")
    
    try:
        from services.agent.tools import get_vessel_snapshot
        print("✓ tools module imported successfully")
        return True
    except Exception as e:
        print(f"✗ Failed to import tools: {e}")
        return False

def test_investigation_agent_import():
    """Test investigation agent import."""
    print("Testing investigation agent import...")
    
    try:
        from services.agent.investigation_agent import MaritimeInvestigationAgent, InvestigationRequest
        print("✓ investigation_agent module imported successfully")
        
        # Test creating agent instance
        agent = MaritimeInvestigationAgent()
        print("✓ MaritimeInvestigationAgent created successfully")
        
        # Test creating investigation request
        request = InvestigationRequest(
            question="Test investigation",
            context={"user_id": "test"},
            max_tools=3
        )
        print("✓ InvestigationRequest created successfully")
        
        return True
    except Exception as e:
        print(f"✗ Failed to import investigation_agent: {e}")
        traceback.print_exc()
        return False

def test_fastapi_router_import():
    """Test FastAPI router import."""
    print("Testing FastAPI router import...")
    
    try:
        from services.api.agent_router import router
        print("✓ agent_router module imported successfully")
        
        # Check router properties
        print(f"✓ Router prefix: {router.prefix}")
        print(f"✓ Router tags: {router.tags}")
        
        return True
    except Exception as e:
        print(f"✗ Failed to import agent_router: {e}")
        traceback.print_exc()
        return False

def test_tool_metadata():
    """Test tool metadata."""
    print("Testing tool metadata...")
    
    try:
        from services.agent.tools import TOOL_METADATA
        print(f"✓ Tool metadata loaded with {len(TOOL_METADATA)} tools")
        
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
            print(f"✓ Tool {tool_name} found")
        
        return True
    except Exception as e:
        print(f"✗ Failed to test tool metadata: {e}")
        traceback.print_exc()
        return False

def test_agent_methods():
    """Test agent methods."""
    print("Testing agent methods...")
    
    try:
        from services.agent.investigation_agent import MaritimeInvestigationAgent
        
        agent = MaritimeInvestigationAgent()
        
        # Test that agent has expected methods
        expected_methods = [
            "start_investigation",
            "execute_investigation", 
            "get_session_status",
            "cancel_investigation",
            "get_session_report"
        ]
        
        for method_name in expected_methods:
            if not hasattr(agent, method_name):
                print(f"✗ Missing method: {method_name}")
                return False
            print(f"✓ Method {method_name} exists")
        
        return True
    except Exception as e:
        print(f"✗ Failed to test agent methods: {e}")
        traceback.print_exc()
        return False

def test_agent_session_creation():
    """Test agent session creation."""
    print("Testing agent session creation...")
    
    try:
        from services.agent.investigation_agent import MaritimeInvestigationAgent, InvestigationRequest
        
        agent = MaritimeInvestigationAgent()
        
        # Create test request
        request = InvestigationRequest(
            question="Test vessel 123456789 investigation",
            context={"user_id": "test_user"},
            max_tools=2
        )
        
        # Start investigation
        session_id = agent.start_investigation(request)
        
        if not session_id.startswith("inv_"):
            print(f"✗ Invalid session ID: {session_id}")
            return False
        
        print(f"✓ Investigation session created: {session_id}")
        print(f"✓ Session status: {agent.active_sessions[session_id].status}")
        
        return True
    except Exception as e:
        print(f"✗ Failed to test agent session creation: {e}")
        traceback.print_exc()
        return False

def main():
    """Run all tests."""
    print("=" * 80)
    print("AQUATRACE AI MARITIME INVESTIGATION AGENT - VALIDATION TEST")
    print("=" * 80)
    print(f"Test started: {datetime.now().isoformat()}")
    print()
    
    tests = [
        ("Basic Imports", test_basic_imports),
        ("Investigation Agent Import", test_investigation_agent_import),
        ("FastAPI Router Import", test_fastapi_router_import),
        ("Tool Metadata", test_tool_metadata),
        ("Agent Methods", test_agent_methods),
        ("Agent Session Creation", test_agent_session_creation)
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        try:
            if test_func():
                passed += 1
                print(f"✅ {test_name}: PASSED")
            else:
                failed += 1
                print(f"❌ {test_name}: FAILED")
        except Exception as e:
            failed += 1
            print(f"❌ {test_name}: CRASHED - {e}")
            traceback.print_exc()
    
    print("\n" + "=" * 80)
    print("TEST RESULTS SUMMARY")
    print("=" * 80)
    print(f"Total tests: {len(tests)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Success rate: {passed/len(tests)*100:.1f}%")
    
    if failed == 0:
        print("\n🎉 ALL TESTS PASSED!")
        print("The AquaTrace AI Maritime Investigation Agent implementation is valid.")
        return True
    else:
        print(f"\n⚠️  {failed} test(s) failed.")
        print("The implementation may have issues that need to be addressed.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
