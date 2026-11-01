#!/usr/bin/env python3
import sys

try:
    import jwt
    print('jwt module available')
except ImportError as e:
    print('jwt import failed:', str(e))
    sys.exit(1)

# Test websocket imports
try:
    from services.websocket.handler import WebSocketHandler
    from services.websocket.connection_manager import EnhancedConnectionManager
    print('WebSocketHandler imported successfully')
    print('EnhancedConnectionManager imported successfully')
    
    # Test instantiation
    manager = EnhancedConnectionManager('test-secret', 60)
    print('EnhancedConnectionManager instantiated')
    
    handler = WebSocketHandler(manager)
    print('WebSocketHandler instantiated')
    
    print('\n[OK] WebSocket infrastructure is working correctly!')
    
except Exception as e:
    print('WebSocket infrastructure failed:', str(e))
    import traceback
    traceback.print_exc()
    sys.exit(1)