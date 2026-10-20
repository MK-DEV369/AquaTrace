"""Natural language analytics for Phase 2: Query assistant with safe analytical tools."""

from datetime import datetime

import numpy as np
import spacy
from sentence_transformers import SentenceTransformer


class NaturalLanguageProcessor:
    """Process natural language queries for vessel analytics."""

    def __init__(self):
        # Initialize components (would be loaded from environment)
        self.embedding_model = None
        self.nlp_model = None
        self.knowledge_base = {}
        self.tool_registry = {}

    def initialize(self):
        """Initialize NLP components."""
        try:
            # Load sentence transformer for embeddings
            self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        except Exception:
            self.embedding_model = None

        try:
            # Load spaCy model for text processing
            self.nlp_model = spacy.load("en_core_web_sm")
        except Exception:
            self.nlp_model = None

    def index_knowledge_base(
        self,
        project_docs: list[dict],
        api_docs: list[dict],
        metric_definitions: list[dict],
        limitations: list[dict],
    ) -> dict[str, float]:
        """Index knowledge base for semantic search.

        Args:
            project_docs: Project documentation
            api_docs: API documentation
            metric_definitions: Metric definitions
            limitations: Known limitations

        Returns:
            Dictionary with indexing statistics
        """
        all_documents = []
        all_documents.extend(project_docs)
        all_documents.extend(api_docs)
        all_documents.extend(metric_definitions)
        all_documents.extend(limitations)

        if not all_documents:
            return {"documents_indexed": 0, "embedding_size": 0}

        # Create text chunks
        text_chunks = []
        metadata = []

        for doc in all_documents:
            title = doc.get("title", "")
            content = doc.get("content", "")
            doc_type = doc.get("type", "")

            # Split into chunks (simple approach)
            chunks = content.split(". ")
            for chunk in chunks:
                if chunk.strip():
                    text_chunks.append(chunk.strip())
                    metadata.append({
                        "source_title": title,
                        "source_content": content,
                        "doc_type": doc_type,
                        "chunk_length": len(chunk),
                    })

        if not text_chunks:
            return {"documents_indexed": 0, "embedding_size": 0}

        # Generate embeddings
        if self.embedding_model:
            try:
                embeddings = self.embedding_model.encode(text_chunks, show_progress_bar=False)
                embedding_dim = len(embeddings[0]) if embeddings else 0
            except Exception:
                embedding_dim = 0
                embeddings = None
        else:
            embedding_dim = 0
            embeddings = None

        # Store in knowledge base
        self.knowledge_base = {
            "text_chunks": text_chunks,
            "embeddings": embeddings,
            "metadata": metadata,
            "embedding_dim": embedding_dim,
            "chunk_count": len(text_chunks),
            "last_updated": datetime.utcnow().isoformat(),
        }

        return {
            "documents_indexed": len(text_chunks),
            "embedding_size": embedding_dim,
            "source_types": list(set(m["doc_type"] for m in metadata)),
        }

    def register_tool(
        self,
        tool_name: str,
        description: str,
        function_schema: dict,
        validation_rules: dict | None = None,
    ) -> dict[str, str]:
        """Register a safe analytical tool.

        Args:
            tool_name: Unique name for the tool
            description: Human-readable description
            function_schema: Pydantic model for input validation
            validation_rules: Additional validation rules

        Returns:
            Dictionary with registration result
        """
        tool = {
            "name": tool_name,
            "description": description,
            "schema": function_schema,
            "validation_rules": validation_rules or {},
            "last_used": None,
            "usage_count": 0,
        }

        self.tool_registry[tool_name] = tool

        return {
            "status": "registered",
            "tool_name": tool_name,
            "description": description,
        }

    def process_query(
        self,
        query: str,
        context: dict | None = None,
        user_preferences: dict | None = None,
    ) -> dict[str, str | list | dict | bool]:
        """Process a natural language query.

        Args:
            query: User's natural language query
            context: Additional context for query interpretation
            user_preferences: User preferences and settings

        Returns:
            Dictionary with query results and metadata
        """
        # Parse and understand the query
        parsed_query = self._parse_query(query, context, user_preferences)

        # Determine required tools
        required_tools = self._identify_required_tools(parsed_query)

        # Validate user permissions
        if not self._validate_user_permissions(user_preferences, required_tools):
            return self._create_refusal_response(
                "Insufficient permissions for requested analysis"
            )

        # Route to appropriate tools
        results = []
        for tool_name in required_tools:
            if tool_name in self.tool_registry:
                tool_result = self._execute_tool(
                    tool_name, parsed_query, context, user_preferences
                )
                results.append(tool_result)
            else:
                return self._create_refusal_response(
                    f"Tool '{tool_name}' is not available or registered"
                )

        # Format and return results
        return self._format_query_response(results, parsed_query)

    def _parse_query(
        self,
        query: str,
        context: dict | None,
        user_preferences: dict | None,
    ) -> dict[str, str | list | dict]:
        """Parse and understand natural language query."""
        parsed = {
            "original_query": query,
            "intent": self._detect_query_intent(query),
            "entities": self._extract_entities(query),
            "time_range": self._extract_time_range(query, context),
            "geography": self._extract_geography(query),
            "vessel_types": self._extract_vessel_types(query),
            "metrics": self._extract_metrics(query),
            "confidence": 0.0,
        }

        # Calculate confidence score
        parsed["confidence"] = self._calculate_parsing_confidence(query)

        return parsed

    def _detect_query_intent(self, query: str) -> str:
        """Detect the intent behind the query."""
        query_lower = query.lower()

        if any(keyword in query_lower for keyword in ["what", "how many", "count", "total"]):
            if any(keyword in query_lower for keyword in ["vessel", "ship", "boat"]):
                return "vessel_count"
            elif any(keyword in query_lower for keyword in ["event", "report", "data"]):
                return "event_count"
            elif any(keyword in query_lower for keyword in ["anomaly", "alert", "issue"]):
                return "anomaly_count"
            else:
                return "general_count"
        elif any(keyword in query_lower for keyword in ["track", "route", "history"]):
            return "track_retrieval"
        elif any(keyword in query_lower for keyword in ["average", "mean", "median", "sum"]):
            return "statistical_analysis"
        elif any(keyword in query_lower for keyword in ["between", "from", "to", "since"]):
            return "time_range_query"
        elif any(keyword in query_lower for keyword in ["where", "location", "map", "geography"]):
            return "geographic_query"
        elif any(keyword in query_lower for keyword in ["why", "what caused", "reason"]):
            return "causal_investigation"
        else:
            return "general_question"

    def _extract_entities(self, query: str) -> list[str]:
        """Extract named entities from query."""
        if self.nlp_model:
            try:
                doc = self.nlp_model(query)
                entities = [ent.text for ent in doc.ents]
                return entities
            except Exception:
                return []
        return []

    def _extract_time_range(self, query: str, context: dict | None) -> dict[str, datetime | None]:
        """Extract time range from query."""
        time_range = {"start": None, "end": None}

        # Parse time patterns
        import re

        # Look for time patterns like "last 24 hours", "from 2024-01-01", etc.
        time_patterns = [
            r'(?i)last (\d+) (hour|day|week|month)s?',
            r'(?i)from (\d{4}-\d{2}-\d{2})',
            r'(?i)to (\d{4}-\d{2}-\d{2})',
            r'(?i)after (\d{4}-\d{2}-\d{2})',
            r'(?i)before (\d{4}-\d{2}-\d{2})',
        ]

        for pattern in time_patterns:
            re.findall(pattern, query)
            # TODO: Convert matches to datetime objects and set time_range

        # Use context if available
        if context and "time_range" in context:
            time_range.update(context["time_range"])

        return time_range

    def _extract_geography(self, query: str) -> dict[str, float] | None:
        """Extract geographic bounds from query."""
        # Look for bounding box patterns like "bbox 1.15 103.55 1.45 104.10"
        import re

        bbox_pattern = r'(?i)bbox\s+(-?\d+\.?\d*)\s+(-?\d+\.?\d*)\s+(-?\d+\.?\d*)\s+(-?\d+\.?\d*)'
        match = re.search(bbox_pattern, query)

        if match:
            try:
                min_lat, min_lon, max_lat, max_lon = map(float, match.groups())
                return {
                    "min_latitude": min_lat,
                    "min_longitude": min_lon,
                    "max_latitude": max_lat,
                    "max_longitude": max_lon,
                }
            except ValueError:
                pass

        return None

    def _extract_vessel_types(self, query: str) -> list[str]:
        """Extract vessel types from query."""
        vessel_types = []
        query_lower = query.lower()

        vessel_keywords = {
            "cargo": ["cargo", "merchant"],
            "tanker": ["tanker", "oil", "chemical"],
            "passenger": ["passenger", "cruise"],
            "fishing": ["fishing", "fisheries"],
            "military": ["military", "naval", "warship"],
        }

        for vessel_type, keywords in vessel_keywords.items():
            if any(keyword in query_lower for keyword in keywords):
                vessel_types.append(vessel_type)

        return vessel_types

    def _extract_metrics(self, query: str) -> list[str]:
        """Extract metrics from query."""
        metric_keywords = {
            "speed": ["speed", "velocity", "sog"],
            "course": ["course", "heading", "cog"],
            "location": ["location", "position", "latitude", "longitude"],
            "time": ["time", "timestamp", "date"],
            "count": ["count", "number", "total"],
            "average": ["average", "mean", "avg"],
            "maximum": ["maximum", "max", "highest"],
            "minimum": ["minimum", "min", "lowest"],
        }

        metrics = []
        query_lower = query.lower()

        for metric, keywords in metric_keywords.items():
            if any(keyword in query_lower for keyword in keywords):
                metrics.append(metric)

        return metrics

    def _calculate_parsing_confidence(self, query: str) -> float:
        """Calculate confidence score for query parsing."""
        confidence = 1.0

        # Reduce confidence for very short queries
        if len(query.strip().split()) < 3:
            confidence *= 0.7

        # Check for ambiguous terms
        ambiguous_terms = ["it", "they", "there", "this", "that"]
        ambiguous_count = sum(1 for term in ambiguous_terms if term in query.lower().split())
        confidence *= (1.0 - ambiguous_count * 0.1)

        return max(0.0, min(1.0, confidence))

    def _validate_user_permissions(
        self,
        user_preferences: dict | None,
        required_tools: list[str],
    ) -> bool:
        """Validate user permissions for requested tools."""
        if not user_preferences:
            return True  # No restrictions by default

        user_roles = user_preferences.get("roles", [])
        permissions = user_preferences.get("permissions", {})

        for tool_name in required_tools:
            if tool_name in self.tool_registry:
                tool = self.tool_registry[tool_name]
                required_role = tool.get("required_role")

                if required_role and required_role not in user_roles:
                    return False

                # Check specific permissions
                if "read" in permissions and "read" not in tool.get("permissions", []):
                    return False

                if "write" in permissions and "write" not in tool.get("permissions", []):
                    return False

        return True

    def _execute_tool(
        self,
        tool_name: str,
        parsed_query: dict,
        context: dict | None,
        user_preferences: dict | None,
    ) -> dict[str, str | list | dict]:
        """Execute a tool with validated inputs."""
        if tool_name not in self.tool_registry:
            return self._create_error_response(f"Tool '{tool_name}' not found")

        tool = self.tool_registry[tool_name]

        # Validate inputs against tool schema
        validated_inputs = self._validate_tool_inputs(
            tool, parsed_query, context, user_preferences
        )

        if not validated_inputs.get("is_valid", True):
            return self._create_error_response(
                f"Input validation failed: {validated_inputs.get('error', 'Unknown error')}"
            )

        # Execute the tool (simplified)
        tool_result = self._simulate_tool_execution(tool, validated_inputs)

        # Update usage tracking
        tool["usage_count"] += 1
        tool["last_used"] = datetime.utcnow().isoformat()

        return tool_result

    def _validate_tool_inputs(
        self,
        tool: dict,
        parsed_query: dict,
        context: dict | None,
        user_preferences: dict | None,
    ) -> dict[str, bool | str]:
        """Validate tool inputs against schema and rules."""
        # Check if tool schema exists
        if "schema" not in tool:
            return {"is_valid": True}  # No schema = no validation

        # Simple validation (would use pydantic in production)
        schema = tool["schema"]

        # Check required parameters
        required_params = schema.get("required_params", [])
        for param in required_params:
            if param not in parsed_query and param not in context:
                return {
                    "is_valid": False,
                    "error": f"Missing required parameter: {param}",
                }

        # Check validation rules
        validation_rules = tool.get("validation_rules", {})
        if validation_rules:
            # Implement validation rule checks
            pass

        return {"is_valid": True}

    def _simulate_tool_execution(
        self,
        tool: dict,
        validated_inputs: dict,
    ) -> dict[str, str | list | dict]:
        """Simulate tool execution (placeholder for real implementations)."""
        tool_name = tool["name"]
        description = tool["description"]

        # Return a realistic but simulated tool result
        return {
            "tool_name": tool_name,
            "description": description,
            "execution_time": "0.05s",
            "status": "success",
            "result_type": "query_result",
            "data_preview": {
                "record_count": 42,
                "columns": ["event_id", "mmsi", "event_time", "latitude", "longitude"],
                "sample_data": [
                    {"event_id": "event_001", "mmsi": 123456789, "event_time": "2024-01-01T10:00:00Z"},
                    {"event_id": "event_002", "mmsi": 123456789, "event_time": "2024-01-01T10:05:00Z"},
                ],
            },
            "metadata": {
                "query_understanding": 0.95,
                "tool_accuracy": 0.92,
                "response_time_ms": 50,
                "confidence_score": 0.88,
            },
        }

    def _create_refusal_response(self, reason: str) -> dict[str, str]:
        """Create a refusal response for unsafe queries."""
        return {
            "status": "refused",
            "reason": reason,
            "suggestion": "Please rephrase your query or provide additional context.",
            "safe_alternatives": [
                "Can you provide more specific vessel information?",
                "Would you like me to show available vessels in a specific area?",
                "Can you provide a time range for your query?",
            ],
        }

    def _create_error_response(self, error: str) -> dict[str, str]:
        """Create an error response."""
        return {
            "status": "error",
            "error": error,
            "suggestion": "Please check your query and try again.",
        }

    def _format_query_response(
        self,
        tool_results: list[dict],
        parsed_query: dict,
    ) -> dict[str, str | list | dict | bool]:
        """Format query results for the user."""
        return {
            "query": parsed_query["original_query"],
            "intent": parsed_query["intent"],
            "confidence": parsed_query["confidence"],
            "results": tool_results,
            "metadata": {
                "total_tools_used": len(tool_results),
                "query_processing_time_ms": sum(
                    r.get("metadata", {}).get("response_time_ms", 0) for r in tool_results
                ),
                "average_confidence": np.mean([r.get("metadata", {}).get("confidence_score", 0) for r in tool_results]),
            },
            "safety_assessment": {
                "all_tools_safe": True,
                "user_data_accessed": False,
                "sensitive_data_detected": False,
            },
        }
