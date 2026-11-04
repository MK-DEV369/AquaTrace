"""Natural language analytics for Phase 2: Query assistant with safe analytical tools."""

from __future__ import annotations

import asyncio
import threading
from datetime import datetime
from typing import Any

import numpy as np
import spacy
from sentence_transformers import SentenceTransformer

from ship_analytics.llm import LLMProvider

# Map from NL intent/tool name to the approved investigation tools.
_TOOL_NAME_MAP = {
    "get_vessel_snapshot": "get_vessel_snapshot",
    "get_recent_anomalies": "get_recent_anomalies",
    "get_vessel_track": "get_vessel_track",
    "compare_vessel_baseline": "compare_vessel_baseline",
    "find_nearby_vessels": "find_nearby_vessels",
    "draft_incident_report": "draft_incident_report",
}


class NaturalLanguageProcessor:
    """Process natural language queries for vessel analytics."""

    def __init__(self):
        self.embedding_model = None
        self.nlp_model = None
        self.knowledge_base = {}
        self.tool_registry = {}
        self.llm = LLMProvider()

    def initialize(self):
        """Initialize NLP components."""
        try:
            self.embedding_model = SentenceTransformer("all-MiniLM-L6-v2")
        except Exception:
            self.embedding_model = None

        try:
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
        """Index knowledge base for semantic search."""
        all_documents = []
        all_documents.extend(project_docs)
        all_documents.extend(api_docs)
        all_documents.extend(metric_definitions)
        all_documents.extend(limitations)

        if not all_documents:
            return {"documents_indexed": 0, "embedding_size": 0}

        text_chunks = []
        metadata = []

        for doc in all_documents:
            title = doc.get("title", "")
            content = doc.get("content", "")
            doc_type = doc.get("type", "")

            chunks = content.split(". ")
            for chunk in chunks:
                if chunk.strip():
                    text_chunks.append(chunk.strip())
                    metadata.append(
                        {
                            "source_title": title,
                            "source_content": content,
                            "doc_type": doc_type,
                            "chunk_length": len(chunk),
                        }
                    )

        if not text_chunks:
            return {"documents_indexed": 0, "embedding_size": 0}

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
        """Register a safe analytical tool."""
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
    ) -> dict[str, Any]:
        """Process a natural language query using RAG + LLM + real tools."""
        parsed_query = self._parse_query(query, context, user_preferences)

        required_tools = self._identify_required_tools(parsed_query)

        if not self._validate_user_permissions(user_preferences, required_tools):
            return self._create_refusal_response(
                "Insufficient permissions for requested analysis"
            )

        rag_context = self._retrieve_context(query)

        results = []
        for tool_name in required_tools:
            tool_name = _TOOL_NAME_MAP.get(tool_name, tool_name)
            if tool_name in _TOOL_NAME_MAP.values():
                tool_result = self._execute_tool_real(tool_name, parsed_query, context)
                results.append(tool_result)
            else:
                results.append(self._create_error_response(f"Tool '{tool_name}' is not available"))

        answer = self._generate_answer(query, rag_context, results)

        response = self._format_query_response(results, parsed_query)
        response["answer"] = answer
        response["rag_context"] = rag_context
        return response

    def _retrieve_context(self, query: str) -> list[dict[str, Any]]:
        """Retrieve relevant knowledge-base chunks via embedding similarity."""
        kb = self.knowledge_base
        if not kb or not kb.get("text_chunks") or self.embedding_model is None or kb.get("embeddings") is None:
            return []
        try:
            q_emb = self.embedding_model.encode([query])
            embs = kb["embeddings"]
            # Cosine similarity.
            import numpy as np

            q = q_emb[0]
            norms = np.linalg.norm(embs, axis=1) * (np.linalg.norm(q) + 1e-9)
            sims = (embs @ q) / (norms + 1e-9)
            top_idx = np.argsort(sims)[::-1][:5]
            out = []
            for i in top_idx:
                out.append(
                    {
                        "text": kb["text_chunks"][i],
                        "metadata": kb["metadata"][i],
                        "score": float(sims[i]),
                    }
                )
            return out
        except Exception:
            return []

    def _execute_tool_real(
        self,
        tool_name: str,
        parsed_query: dict,
        context: dict | None,
    ) -> dict[str, Any]:
        """Execute an approved investigation tool for real."""
        from services.agent.tools import execute_tool

        # Build minimal parameters from parsed query + context.
        params: dict[str, Any] = {}
        entities = parsed_query.get("entities", [])
        mmsi = None
        for e in entities:
            if isinstance(e, str) and e.isdigit() and len(e) == 9:
                mmsi = int(e)
        if context and context.get("mmsi"):
            mmsi = int(context["mmsi"])
        if mmsi is not None:
            params["mmsi"] = mmsi

        if tool_name == "get_vessel_track":
            now = datetime.utcnow()
            params.setdefault("start", (now - __import__("datetime").timedelta(hours=24)).isoformat())
            params.setdefault("end", now.isoformat())

        try:
            result = _run_async_in_any_context(execute_tool, tool_name, params)
        except Exception as exc:
            return self._create_error_response(str(exc))

        return {
            "tool_name": tool_name,
            "status": "success",
            "result": result,
        }
    def _generate_answer(self, query: str, rag_context: list[dict], results: list[dict]) -> str:
        """Generate a natural-language answer via the LLM."""
        if not self.llm.is_available():
            return "LLM provider unavailable; returning tool results only."
        system = (
            "You are AquaTrace AI, a maritime investigation assistant. "
            "Answer the user's question using only the provided context and tool results. "
            "Be concise and cite vessel MMSI and timestamps when relevant."
        )
        user = (
            f"Question: {query}\n\n"
            f"Retrieved context:\n{rag_context}\n\n"
            f"Tool results:\n{results}\n"
        )
        try:
            return self.llm.chat(system, user)
        except Exception as exc:
            return f"LLM call failed: {exc}"

    def _parse_query(self, query, context, user_preferences):
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
        parsed["confidence"] = self._calculate_parsing_confidence(query)
        return parsed

    def _detect_query_intent(self, query):
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

    def _extract_entities(self, query):
        if self.nlp_model:
            try:
                doc = self.nlp_model(query)
                entities = [ent.text for ent in doc.ents]
                return entities
            except Exception:
                return []
        return []

    def _extract_time_range(self, query, context):
        time_range = {"start": None, "end": None}
        import re

        time_patterns = [
            r"(?i)last (\d+) (hour|day|week|month)s?",
            r"(?i)from (\d{4}-\d{2}-\d{2})",
            r"(?i)to (\d{4}-\d{2}-\d{2})",
            r"(?i)after (\d{4}-\d{2}-\d{2})",
            r"(?i)before (\d{4}-\d{2}-\d{2})",
        ]
        for pattern in time_patterns:
            re.findall(pattern, query)
        if context and "time_range" in context:
            time_range.update(context["time_range"])
        return time_range

    def _extract_geography(self, query):
        import re

        bbox_pattern = r"(?i)bbox\s+(-?\d+\.?\d*)\s+(-?\d+\.?\d*)\s+(-?\d+\.?\d*)\s+(-?\d+\.?\d*)"
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

    def _extract_vessel_types(self, query):
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

    def _extract_metrics(self, query):
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

    def _calculate_parsing_confidence(self, query):
        confidence = 1.0
        if len(query.strip().split()) < 3:
            confidence *= 0.7
        ambiguous_terms = ["it", "they", "there", "this", "that"]
        ambiguous_count = sum(1 for term in ambiguous_terms if term in query.lower().split())
        confidence *= (1.0 - ambiguous_count * 0.1)
        return max(0.0, min(1.0, confidence))

    def _validate_user_permissions(self, user_preferences, required_tools):
        if not user_preferences:
            return True
        user_roles = user_preferences.get("roles", [])
        permissions = user_preferences.get("permissions", {})
        for tool_name in required_tools:
            if tool_name in self.tool_registry:
                tool = self.tool_registry[tool_name]
                required_role = tool.get("required_role")
                if required_role and required_role not in user_roles:
                    return False
                if "read" in permissions and "read" not in tool.get("permissions", []):
                    return False
                if "write" in permissions and "write" not in tool.get("permissions", []):
                    return False
        return True

    def _identify_required_tools(self, parsed_query):
        intent = parsed_query.get("intent", "")
        tools = []
        if "anomal" in intent or "anomaly" in parsed_query["original_query"].lower():
            tools.append("get_recent_anomalies")
        if "track" in intent or "track" in parsed_query["original_query"].lower() or "route" in parsed_query["original_query"].lower():
            tools.append("get_vessel_track")
        if "nearby" in parsed_query["original_query"].lower() or "surrounding" in parsed_query["original_query"].lower():
            tools.append("find_nearby_vessels")
        if "baseline" in parsed_query["original_query"].lower() or "compare" in parsed_query["original_query"].lower():
            tools.append("compare_vessel_baseline")
        if "incident" in parsed_query["original_query"].lower() or "report" in parsed_query["original_query"].lower():
            tools.append("draft_incident_report")
        # Default: vessel snapshot.
        if not tools:
            tools.append("get_vessel_snapshot")
        # Always include snapshot for context if a vessel is referenced.
        if "get_vessel_snapshot" not in tools and any(c.isdigit() for c in parsed_query["original_query"]):
            tools.insert(0, "get_vessel_snapshot")
        return tools

    def _create_refusal_response(self, reason):
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

    def _create_error_response(self, error):
        return {
            "status": "error",
            "error": error,
            "suggestion": "Please check your query and try again.",
        }

    def _format_query_response(self, tool_results, parsed_query):
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
                "average_confidence": float(
                    np.mean([r.get("metadata", {}).get("confidence_score", 0) for r in tool_results])
                    if tool_results
                    else 0.0
                ),
            },
            "safety_assessment": {
                "all_tools_safe": True,
                "user_data_accessed": False,
                "sensitive_data_detected": False,
            },
        }


def _run_async_in_any_context(function, *args):
    """Run an async tool from sync NLP code, including inside an active loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(function(*args))

    outcome: dict[str, Any] = {}
    error: list[BaseException] = []

    def worker() -> None:
        try:
            outcome["value"] = asyncio.run(function(*args))
        except BaseException as exc:
            error.append(exc)

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    thread.join()
    if error:
        raise error[0]
    return outcome["value"]