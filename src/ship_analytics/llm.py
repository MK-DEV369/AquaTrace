"""Pluggable LLM provider for the RAG query agent.

Supports:
    * ollama            - local Ollama server (default; recommended free option)
    * transformers      - local Hugging Face model
    * openai-compatible - any OpenAI-compatible HTTP endpoint
"""

from __future__ import annotations

import logging

import httpx

from ship_analytics.config import get_settings

logger = logging.getLogger("ship_analytics.llm")


class LLMProvider:
    """Thin wrapper around a chat completion backend."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self.provider = self.settings.llm_provider
        self.model = self.settings.llm_model
        self.base_url = self.settings.llm_base_url.rstrip("/")
        self.api_key = self.settings.llm_api_key

    def chat(self, system: str, user: str, temperature: float = 0.2) -> str:
        if self.provider == "ollama":
            return self._ollama(system, user, temperature)
        if self.provider == "transformers":
            return self._transformers(system, user, temperature)
        if self.provider == "openai-compatible":
            return self._openai(system, user, temperature)
        raise ValueError(f"unknown llm provider {self.provider}")

    def _ollama(self, system: str, user: str, temperature: float) -> str:
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "stream": False,
            "options": {"temperature": temperature},
        }
        with httpx.Client(timeout=120) as client:
            resp = client.post(f"{self.base_url}/api/chat", json=payload)
            resp.raise_for_status()
            data = resp.json()
        return data.get("message", {}).get("content", "")

    def _openai(self, system: str, user: str, temperature: float) -> str:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": temperature,
        }
        with httpx.Client(timeout=120) as client:
            resp = client.post(f"{self.base_url}/v1/chat/completions", json=payload, headers=headers)
            resp.raise_for_status()
            data = resp.json()
        return data["choices"][0]["message"]["content"]

    def _transformers(self, system: str, user: str, temperature: float) -> str:
        try:
            import torch
            from transformers import AutoModelForCausalLM, AutoTokenizer
        except Exception as exc:
            raise RuntimeError(f"transformers not available: {exc}") from exc
        tokenizer = AutoTokenizer.from_pretrained(self.model)
        model = AutoModelForCausalLM.from_pretrained(self.model)
        device = "cuda" if torch.cuda.is_available() else "cpu"
        model = model.to(device)
        prompt = f"<|system|>\n{system}\n<|user|>\n{user}\n<|assistant|>\n"
        inputs = tokenizer(prompt, return_tensors="pt").to(device)
        out = model.generate(**inputs, max_new_tokens=512, temperature=max(temperature, 1e-4), do_sample=True)
        text = tokenizer.decode(out[0], skip_special_tokens=True)
        return text[len(prompt):].strip()

    def is_available(self) -> bool:
        if self.provider == "ollama":
            try:
                with httpx.Client(timeout=5) as client:
                    r = client.get(f"{self.base_url}/api/tags")
                    return r.status_code == 200
            except Exception:
                return False
        return True