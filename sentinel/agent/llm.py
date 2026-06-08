"""LLM client wrapper.

LiteLLM in front of whichever provider we point at via env. Default is
Groq's llama-3.1-8b-instant — cheap, fast, good enough for diagnosis,
and happily returns JSON when asked. Override with ``SENTINEL_LLM_MODEL``
or pass ``model=`` per call. Anthropic and OpenAI just work because
LiteLLM handles the translation; the only hand-tuning is which messages
get the system role.

Three things this module owns:

1. A retry policy on the provider errors that are actually retryable
   (rate limit + 5xx + transient network). We do not retry on auth
   errors or context-length-exceeded — those need human intervention.

2. A JSON-mode helper. We pass ``response_format={"type": "json_object"}``
   and parse the response. If the model returns garbage that doesn't
   parse, that's a model failure; surface it.

3. A mock client (``MockLLMClient``) for tests. The agent is the entire
   thing being tested in week 9; not bothering to mock the LLM would be
   irresponsible. The mock takes a queue of canned responses.

Notes:

- We don't stream. The diagnosis is short and the agent waits for the full
  payload anyway. Streaming complicates retry semantics and adds nothing.
- We don't try to count tokens precisely. LiteLLM returns usage if the
  provider does; we surface what we get.
"""

from __future__ import annotations

import json
import os
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, Protocol

from pydantic import BaseModel, ValidationError
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from sentinel.observability.logging import get_logger

log = get_logger(__name__)

DEFAULT_MODEL = os.environ.get("SENTINEL_LLM_MODEL", "groq/llama-3.1-8b-instant")


@dataclass
class LLMResponse:
    text: str
    model: str
    parsed: dict[str, Any] | None = None
    tokens_in: int = 0
    tokens_out: int = 0
    raw: dict[str, Any] = field(default_factory=dict)


class LLMError(RuntimeError):
    """Raised when the model returns something we can't use.

    Distinct from provider exceptions (rate limit, auth) — those bubble up
    as their original types so callers can react to them differently.
    """


# Retryable provider exception names. Listed by class name to avoid hard
# importing every provider's error hierarchy.
_RETRYABLE_NAMES = frozenset(
    {
        "RateLimitError",
        "InternalServerError",
        "Timeout",
        "APITimeoutError",
        "APIConnectionError",
        "ServiceUnavailableError",
    }
)


def _is_retryable(exc: BaseException) -> bool:
    return type(exc).__name__ in _RETRYABLE_NAMES


class LLMClientProtocol(Protocol):
    def complete(
        self,
        prompt: str,
        *,
        system: str | None = None,
        model: str | None = None,
        json_schema: type[BaseModel] | None = None,
        temperature: float = 0.0,
        max_tokens: int = 1024,
    ) -> LLMResponse: ...


class LLMClient:
    """Real client. Thin wrapper around ``litellm.completion``.

    LiteLLM is imported lazily so importing this module doesn't pull in
    the provider SDK chain at startup. That matters for the dagster
    daemon, which would otherwise import openai/anthropic just to define
    sensors that may never call the agent.
    """

    def __init__(
        self,
        model: str | None = None,
        *,
        max_tokens: int = 1024,
        max_retries: int = 4,
    ):
        self.model = model or DEFAULT_MODEL
        self.max_tokens = max_tokens
        self.max_retries = max_retries

    def complete(
        self,
        prompt: str,
        *,
        system: str | None = None,
        model: str | None = None,
        json_schema: type[BaseModel] | None = None,
        temperature: float = 0.0,
        max_tokens: int | None = None,
    ) -> LLMResponse:
        target_model = model or self.model
        messages = self._build_messages(prompt, system)

        kwargs: dict[str, Any] = {
            "model": target_model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens or self.max_tokens,
        }
        if json_schema is not None:
            # OpenAI-compatible json mode. LiteLLM relays this where supported.
            kwargs["response_format"] = {"type": "json_object"}

        log.info(
            "llm.request",
            model=target_model,
            json_mode=json_schema is not None,
            prompt_chars=len(prompt),
        )

        text, raw, tokens_in, tokens_out = self._call_with_retry(kwargs)

        parsed: dict[str, Any] | None = None
        if json_schema is not None:
            parsed = _parse_json_response(text, json_schema)

        return LLMResponse(
            text=text,
            model=target_model,
            parsed=parsed,
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            raw=raw,
        )

    def _call_with_retry(self, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any], int, int]:
        @retry(
            retry=retry_if_exception(_is_retryable),
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=1, min=2, max=15),
            reraise=True,
        )
        def _call() -> tuple[str, dict[str, Any], int, int]:
            import litellm  # lazy

            resp = litellm.completion(**kwargs)
            return _extract(resp)

        return _call()

    @staticmethod
    def _build_messages(prompt: str, system: str | None) -> list[dict[str, str]]:
        msgs: list[dict[str, str]] = []
        if system:
            msgs.append({"role": "system", "content": system})
        msgs.append({"role": "user", "content": prompt})
        return msgs


def _extract(resp: Any) -> tuple[str, dict[str, Any], int, int]:
    """Pull text + token usage out of a LiteLLM response.

    LiteLLM normalizes most providers to the OpenAI shape, so this is
    forgiving but not bulletproof. If a provider returns nothing parseable
    we get an empty string out, which the caller should treat as failure.
    """
    try:
        choice = resp.choices[0]
        text = choice.message.content or ""
    except (AttributeError, IndexError, KeyError):
        text = ""

    tokens_in = 0
    tokens_out = 0
    usage = getattr(resp, "usage", None)
    if usage is not None:
        tokens_in = getattr(usage, "prompt_tokens", 0) or 0
        tokens_out = getattr(usage, "completion_tokens", 0) or 0

    raw: dict[str, Any] = {}
    try:
        # litellm responses are pydantic; the safe cast is via .model_dump()
        # if available, else best-effort.
        if hasattr(resp, "model_dump"):
            raw = resp.model_dump()
        elif hasattr(resp, "to_dict"):
            raw = resp.to_dict()
    except Exception:
        raw = {}
    return text, raw, tokens_in, tokens_out


def _parse_json_response(text: str, schema: type[BaseModel]) -> dict[str, Any]:
    """Validate JSON output against a pydantic schema.

    Models love to wrap JSON in ```json ``` fences, even when explicitly
    told not to. We strip them defensively before parsing.
    """
    cleaned = text.strip()
    if cleaned.startswith("```"):
        # rough fence strip; works for ```json...``` and bare ```...```
        cleaned = cleaned.split("\n", 1)[-1] if "\n" in cleaned else ""
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()
    try:
        loaded = json.loads(cleaned)
    except json.JSONDecodeError as e:
        raise LLMError(f"model returned non-JSON: {e}") from e
    try:
        validated = schema.model_validate(loaded)
    except ValidationError as e:
        raise LLMError(f"model JSON did not match schema: {e}") from e
    return validated.model_dump()


# --- Mock ------------------------------------------------------------------


class MockLLMClient:
    """Test double. Returns canned responses in order.

    Pass either:
    - a list of strings (each call pops the next),
    - or a list of dicts (interpreted as JSON when ``json_schema`` is set,
      passed through as ``text=str(dict)`` otherwise).

    If the queue runs out, raises. Loud failure beats silent reuse.
    """

    def __init__(self, responses: Iterable[str | dict[str, Any]] | None = None) -> None:
        self._queue: deque[str | dict[str, Any]] = deque(responses or [])
        self.calls: list[dict[str, Any]] = []

    def queue(self, response: str | dict[str, Any]) -> None:
        self._queue.append(response)

    def complete(
        self,
        prompt: str,
        *,
        system: str | None = None,
        model: str | None = None,
        json_schema: type[BaseModel] | None = None,
        temperature: float = 0.0,
        max_tokens: int = 1024,
    ) -> LLMResponse:
        self.calls.append(
            {
                "prompt": prompt,
                "system": system,
                "model": model,
                "json_schema": json_schema.__name__ if json_schema else None,
                "temperature": temperature,
                "max_tokens": max_tokens,
            }
        )
        if not self._queue:
            raise LLMError("MockLLMClient queue exhausted")
        next_resp = self._queue.popleft()

        if isinstance(next_resp, dict):
            text = json.dumps(next_resp)
            parsed: dict[str, Any] | None = next_resp if json_schema else None
            if json_schema is not None:
                # validate even mock responses so tests catch schema drift,
                # surfacing the same exception type the real client would raise
                try:
                    parsed = schema_to_dict(next_resp, json_schema)
                except ValidationError as e:
                    raise LLMError(f"mock response did not match schema: {e}") from e
            return LLMResponse(
                text=text,
                model=model or "mock",
                parsed=parsed,
            )

        if json_schema is not None:
            parsed_validated = _parse_json_response(next_resp, json_schema)
            return LLMResponse(text=next_resp, model=model or "mock", parsed=parsed_validated)
        return LLMResponse(text=next_resp, model=model or "mock")


def schema_to_dict(payload: dict[str, Any], schema: type[BaseModel]) -> dict[str, Any]:
    return schema.model_validate(payload).model_dump()
