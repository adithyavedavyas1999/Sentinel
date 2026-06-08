from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel

from sentinel.agent import llm


class _Diagnosis(BaseModel):
    category: str
    proposed_fix: str
    confidence: float


def test_mock_returns_text_when_no_schema():
    client = llm.MockLLMClient(["hello world"])
    resp = client.complete("anything")
    assert resp.text == "hello world"
    assert resp.parsed is None


def test_mock_returns_parsed_dict():
    client = llm.MockLLMClient(
        [{"category": "schema_drift", "proposed_fix": "coerce-to-string", "confidence": 0.9}]
    )
    resp = client.complete("ignored", json_schema=_Diagnosis)
    assert resp.parsed == {
        "category": "schema_drift",
        "proposed_fix": "coerce-to-string",
        "confidence": 0.9,
    }


def test_mock_validates_against_schema():
    client = llm.MockLLMClient([{"category": "x"}])  # missing required fields
    with pytest.raises(llm.LLMError):
        client.complete("p", json_schema=_Diagnosis)


def test_mock_records_call():
    client = llm.MockLLMClient(["ok"])
    client.complete("the prompt", system="be terse", temperature=0.2)
    [call] = client.calls
    assert call["prompt"] == "the prompt"
    assert call["system"] == "be terse"
    assert call["temperature"] == 0.2


def test_mock_exhausted_queue_raises():
    client = llm.MockLLMClient([])
    with pytest.raises(llm.LLMError):
        client.complete("p")


def test_mock_string_response_with_schema_validates():
    client = llm.MockLLMClient(
        ['{"category": "infra", "proposed_fix": "retry-with-backoff", "confidence": 0.5}']
    )
    resp = client.complete("p", json_schema=_Diagnosis)
    assert resp.parsed["category"] == "infra"


def test_parse_strips_json_fence():
    text = '```json\n{"category": "a", "proposed_fix": "b", "confidence": 0.1}\n```'
    parsed = llm._parse_json_response(text, _Diagnosis)
    assert parsed["category"] == "a"


def test_parse_raises_on_bad_json():
    with pytest.raises(llm.LLMError):
        llm._parse_json_response("not json at all", _Diagnosis)


def test_extract_handles_minimal_response():
    fake = MagicMock()
    fake.choices = [MagicMock()]
    fake.choices[0].message.content = "hi"
    fake.usage.prompt_tokens = 10
    fake.usage.completion_tokens = 5
    text, _, t_in, t_out = llm._extract(fake)
    assert text == "hi"
    assert t_in == 10
    assert t_out == 5


def test_extract_tolerates_no_usage():
    fake = MagicMock()
    fake.choices = [MagicMock()]
    fake.choices[0].message.content = "hi"
    fake.usage = None
    text, _, t_in, t_out = llm._extract(fake)
    assert text == "hi"
    assert t_in == 0
    assert t_out == 0


def test_extract_empty_when_no_choices():
    fake = MagicMock()
    fake.choices = []
    text, _, _, _ = llm._extract(fake)
    assert text == ""


def test_real_client_calls_litellm(monkeypatch):
    """LLMClient.complete -> litellm.completion. Mock the import."""
    captured: dict[str, Any] = {}

    def fake_completion(**kwargs: Any) -> Any:
        captured["kwargs"] = kwargs
        resp = MagicMock()
        resp.choices = [MagicMock()]
        resp.choices[0].message.content = "all good"
        resp.usage.prompt_tokens = 1
        resp.usage.completion_tokens = 2
        return resp

    fake_module = MagicMock()
    fake_module.completion = fake_completion
    monkeypatch.setitem(__import__("sys").modules, "litellm", fake_module)

    client = llm.LLMClient(model="groq/test-model")
    resp = client.complete("hello", system="be brief")
    assert resp.text == "all good"
    assert captured["kwargs"]["model"] == "groq/test-model"
    assert captured["kwargs"]["messages"][0] == {"role": "system", "content": "be brief"}
    assert captured["kwargs"]["messages"][1] == {"role": "user", "content": "hello"}


def test_real_client_retries_on_retryable(monkeypatch):
    class RateLimitError(Exception):
        pass

    calls = {"n": 0}

    def fake_completion(**_: Any) -> Any:
        calls["n"] += 1
        if calls["n"] < 3:
            raise RateLimitError("slow down")
        resp = MagicMock()
        resp.choices = [MagicMock()]
        resp.choices[0].message.content = "made it"
        resp.usage = None
        return resp

    fake_module = MagicMock()
    fake_module.completion = fake_completion
    monkeypatch.setitem(__import__("sys").modules, "litellm", fake_module)

    client = llm.LLMClient(model="x", max_retries=4)
    resp = client.complete("p")
    assert resp.text == "made it"
    assert calls["n"] == 3


def test_real_client_does_not_retry_on_non_retryable(monkeypatch):
    class AuthenticationError(Exception):  # not in retry allowlist
        pass

    calls = {"n": 0}

    def fake_completion(**_: Any) -> Any:
        calls["n"] += 1
        raise AuthenticationError("no key")

    fake_module = MagicMock()
    fake_module.completion = fake_completion
    monkeypatch.setitem(__import__("sys").modules, "litellm", fake_module)

    client = llm.LLMClient(model="x", max_retries=4)
    with pytest.raises(AuthenticationError):
        client.complete("p")
    assert calls["n"] == 1


def test_real_client_json_mode_validates(monkeypatch):
    def fake_completion(**_: Any) -> Any:
        resp = MagicMock()
        resp.choices = [MagicMock()]
        resp.choices[
            0
        ].message.content = (
            '{"category": "infra", "proposed_fix": "retry-with-backoff", "confidence": 0.4}'
        )
        resp.usage = None
        return resp

    fake_module = MagicMock()
    fake_module.completion = fake_completion
    monkeypatch.setitem(__import__("sys").modules, "litellm", fake_module)

    client = llm.LLMClient(model="x")
    resp = client.complete("p", json_schema=_Diagnosis)
    assert resp.parsed == {
        "category": "infra",
        "proposed_fix": "retry-with-backoff",
        "confidence": 0.4,
    }
