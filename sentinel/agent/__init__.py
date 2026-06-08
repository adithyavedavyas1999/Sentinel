"""Phase 2 agent surface.

The agent is the point of the project — but very little of it lives here
yet. Week 7 ships the LLM wrapper. Week 8 ships context retrieval. The
LangGraph state machine that ties them together lands in week 9.

Public surface (today):

- :mod:`sentinel.agent.llm`: provider-agnostic LLM client, mock for tests
- :mod:`sentinel.agent.context`: pulls dbt manifest, run history, recent
  logs, similar past incidents
- :mod:`sentinel.agent.embeddings`: fastembed + Qdrant for similar-incident
  retrieval

Nothing in this package should reach into Dagster's `Definitions` directly.
The flow is: sensor catches failure → builds an incident → agent reads
the incident + context → returns a structured diagnosis. Keeping the
agent decoupled from the orchestrator makes the unit tests possible at
all, and makes provider-swap less painful when (not if) we change LLMs.
"""
