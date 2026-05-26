# Phase 1 blog post — outline only

> **DO NOT POST AN AI DRAFT.** Blog prose is the easiest AI fingerprint;
> reviewers and engineers spot it instantly. Use this outline as a skeleton,
> sit with it for a few days, write the prose yourself. ~1500 words is the
> target. Personal voice beats polish every time.

## Working titles (pick one or write your own)

- "What I learned building a self-healing pipeline that mostly heals itself"
- "Sentinel, week 1-6: the failure path is the product"
- "I built a data pipeline whose only job is to break interestingly"

## Hook — 100 words

Open with a specific failure. Not "data pipelines often break." Something
concrete: "TLC renamed `VendorID` to `vendor_id` in early 2025 and broke
five hundred downstream dashboards at midnight." Then your one-line thesis:
the interesting work is the failure path; happy paths all look the same.

## Why this project (~200 words)

What you actually wanted to learn. Self-healing systems are hyped; you
wanted to see where the line between "useful automation" and "an agent
confidently doing the wrong thing" sits. Pick one personal anecdote about
a real on-call incident if you have one; if not, skip — don't invent.

## The stack and the trade-offs (~300 words)

- Why Dagster (asset graph feeds the agent's context — link to ADR-001)
- Why dbt + DuckDB (local, but warehouse-shaped — link to ADR-002)
- Why dbt tests over Great Expectations (link to ADR-003)
- Why a hard remediation allowlist (link to ADR-004 — this is the one
  readers will engage with)

Don't restate the ADRs. Quote your own two best lines from each and link
out. Reviewers who care will read the ADRs.

## What was actually hard (~400 words)

Pick three concrete bugs you hit. Honest ones. Examples from the repo:

- The `from __future__ import annotations` thing — Dagster validates
  asset context against the actual class, not strings.
- The Pydantic `ConfigurableResource` model-copy that broke the fake
  storage test.
- The dbt sources pattern over MinIO via DuckDB httpfs — was harder to
  configure than it should have been.

For each: what you tried first, what you discovered, what you committed.
Include a code snippet for at least one. Engineers trust posts with code.

## The refactor (~200 words)

The Python-silver to dbt-silver move in week 4. Why it had to wait until
week 4 (you didn't know what the silver shape needed to be until you'd
lived with it). What you'd do differently. The commit message you wrote.

This section is what makes the post read like a project journal, not a
tutorial.

## What I'm not going to do (~150 words)

The bits you deliberately punted:
- No real auto-remediation for SQL errors — that's a non-goal
- No multi-tenant story
- No streaming (Phase 2 — flag it but don't pre-spoiler)
- No production deploy story — this is a local rig and that's the point

## What's next (~100 words)

One sentence on Phase 2. Don't oversell.

## Closing (~50 words)

A line that's specific to you. Something like the project taught you about
trust boundaries, or about resisting the urge to over-engineer.

## After you write

- Read it out loud. If a sentence sounds like a blog post, rewrite.
- Cut every "leverages", "robust", "comprehensive", "seamlessly", "powerful",
  "production-ready", "modern stack", "industry-leading", "best practices",
  "in this article we will...", "I hope this helps".
- Cut at least 100 words on the second pass. Tighter is better.
- Have one engineer friend read it. Ask: "does this sound like me?"
