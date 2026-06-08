"""Chaos flag store.

Some chaos scenarios can't be expressed as a single one-shot mutation. A
"simulated upstream 5xx", for instance, has to persist across the gap
between when the operator runs `sentinel chaos inject tlc_5xx` and when
the next Dagster run picks up the partition. We need somewhere to leave
a note saying "next run of this asset, raise."

That's all this module is. A flat JSON file keyed by scenario name. Each
asset that participates in chaos calls :func:`is_active` early, raises
:class:`ChaosTriggered` if the flag is set, and otherwise carries on.

Persistence rationale (as opposed to e.g. SQLite or Redis):

- It survives process restarts, which Dagster sensors and runs do constantly.
- It's introspectable: ``cat data/chaos/state.json`` is a useful debug
  primitive and the agent's context builder reads from here too.
- It's ~50 lines of code instead of ~500.
- Concurrency is not a concern. There's one human running the chaos CLI;
  pipeline runs only read.

When the agent gets remediation in week 10, the expectation is that a
successful auto-retry clears the corresponding flag. That's the audit
trail: flag set by chaos at T0, cleared by agent at T1 — the gap is the
self-heal latency we'd report on.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_DEFAULT_PATH = "./data/chaos/state.json"


class ChaosTriggered(RuntimeError):
    """Raised by assets when a chaos flag is set for their scenario.

    Distinct from a regular runtime error so the failure_capture sensor
    can label these incidents and the eval suite can grade the agent
    against known ground truth.
    """


def _path() -> Path:
    return Path(os.environ.get("SENTINEL_CHAOS_STATE_PATH", _DEFAULT_PATH))


def _read() -> dict[str, dict[str, Any]]:
    p = _path()
    if not p.exists():
        return {}
    try:
        text = p.read_text()
        return json.loads(text) if text.strip() else {}
    except json.JSONDecodeError:
        # Malformed file. Don't blow up here — the operator can `chaos clear-all`
        # and we shouldn't take down a production sensor over a bad json file.
        return {}


def _write(state: dict[str, dict[str, Any]]) -> None:
    p = _path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(state, indent=2, sort_keys=True))


def is_active(name: str) -> bool:
    return name in _read()


def set_active(name: str, **payload: Any) -> None:
    state = _read()
    state[name] = {
        "set_at": datetime.now(UTC).isoformat(),
        "payload": payload,
    }
    _write(state)


def clear(name: str) -> bool:
    state = _read()
    if name not in state:
        return False
    del state[name]
    _write(state)
    return True


def clear_all() -> int:
    state = _read()
    n = len(state)
    if n:
        _write({})
    return n


def list_active() -> list[dict[str, Any]]:
    return [{"name": k, **v} for k, v in _read().items()]
