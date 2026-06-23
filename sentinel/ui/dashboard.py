"""Streamlit page for browsing + acting on incidents.

Layout:
- Left column: list of open incidents (compact). Click selects.
- Right column: detail of the selected incident -- error, diagnosis,
  proposed action, resolve/approve buttons.

The page is a thin client over the FastAPI service. Anything that
matters (allowlist, dispatch, store mutation) lives there; the page is
only display + form post.

This is intentionally not a "production" dashboard. It exists so a
reviewer can click through the agent's behavior on a chaos run without
SSH-ing into a Dagster instance.
"""

from __future__ import annotations

import os
from typing import Any

import httpx
import streamlit as st  # type: ignore[import-untyped]

API_URL = os.environ.get("SENTINEL_API_URL", "http://localhost:8000")


def _get(path: str, **params: Any) -> Any:
    r = httpx.get(f"{API_URL}{path}", params=params, timeout=10.0)
    r.raise_for_status()
    return r.json()


def _post(path: str, json: dict[str, Any] | None = None) -> Any:
    r = httpx.post(f"{API_URL}{path}", json=json or {}, timeout=10.0)
    r.raise_for_status()
    return r.json()


def render() -> None:
    st.set_page_config(page_title="Sentinel · Incidents", layout="wide")
    st.title("Sentinel · Incident Dashboard")
    st.caption(
        "Open incidents from the self-healing pipeline. "
        "Backend at " + API_URL + " (override with SENTINEL_API_URL)."
    )

    try:
        incidents = _get("/incidents", limit=50)
    except httpx.RequestError as e:
        st.error(f"could not reach API: {e}")
        return

    if not incidents:
        st.success("No open incidents. Nothing to look at right now.")
        return

    col_list, col_detail = st.columns([1, 2])

    with col_list:
        st.subheader(f"Open ({len(incidents)})")
        labels = [f"{r['asset_key'].rsplit('/', 1)[-1]}  ·  {r['error_type']}" for r in incidents]
        idx = st.radio(
            "Pick one",
            options=list(range(len(incidents))),
            format_func=lambda i: labels[i],
            label_visibility="collapsed",
        )

    selected = incidents[idx]
    try:
        detail = _get(f"/incidents/{selected['id']}")
    except httpx.HTTPStatusError as e:
        st.error(f"could not load incident {selected['id']}: {e}")
        return

    with col_detail:
        st.subheader(detail["asset_key"])
        st.caption(
            f"id `{detail['id']}` · created {detail['created_at']} · status **{detail['status']}**"
        )

        with st.expander("Failure", expanded=True):
            st.code(detail["error_type"] + ": " + detail["error_message"], language="text")
            if detail.get("upstream_lineage"):
                st.write("Upstream lineage:", detail["upstream_lineage"])

        report = detail.get("incident_report") or {}
        diagnosis = (report or {}).get("diagnosis") or {}
        action = (report or {}).get("proposed_action") or {}

        if diagnosis:
            with st.expander("Agent diagnosis", expanded=True):
                st.write("**Category:**", diagnosis.get("category"))
                st.write("**Root cause:**", diagnosis.get("root_cause"))
                st.write("**Proposed fix:**", diagnosis.get("proposed_fix"))
                st.write(
                    "**Confidence:**",
                    f"{(diagnosis.get('confidence') or 0):.2f}",
                )
                st.write(
                    "**Can auto-remediate?**",
                    "yes" if diagnosis.get("can_auto_remediate") else "no",
                )

        if action:
            with st.expander("Proposed action", expanded=True):
                st.write("**Status:**", action.get("status"))
                st.write("**Description:**", action.get("description"))
                if action.get("next_run"):
                    st.write("Next run:", action["next_run"])

        st.divider()
        c1, c2 = st.columns(2)

        with c1:
            if st.button("Resolve incident", type="secondary"):
                _post(f"/incidents/{detail['id']}/resolve")
                st.success("Resolved.")
                st.rerun()

        with c2:
            try:
                actions = _get("/allowlist")
            except httpx.HTTPStatusError:
                actions = []
            with st.form("approve"):
                action_choice = st.selectbox("Approve action", options=actions)
                note = st.text_input("Note (optional)", "")
                if st.form_submit_button("Apply"):
                    resp = _post(
                        f"/incidents/{detail['id']}/approve",
                        json={"action": action_choice, "note": note or None},
                    )
                    if resp["status"] == "applied":
                        st.success(resp["detail"])
                    elif resp["status"] == "off_allowlist":
                        st.error(resp["detail"])
                    else:
                        st.warning(resp["detail"])


if __name__ == "__main__":
    render()
