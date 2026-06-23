"""Streamlit incident dashboard.

Run with::

    streamlit run sentinel/ui/dashboard.py

Single-page UI that talks to the FastAPI service at ``SENTINEL_API_URL``
(default ``http://localhost:8000``). The split (API + UI as separate
processes) is overkill for one user but it's the demo-friendly shape:
a reviewer can curl the API to see how the dashboard's buttons
translate into the same allowlisted remediation that the agent uses.
"""
