"""dbt resource wiring for Dagster.

The DbtCliResource needs a project_dir and profiles_dir. We point at the
in-repo dbt/ directory; same paths work locally and in the container because
of the bind mount.
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

from dagster_dbt import DbtCliResource, DbtProject

# Resolve relative to the dagster working dir. In containers this is
# /opt/dagster/app; locally it's the repo root.
_DBT_DIR = Path(__file__).resolve().parents[2] / "dbt"

dbt_project = DbtProject(
    project_dir=_DBT_DIR,
    profiles_dir=_DBT_DIR,
)
dbt_project.prepare_if_dev()


def _find_dbt() -> str:
    """Locate dbt without leaning on the caller's PATH.

    DbtCliResource validates the executable at construction time, which
    breaks when pytest is invoked directly from a venv bin without
    activating it. Fall back to the venv next to the running python.
    """
    found = shutil.which("dbt")
    if found:
        return found
    candidate = Path(sys.executable).parent / "dbt"
    if candidate.exists():
        return str(candidate)
    return "dbt"  # let DbtCliResource raise; this isn't supposed to happen in prod


dbt_resource = DbtCliResource(project_dir=dbt_project, dbt_executable=_find_dbt())
