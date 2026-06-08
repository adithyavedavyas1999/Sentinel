"""sentinel CLI.

Two subcommands today: `incident list` and `chaos inject`. Wire-up is
typer because the future agent commands will want completions and rich
help, and that's typer's strength.
"""

from __future__ import annotations

import json
import sys

import typer

from sentinel.quality.incidents import IncidentStore

app = typer.Typer(help="Sentinel ops CLI", no_args_is_help=True)
incident_app = typer.Typer(help="Inspect captured incidents.")
chaos_app = typer.Typer(help="Deliberately break things. Phase 2 eval harness.")
app.add_typer(incident_app, name="incident")
app.add_typer(chaos_app, name="chaos")


@incident_app.command("list")
def incident_list(
    limit: int = typer.Option(20, "--limit", "-n", help="rows to show"),
    json_out: bool = typer.Option(False, "--json", help="emit json instead of table"),
):
    store = IncidentStore()
    rows = store.list_open(limit=limit)
    if json_out:
        typer.echo(json.dumps(rows, indent=2, default=str))
        return
    if not rows:
        typer.echo("no open incidents.")
        return
    for r in rows:
        typer.echo(
            f"{r['id'][:8]}  {r['created_at']}  {r['asset_key']:35s}  "
            f"{r['error_type']:20s}  {r['error_message'][:60]}"
        )


@incident_app.command("show")
def incident_show(incident_id: str):
    store = IncidentStore()
    row = store.get(incident_id)
    if row is None:
        typer.echo(f"no incident with id {incident_id}", err=True)
        raise typer.Exit(code=1)
    typer.echo(json.dumps(row, indent=2, default=str))


@incident_app.command("resolve")
def incident_resolve(incident_id: str):
    store = IncidentStore()
    if store.get(incident_id) is None:
        typer.echo(f"no incident with id {incident_id}", err=True)
        raise typer.Exit(code=1)
    store.resolve(incident_id)
    typer.echo(f"resolved {incident_id}")


@chaos_app.command("list")
def chaos_list(
    active: bool = typer.Option(False, "--active", help="only show currently-tripped flags"),
):
    from sentinel.chaos import scenarios
    from sentinel.chaos import state as chaos_state

    if active:
        rows = chaos_state.list_active()
        if not rows:
            typer.echo("no active chaos flags.")
            return
        for r in rows:
            typer.echo(f"{r['name']:25s}  set_at={r['set_at']}")
        return

    for s in scenarios():
        typer.echo(s)


@chaos_app.command("inject")
def chaos_inject(
    scenario: str = typer.Argument(..., help="see `sentinel chaos list`"),
    dry_run: bool = typer.Option(False, "--dry-run", help="print the plan, don't apply"),
):
    from sentinel.chaos import run as run_scenario
    from sentinel.chaos import scenarios

    if scenario not in scenarios():
        typer.echo(f"unknown scenario: {scenario}", err=True)
        typer.echo("available: " + ", ".join(scenarios()), err=True)
        raise typer.Exit(code=1)

    typer.echo(f"injecting scenario: {scenario}  dry_run={dry_run}")
    rc = run_scenario(scenario, dry_run=dry_run)
    if rc != 0:
        raise typer.Exit(code=rc)


@chaos_app.command("clear")
def chaos_clear(
    scenario: str = typer.Argument(..., help="scenario to reverse"),
):
    from sentinel.chaos import clear as clear_scenario

    rc = clear_scenario(scenario)
    if rc == 0:
        typer.echo(f"cleared {scenario}")
    else:
        typer.echo(f"nothing to clear for {scenario}", err=True)
        raise typer.Exit(code=rc)


@chaos_app.command("clear-all")
def chaos_clear_all():
    from sentinel.chaos import clear_all

    clear_all()
    typer.echo("cleared all chaos state.")


def main() -> None:
    app()


if __name__ == "__main__":
    sys.exit(main())  # type: ignore[func-returns-value]
