"""Seed a tiny TLC sample into data/raw/ for local development.

This is for poking at parquet files without firing up the full stack.
For an actual pipeline run, use ``make demo`` which goes through Dagster.
"""

from __future__ import annotations

import sys
from pathlib import Path

import httpx

from sentinel.ingest import tlc

OUT = Path("data/raw/tlc")


def main(year: int = 2024, month: int = 1) -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    target = OUT / f"yellow_tripdata_{year:04d}-{month:02d}.parquet"
    if target.exists():
        print(f"already present: {target}")
        return 0

    print(f"fetching {tlc.yellow_url(year, month)}")
    try:
        data = tlc.fetch_yellow_tripdata(year, month)
    except httpx.HTTPError as exc:
        print(f"fetch failed: {exc}", file=sys.stderr)
        return 1

    target.write_bytes(data)
    print(f"wrote {target} ({len(data):,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
