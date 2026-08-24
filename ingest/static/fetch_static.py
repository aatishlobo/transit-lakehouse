"""Fetch and archive the 511 regional GTFS-Static feed.

GTFS-Static is the published timetable: what the agency *said* would happen.
Combined with the derived arrivals in gold, it turns "prediction accuracy"
(actual vs predicted) into true **on-time performance** (actual vs scheduled).

Two properties drive the whole design here:

1. **The schedule changes.** Agencies publish new timetables continuously --
   service changes, holiday schedules, construction reroutes. A trip that ran
   on 2026-08-06 must be evaluated against the schedule that was in force on
   2026-08-06, not against whatever is current. Overwriting the schedule
   silently rewrites history for every past service date, which is the single
   most common way an OTP number becomes quietly wrong. Hence SCD2.

2. **It is fetched from 511, never from an agency directly.** The realtime
   feeds carry 511's trip_ids and stop_ids. An agency's own GTFS-Static is not
   guaranteed to use the same identifiers, so joining across the two sources
   produces a silent, partial join rather than an error.

Archive-before-parse, same as the realtime poller (invariant 3.9): the bytes
land on disk first, and parsing failures quarantine rather than lose.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests

STATIC_URL = "https://api.511.org/transit/datafeeds"
DEFAULT_OPERATOR = "RG"  # consolidated regional feed

# The tables we actually use. GTFS ships far more; pulling only what is needed
# keeps the SCD2 dimension honest about its own scope.
WANTED = {
    "trips.txt",
    "stop_times.txt",
    "stops.txt",
    "routes.txt",
    "calendar.txt",
    "calendar_dates.txt",
    "agency.txt",
    "feed_info.txt",
}


def redact(text: str, secret: str | None) -> str:
    """Never let the key reach a log. Mirrors ingest.poller.poller.redact."""
    import re

    if not text:
        return text
    if secret and len(secret) >= 8:
        text = text.replace(secret, "<REDACTED>")
    return re.sub(r"(api_key=)[^&\s\"')]+", r"\1<REDACTED>", text)


def fetch(api_key: str, operator: str = DEFAULT_OPERATOR, timeout: int = 180) -> bytes:
    try:
        resp = requests.get(
            STATIC_URL,
            params={"api_key": api_key, "operator_id": operator},
            timeout=timeout,
        )
        resp.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(redact(str(exc), api_key)) from exc

    body = resp.content
    if not body[:2] == b"PK":
        # 511 answers some errors with HTTP 200 and an HTML body. Parsing that
        # as a zip fails confusingly much later; fail here instead.
        raise RuntimeError(
            f"expected a zip, got {len(body)} bytes starting {body[:32]!r}"
        )
    return body


def archive(body: bytes, root: str = "data/static") -> Path:
    """Write the raw zip under its content hash.

    Content-addressed on purpose: re-fetching an unchanged schedule must be a
    no-op, not a new version. An SCD2 dimension that opens a new row every time
    a job runs is worse than no versioning at all -- it looks like the schedule
    changed daily when nothing changed.
    """
    digest = hashlib.sha256(body).hexdigest()
    fetched_at = datetime.now(timezone.utc)
    out_dir = Path(root) / f"fetched_dt={fetched_at:%Y-%m-%d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{digest[:16]}.zip"

    if path.exists():
        print(f"  unchanged: {path} already archived")
        return path

    path.write_bytes(body)
    meta = {
        "sha256": digest,
        "bytes": len(body),
        "fetched_at": fetched_at.isoformat(),
        "source": "511 regional GTFS-Static",
        "attribution": "Data provided by 511.org (MTC) -- http://www.511.org",
    }
    path.with_suffix(".meta.json").write_text(json.dumps(meta, indent=2))
    print(f"  archived {len(body):,} bytes -> {path}")
    return path


def extract(zip_path: Path, dest_root: str = "data/static/extracted") -> Path:
    """Extract the wanted tables next to the archived zip."""
    dest = Path(dest_root) / zip_path.stem
    if dest.exists() and any(dest.iterdir()):
        print(f"  already extracted: {dest}")
        return dest
    dest.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path) as zf:
        names = set(zf.namelist())
        missing = {"trips.txt", "stop_times.txt"} - names
        if missing:
            raise RuntimeError(f"feed is missing required tables: {missing}")
        for name in sorted(names & WANTED):
            zf.extract(name, dest)
            size = (dest / name).stat().st_size
            print(f"    {name:24s} {size:>12,} bytes")
    return dest


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--operator", default=DEFAULT_OPERATOR)
    ap.add_argument("--root", default="data/static")
    ap.add_argument("--no-extract", action="store_true")
    args = ap.parse_args()

    key = os.environ.get("API_511_KEY")
    if not key:
        env = Path(".env")
        if env.exists():
            for line in env.read_text().splitlines():
                if line.startswith("API_511_KEY="):
                    key = line.split("=", 1)[1].strip()
    if not key:
        raise SystemExit("API_511_KEY not set (env or .env)")

    print("GTFS-STATIC")
    body = fetch(key, args.operator)
    path = archive(body, args.root)
    if not args.no_extract:
        extract(path)


if __name__ == "__main__":
    main()
