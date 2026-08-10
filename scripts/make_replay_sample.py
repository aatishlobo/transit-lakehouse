"""Cut a committed replay sample from the live archive.

This produces the ONLY data that ships in the course submission. The reviewer
runs the whole pipeline from these bytes with no API key, so the sample has to
be self-contained, small enough to commit, and representative enough that the
output means something.

Three decisions are baked in here.

1. RAW PROTOBUF, NOT DECODED JSON.
   Shipping decoded rows would be smaller and easier. It would also bypass
   decode.py -- the most correctness-critical file in the repo -- so the
   reviewer would never exercise the absent-vs-zero presence logic that the
   whole project is built around. The sample must enter the pipeline at the
   same door live data does.

2. A CONTIGUOUS WINDOW, NOT A RANDOM SAMPLE.
   The arrival resolver works by watching a vehicle move THROUGH states across
   consecutive polls (approaching -> STOPPED_AT -> departed). Randomly sampled
   polls would destroy those sequences and the resolver would find nothing.
   Contiguity is a correctness requirement, not a nicety.

3. GAP-CHECKED.
   A window with a hole in it silently produces missed arrivals, which look
   identical to "this agency doesn't report arrivals." The selector refuses
   windows whose gaps exceed a threshold.

Usage:
    python scripts/make_replay_sample.py --polls 20
    python scripts/make_replay_sample.py --polls 20 --dry-run
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import shutil
from pathlib import Path

FEEDS = ("trip_updates", "vehicle_positions")


def poll_ts(p: Path) -> int:
    """Filename is {poll_epoch}-{sha8}.pb.gz."""
    return int(p.name.split("-", 1)[0])


def contiguous_runs(paths: list[Path], max_gap_s: int) -> list[list[Path]]:
    """Split polls into runs where consecutive polls are <= max_gap_s apart."""
    if not paths:
        return []
    ordered = sorted(paths, key=poll_ts)
    runs, cur = [], [ordered[0]]
    for prev, nxt in zip(ordered, ordered[1:]):
        if poll_ts(nxt) - poll_ts(prev) <= max_gap_s:
            cur.append(nxt)
        else:
            runs.append(cur)
            cur = [nxt]
    runs.append(cur)
    return runs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", default="data")
    ap.add_argument("--out", default="data/replay_sample")
    ap.add_argument("--polls", type=int, default=20, help="polls per feed")
    ap.add_argument(
        "--max-gap-s",
        type=int,
        default=300,
        help="largest allowed gap between consecutive polls",
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    root = Path(args.data_root)
    out = Path(args.out)

    # Anchor on trip_updates: it is the larger, more failure-prone feed, and
    # vehicle_positions is then matched to the SAME wall-clock window. Picking
    # each feed's own best window independently would yield two windows that
    # don't overlap -- and the resolver needs both feeds for the same minutes.
    tu = list((root / "raw" / "trip_updates").rglob("*.pb.gz"))
    if not tu:
        print(f"no archive under {root}/raw/trip_updates")
        return 1

    runs = contiguous_runs(tu, args.max_gap_s)
    best = max(runs, key=len)
    if len(best) < args.polls:
        print(
            f"longest gap-free run is {len(best)} polls, fewer than the "
            f"{args.polls} requested.\n"
            f"Either lower --polls or archive more data."
        )
        return 1

    window = best[: args.polls]
    t0, t1 = poll_ts(window[0]), poll_ts(window[-1])

    # Match vehicle_positions to the same window by timestamp.
    vp = [
        p
        for p in (root / "raw" / "vehicle_positions").rglob("*.pb.gz")
        if t0 <= poll_ts(p) <= t1
    ]
    vp.sort(key=poll_ts)

    span_min = (t1 - t0) / 60
    total_bytes = sum(p.stat().st_size for p in window + vp)

    print(f"window   {dt.datetime.fromtimestamp(t0):%Y-%m-%d %H:%M} "
          f"-> {dt.datetime.fromtimestamp(t1):%H:%M}  ({span_min:.0f} min)")
    print(f"polls    trip_updates={len(window)}  vehicle_positions={len(vp)}")
    print(f"size     {total_bytes / 1024 / 1024:.1f} MB")

    if len(vp) < args.polls * 0.8:
        print(
            f"\nWARNING: only {len(vp)} vehicle_positions polls cover this "
            f"window. The resolver needs vehicle positions -- a sample without "
            f"them cannot produce arrivals."
        )

    if args.dry_run:
        return 0

    if out.exists():
        shutil.rmtree(out)

    manifest_files = []
    for feed, paths in (("trip_updates", window), ("vehicle_positions", vp)):
        for p in paths:
            # Preserve the ingest_dt partition so the sample is byte-identical
            # in layout to the live archive -- the replay producer then needs
            # no special case for it.
            dest = out / "raw" / feed / p.parent.name / p.name
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(p, dest)
            manifest_files.append(
                {"feed": feed, "path": str(dest.relative_to(out)),
                 "poll_epoch": poll_ts(p), "bytes": p.stat().st_size}
            )

    manifest = {
        "description": (
            "Contiguous GTFS-Realtime replay sample cut from the live 511 "
            "regional archive. Raw protobuf, exactly as received."
        ),
        "source": "511.org GTFS-Realtime, agency=RG (Bay Area regional feed)",
        "window_start_utc": dt.datetime.fromtimestamp(t0, dt.timezone.utc).isoformat(),
        "window_end_utc": dt.datetime.fromtimestamp(t1, dt.timezone.utc).isoformat(),
        "window_start_local": dt.datetime.fromtimestamp(t0).isoformat(),
        "window_end_local": dt.datetime.fromtimestamp(t1).isoformat(),
        "span_minutes": round(span_min, 1),
        "poll_interval_s": 120,
        "polls": {"trip_updates": len(window), "vehicle_positions": len(vp)},
        "max_gap_s_allowed": args.max_gap_s,
        "total_bytes": total_bytes,
        "files": manifest_files,
    }
    (out / "MANIFEST.json").write_text(json.dumps(manifest, indent=2))

    print(f"\nwrote {len(manifest_files)} files to {out}/")
    print(f"manifest: {out}/MANIFEST.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
