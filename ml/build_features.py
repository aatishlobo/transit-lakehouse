"""Build the training table for the prediction-correction model.

WHAT IS BEING PREDICTED
-----------------------
Not "when will the bus arrive" from scratch. The agency already answers that,
and yesterday's measurement showed its answer is systematically OPTIMISTIC in a
way that grows with horizon (-11s at 2 min out, -142s at 20+ min). Systematic
structure is learnable, so the task is:

    target = actual_arrival - predicted_arrival        ("residual", seconds)

Positive residual means the vehicle arrived LATER than the agency said. A model
that predicts this residual is a CORRECTION applied on top of the agency's ETA.

THIS IS A PREDICTION-CORRECTION MODEL, SAID LOUDLY
--------------------------------------------------
CLAUDE.md 7.3 warns that using the agency's own prediction as a feature is
normally leakage: if arrival labels were themselves derived from predictions
(resolver C), the model would just learn to copy the agency and post an
excellent, meaningless MAE. It permits the agency prediction as a feature only
when deliberately building a prediction-correction model -- "in which case say
so loudly."

So, loudly: this IS that model. It is safe here for a structural reason, not a
hopeful one --

    labels   come from VehiclePositions (current_status == STOPPED_AT)
    features come from TripUpdates      (the agency's forecast)

Two different feeds. The label cannot contain the feature. Resolver C is not
used anywhere in this project, and on this feed it is barely usable anyway
(only 4 of 28 operators emit settled predictions).

LEAKAGE RULES ENFORCED HERE
---------------------------
Every feature must be knowable at the moment the prediction was ISSUED.

The subtle one, and it is genuinely easy to miss: `lead_time` -- used in
yesterday's *evaluation* -- is (actual_arrival - issued). That contains the
actual arrival, i.e. the target. Using it as a feature would leak the answer
and produce a spectacular, worthless model. The legitimate substitute is

    horizon_s = predicted_arrival - issued

which is what the agency itself claimed, and is knowable at issue time.

TWO PASSES, AND WHY
-------------------
Six days of TripUpdates is ~47M rows and does not fit in memory. But we only
need predictions for stops that actually produced a resolved arrival.

  Pass 1 -- VehiclePositions only (~1,900 rows/poll). Resolve arrivals, collect
           the target grain set.
  Pass 2 -- TripUpdates, keeping ONLY predictions matching that set.

This bounds memory by (arrivals x predictions-per-arrival) instead of by total
feed volume.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingest.poller.decode import decode
from streaming.consumer import ArrivalResolver, Stats

log = logging.getLogger("features")

FEATURE_COLUMNS = [
    # --- identifiers (not features; kept for splitting and auditing)
    "service_date", "trip_id", "route_id", "stop_sequence", "stop_id",
    "issued_epoch", "actual_epoch", "predicted_epoch",
    # --- features, all knowable at issue time
    "horizon_s",            # predicted_arrival - issued. The agency's claim.
    "hour",                 # local hour at issue time
    "dow",                  # day of week at issue time
    "stop_sequence_f",      # how far into the trip
    "agency_delay_s",       # agency's own delay estimate at this stop, or blank
    "agency_delay_known",   # 1/0 -- absent delay is NOT zero (pitfall 2.1)
    "uncertainty",          # agency's stated uncertainty, or blank
    "uncertainty_known",
    "direction_id",
    "n_stops_in_update",    # size of the trip's update
    # --- target
    "residual_s",           # actual - predicted. Positive = arrived later.
]


def poll_ts(p: Path) -> int:
    return int(p.name.split("-", 1)[0])


def resolve_arrivals(root: Path, agency: str) -> dict[tuple, int]:
    """Pass 1: grain -> actual arrival epoch."""
    polls = sorted((root / "raw" / "vehicle_positions").rglob("*.pb.gz"), key=poll_ts)
    log.info("pass 1: resolving arrivals from %d vehicle_position polls", len(polls))
    r = ArrivalResolver(Stats())
    out: dict[tuple, int] = {}
    for i, p in enumerate(polls):
        rows, _ = decode(gzip.decompress(p.read_bytes()), "vehicle_positions", 120)
        for row in rows:
            tid = row.get("trip_id") or ""
            if agency and not tid.startswith(agency + ":"):
                continue
            a = r.process({
                "envelope": {"poll_interval_s": row.get("poll_interval_s")},
                "trip": {k: row.get(k) for k in
                         ("trip_id", "route_id", "service_date", "direction_id")},
                "vehicle_id": row.get("vehicle_id"),
                "vehicle_report_ts": row.get("vehicle_report_ts"),
                "vehicle_report_ts_epoch": row.get("vehicle_report_ts_epoch"),
                "current_stop_sequence": row.get("current_stop_sequence"),
                "current_stop_id": row.get("current_stop_id"),
                "current_status": row.get("current_status"),
            })
            if a:
                out[(a["service_date"], a["trip_id"], a["stop_sequence"])] = \
                    a["actual_arrival_ts_epoch"]
        if (i + 1) % 100 == 0:
            log.info("  %d/%d polls, %d arrivals", i + 1, len(polls), len(out))
    log.info("pass 1 complete: %d arrivals", len(out))
    return out


def build_rows(root: Path, arrivals: dict[tuple, int], agency: str) -> list[dict]:
    """Pass 2: pair each prediction with its eventual actual arrival."""
    polls = sorted((root / "raw" / "trip_updates").rglob("*.pb.gz"), key=poll_ts)
    log.info("pass 2: scanning %d trip_update polls", len(polls))

    seen: set[tuple] = set()   # dedupe identical (grain, issued, predicted)
    rows: list[dict] = []
    dropped_nonpositive_horizon = 0

    for i, p in enumerate(polls):
        decoded, _ = decode(gzip.decompress(p.read_bytes()), "trip_updates", 120)
        for d in decoded:
            tid = d.get("trip_id") or ""
            if agency and not tid.startswith(agency + ":"):
                continue
            seq = d.get("stop_sequence")
            if seq is None:
                continue
            key = (d.get("service_date"), tid, seq)
            actual = arrivals.get(key)
            if actual is None:
                continue

            predicted = d.get("arrival_time_epoch")
            issued = d.get("trip_update_ts_epoch") or d.get("feed_header_ts_epoch")
            if not predicted or not issued:
                continue

            # A "prediction" issued at or after the vehicle already arrived is
            # hindsight, not forecast. Excluded here for the same reason it is
            # excluded from evaluation -- training on it would teach the model
            # that the answer is already known.
            if issued >= actual:
                continue

            horizon = predicted - issued
            if horizon <= 0:
                dropped_nonpositive_horizon += 1
                continue

            dedupe = (key, issued, predicted)
            if dedupe in seen:
                continue
            seen.add(dedupe)

            local = datetime.fromtimestamp(issued, tz=timezone.utc).astimezone()
            delay = d.get("arrival_delay_s")
            unc = d.get("arrival_uncertainty")

            rows.append({
                "service_date": d.get("service_date") or "",
                "trip_id": tid,
                "route_id": d.get("route_id") or "",
                "stop_sequence": seq,
                "stop_id": d.get("stop_id") or "",
                "issued_epoch": issued,
                "actual_epoch": actual,
                "predicted_epoch": predicted,
                "horizon_s": horizon,
                "hour": local.hour,
                "dow": local.weekday(),
                "stop_sequence_f": seq,
                # Absent delay stays BLANK, never 0. Carrying a separate
                # _known flag lets the model distinguish "on time" from "no
                # information" instead of conflating them (pitfall 2.1).
                "agency_delay_s": "" if delay is None else delay,
                "agency_delay_known": 0 if delay is None else 1,
                "uncertainty": "" if unc is None else unc,
                "uncertainty_known": 0 if unc is None else 1,
                "direction_id": d.get("direction_id") if d.get("direction_id") is not None else "",
                "n_stops_in_update": d.get("n_stop_time_updates") or 0,
                "residual_s": actual - predicted,
            })
        if (i + 1) % 100 == 0:
            log.info("  %d/%d polls, %d rows", i + 1, len(polls), len(rows))

    log.info("pass 2 complete: %d rows (%d dropped for non-positive horizon)",
             len(rows), dropped_nonpositive_horizon)
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", default="data")
    ap.add_argument("--agency", default="SF")
    ap.add_argument("--out", default="ml/data/features.csv")
    ap.add_argument("--max-horizon-s", type=int, default=3600,
                    help="drop absurd horizons (feed glitches)")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(message)s")

    root = Path(args.data_root)
    arrivals = resolve_arrivals(root, args.agency)
    if not arrivals:
        log.error("no arrivals resolved -- is there an archive under %s?", root)
        return 1

    rows = build_rows(root, arrivals, args.agency)
    rows = [r for r in rows if r["horizon_s"] <= args.max_horizon_s]
    if not rows:
        log.error("no feature rows produced")
        return 1

    rows.sort(key=lambda r: r["issued_epoch"])

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FEATURE_COLUMNS)
        w.writeheader()
        w.writerows(rows)

    resid = sorted(r["residual_s"] for r in rows)
    n = len(resid)
    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "agency": args.agency,
        "rows": n,
        "arrivals_resolved": len(arrivals),
        "distinct_trips": len({r["trip_id"] for r in rows}),
        "distinct_routes": len({r["route_id"] for r in rows}),
        "time_span": {
            "first_issued": datetime.fromtimestamp(rows[0]["issued_epoch"]).isoformat(),
            "last_issued": datetime.fromtimestamp(rows[-1]["issued_epoch"]).isoformat(),
        },
        "target_residual_s": {
            "mean": round(sum(resid) / n, 1),
            "median": resid[n // 2],
            "p10": resid[int(0.10 * n)],
            "p90": resid[int(0.90 * n)],
            "min": resid[0],
            "max": resid[-1],
        },
        "label_provenance": "VehiclePositions STOPPED_AT (resolver A)",
        "feature_provenance": "TripUpdates (agency forecast)",
        "leakage_note": (
            "Features are restricted to values knowable at issue time. "
            "lead_time (actual - issued) is deliberately EXCLUDED because it "
            "contains the target; horizon_s (predicted - issued) is used "
            "instead. Labels and features come from different feeds."
        ),
    }
    Path(str(out).replace(".csv", "_meta.json")).write_text(json.dumps(meta, indent=2))

    log.info("wrote %s (%d rows)", out, n)
    print(json.dumps(meta, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
