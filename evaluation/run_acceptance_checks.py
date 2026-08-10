"""Acceptance checks -- the validation evidence for this pipeline.

These are not unit tests. Unit tests assert that a function behaves on inputs
the author imagined. These assert that properties hold across the ACTUAL data
the pipeline produced, which is where the interesting failures live.

EVERY CHECK CARRIES A ROW-COUNT FLOOR.
A check that passes on an empty table is worse than no check: it reports green
while measuring nothing, and it will keep reporting green after an upstream
break empties the input. So each check below fails if it was handed too little
data to be meaningful, and says so distinctly from a real failure.

Run:  python evaluation/run_acceptance_checks.py
      python evaluation/run_acceptance_checks.py --json

Requires the pipeline to have been run first (see README). Does NOT require
Kafka to be running -- checks read the produced artifacts and re-derive from the
committed sample, so the evidence is reproducible offline.
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingest.poller.decode import decode
from streaming.consumer import ArrivalResolver, Stats
from streaming.contracts import from_decoded_row

# Floors below which a check is considered unmeasured rather than passed.
MIN_ARRIVALS = 100
MIN_ROWS = 10_000
MIN_POLLS = 10


@dataclass
class Result:
    name: str
    passed: bool
    detail: str
    measured: dict = field(default_factory=dict)
    unmeasured: bool = False
    # Informational rows describe the data rather than assert a property.
    # They are reported but never counted as passes -- a check that asserts
    # nothing must not inflate a green tally.
    informational: bool = False

    @property
    def status(self) -> str:
        if self.informational:
            return "INFO"
        if self.unmeasured:
            return "UNMEASURED"
        return "PASS" if self.passed else "FAIL"


def _load_arrivals(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(l) for l in path.open() if l.strip()]


def _sample_polls(root: Path, feed: str) -> list[Path]:
    base = root / "raw" / feed
    return sorted(base.rglob("*.pb.gz")) if base.exists() else []


# --------------------------------------------------------------------------
# Checks
# --------------------------------------------------------------------------

def check_grain_uniqueness(arrivals: list[dict]) -> Result:
    """One arrival per (service_date, trip_id, stop_sequence).

    A duplicate here means the same real-world event was counted twice, which
    inflates every downstream metric silently.
    """
    if len(arrivals) < MIN_ARRIVALS:
        return Result("grain_uniqueness", False,
                      f"only {len(arrivals)} arrivals (floor {MIN_ARRIVALS})",
                      unmeasured=True)
    keys = [(a.get("service_date"), a.get("trip_id"), a.get("stop_sequence"))
            for a in arrivals]
    dupes = [k for k, n in Counter(keys).items() if n > 1]
    return Result(
        "grain_uniqueness",
        not dupes,
        f"{len(arrivals)} arrivals, {len(set(keys))} distinct keys, "
        f"{len(dupes)} duplicated",
        {"arrivals": len(arrivals), "distinct": len(set(keys)),
         "duplicates": len(dupes)},
    )


def check_idempotent_resolution(root: Path) -> Result:
    """Re-deriving arrivals from identical input must give identical output.

    Delivery is at-least-once, so the same vehicle position can legitimately
    be processed twice. If that changed the output, replays and crash recovery
    would silently corrupt the arrival table (pitfall 3.7).
    """
    polls = _sample_polls(root, "vehicle_positions")
    if len(polls) < MIN_POLLS:
        return Result("idempotent_resolution", False,
                      f"only {len(polls)} polls (floor {MIN_POLLS})",
                      unmeasured=True)

    def run() -> list[tuple]:
        r = ArrivalResolver(Stats())
        out = []
        for p in polls:
            rows, _ = decode(gzip.decompress(p.read_bytes()),
                             "vehicle_positions", 120)
            for row in rows:
                a = r.process({
                    "envelope": {k: row.get(k) for k in
                                 ("poll_interval_s", "decoder_version")},
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
                    out.append((a["service_date"], a["trip_id"],
                                a["stop_sequence"], a["actual_arrival_ts_epoch"]))
        return out

    a, b = run(), run()
    return Result(
        "idempotent_resolution",
        a == b and len(a) >= MIN_ARRIVALS,
        f"run1={len(a)} run2={len(b)} identical={a == b}",
        {"run1": len(a), "run2": len(b), "identical": a == b},
    )


def check_null_zero_discrimination(root: Path) -> Result:
    """No delay field may hold 0 where the source field was absent.

    This is the project's founding invariant (pitfall 2.1). Measured on the
    live feed, 44% of StopTimeUpdate rows have NO delay -- reporting those as
    0 would fabricate a huge spike of on-time records.

    Verified by decoding the raw protobuf and comparing presence directly
    against the decoded output, rather than trusting the decoder's own claim.
    """
    from google.transit import gtfs_realtime_pb2 as pb

    polls = _sample_polls(root, "trip_updates")
    if len(polls) < MIN_POLLS:
        return Result("null_zero_discrimination", False,
                      f"only {len(polls)} polls (floor {MIN_POLLS})",
                      unmeasured=True)

    absent = present_zero = present_nonzero = violations = 0
    for p in polls[:5]:
        raw = gzip.decompress(p.read_bytes())
        feed = pb.FeedMessage()
        feed.ParseFromString(raw)
        rows, _ = decode(raw, "trip_updates", 120)
        it = iter(rows)
        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue
            tu = entity.trip_update
            stus = list(tu.stop_time_update) or [None]
            for stu in stus:
                row = next(it, None)
                if row is None:
                    break
                if stu is None or not stu.HasField("arrival"):
                    truth = None
                elif not stu.arrival.HasField("delay"):
                    truth = None
                else:
                    truth = stu.arrival.delay
                got = row.get("arrival_delay_s")
                if truth is None:
                    absent += 1
                    if got is not None:
                        violations += 1
                else:
                    if truth == 0:
                        present_zero += 1
                    else:
                        present_nonzero += 1
                    if got != truth:
                        violations += 1

    total = absent + present_zero + present_nonzero
    if total < MIN_ROWS:
        return Result("null_zero_discrimination", False,
                      f"only {total} rows compared (floor {MIN_ROWS})",
                      unmeasured=True)
    return Result(
        "null_zero_discrimination",
        violations == 0,
        f"{total} rows compared against raw protobuf presence: "
        f"{absent} absent, {present_zero} explicit-zero, "
        f"{present_nonzero} nonzero, {violations} violations",
        {"absent": absent, "explicit_zero": present_zero,
         "nonzero": present_nonzero, "violations": violations,
         "pct_absent": round(100 * absent / total, 1)},
    )


def check_contract_validation(root: Path) -> Result:
    """Every decoded row must satisfy the published contract."""
    polls = _sample_polls(root, "trip_updates")[:3]
    if not polls:
        return Result("contract_validation", False, "no polls found",
                      unmeasured=True)
    ok = bad = 0
    errs: Counter = Counter()
    for p in polls:
        rows, _ = decode(gzip.decompress(p.read_bytes()), "trip_updates", 120)
        for row in rows:
            try:
                from_decoded_row(row)
                ok += 1
            except Exception as e:
                bad += 1
                errs[type(e).__name__] += 1
    if ok + bad < MIN_ROWS:
        return Result("contract_validation", False,
                      f"only {ok + bad} rows (floor {MIN_ROWS})", unmeasured=True)
    return Result("contract_validation", bad == 0,
                  f"{ok} valid, {bad} invalid {dict(errs) if errs else ''}",
                  {"valid": ok, "invalid": bad})


def check_provenance_complete(arrivals: list[dict]) -> Result:
    """Every arrival must say how it was derived, and with what precision."""
    if len(arrivals) < MIN_ARRIVALS:
        return Result("provenance_complete", False,
                      f"only {len(arrivals)} arrivals", unmeasured=True)
    required = ("arrival_method", "arrival_confidence", "poll_interval_s",
                "resolver_version")
    missing = Counter()
    for a in arrivals:
        for f in required:
            if a.get(f) is None:
                missing[f] += 1
    return Result("provenance_complete", not missing,
                  f"{len(arrivals)} arrivals; missing fields: "
                  f"{dict(missing) if missing else 'none'}",
                  {"arrivals": len(arrivals), "missing": dict(missing)})


def check_event_time_not_ingest_time(arrivals: list[dict]) -> Result:
    """Arrival times must come from the vehicle, not our clock.

    Detected by comparing against resolved_at: if arrival timestamps tracked
    our processing time, the gap would be near zero and near-constant.
    """
    if len(arrivals) < MIN_ARRIVALS:
        return Result("event_time_not_ingest_time", False,
                      f"only {len(arrivals)} arrivals", unmeasured=True)
    same = sum(1 for a in arrivals
               if (a.get("actual_arrival_ts") or "")[:19] ==
                  (a.get("resolved_at") or "")[:19])
    distinct_ts = len({a.get("actual_arrival_ts_epoch") for a in arrivals})
    return Result(
        "event_time_not_ingest_time",
        same == 0 and distinct_ts > MIN_ARRIVALS // 2,
        f"{same} arrivals share a timestamp with resolved_at; "
        f"{distinct_ts} distinct event times across {len(arrivals)} arrivals",
        {"collisions": same, "distinct_event_times": distinct_ts},
    )


def check_grain_uses_sequence_not_stop_id(arrivals: list[dict]) -> Result:
    """Loop routes must produce two arrivals for one stop_id in a trip.

    If this finds zero, the grain choice is untested by the data rather than
    wrong -- reported honestly as such.
    """
    if len(arrivals) < MIN_ARRIVALS:
        return Result("loop_routes_resolved_separately", False,
                      f"only {len(arrivals)} arrivals", unmeasured=True)
    per_trip: dict = defaultdict(lambda: defaultdict(set))
    for a in arrivals:
        if a.get("stop_id") and a.get("stop_sequence") is not None:
            per_trip[(a.get("service_date"), a.get("trip_id"))][a["stop_id"]].add(
                a["stop_sequence"]
            )
    loops = sum(1 for stops in per_trip.values()
                if any(len(s) > 1 for s in stops.values()))
    # Reported, not asserted. With ~21% stop capture, catching BOTH visits of
    # a loop within a 39-minute window is rare, so zero here means the case is
    # unexercised by this sample -- not that the grain is wrong. Profiling the
    # full feed found 7 operators running such trips; tests/test_resolver.py
    # asserts the behaviour directly.
    return Result(
        "loop_route_revisits_observed", True,
        f"{loops} of {len(per_trip)} trips produced multiple arrivals at one "
        f"stop_id in this sample (informational: a stop_id grain would collapse "
        f"these; behaviour is asserted in tests/test_resolver.py)",
        {"trips_with_revisits": loops, "trips": len(per_trip)},
        informational=True,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", default="data/replay_sample")
    ap.add_argument("--arrivals", default="outputs/arrival_events.jsonl")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--out", default="evaluation/acceptance_report.json")
    args = ap.parse_args()

    root = Path(args.sample)
    arrivals = _load_arrivals(Path(args.arrivals))

    results = [
        check_null_zero_discrimination(root),
        check_contract_validation(root),
        check_grain_uniqueness(arrivals),
        check_idempotent_resolution(root),
        check_provenance_complete(arrivals),
        check_event_time_not_ingest_time(arrivals),
        check_grain_uses_sequence_not_stop_id(arrivals),
    ]

    report = {
        "checks": [
            {"name": r.name, "status": r.status, "detail": r.detail,
             "measured": r.measured}
            for r in results
        ],
        "passed": sum(1 for r in results
                      if r.passed and not r.unmeasured and not r.informational),
        "failed": sum(1 for r in results
                      if not r.passed and not r.unmeasured and not r.informational),
        "unmeasured": sum(1 for r in results
                          if r.unmeasured and not r.informational),
        "informational": sum(1 for r in results if r.informational),
    }
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(report, indent=2))

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print("=" * 72)
        print("ACCEPTANCE CHECKS")
        print("=" * 72)
        for r in results:
            mark = {"PASS": "PASS", "FAIL": "FAIL",
                    "UNMEASURED": "----", "INFO": "info"}[r.status]
            print(f"[{mark}] {r.name}")
            print(f"       {r.detail}")
        print("=" * 72)
        print(f"passed={report['passed']}  failed={report['failed']}  "
              f"unmeasured={report['unmeasured']}  info={report['informational']}")
        print(f"report written to {args.out}")

    # Unmeasured is a failure: a check that could not run is not a check that
    # passed.
    return 0 if (report["failed"] == 0 and report["unmeasured"] == 0) else 1


if __name__ == "__main__":
    raise SystemExit(main())
