"""
Week 0 feed profiler.

Reads the decoded archive and answers the questions that every subsequent
design decision depends on. Run this after ~3 hours of polling; the output
IS ADR-002.

Questions answered, per agency where the feed identifies one:

  Q1  Which arrival resolvers are even available?
      -> current_status / stop_id / current_stop_sequence population rates
  Q2  Do TripUpdates retain passed stops, and does uncertainty go to 0?
  Q3  Is the feed actually advancing, or serving stale payloads?
  Q4  What is the schedule_relationship distribution? (exclusion rules)
  Q5  What is the real fan-out ratio? (volume sizing, pitfall 2.4)
  Q6  How often is delay absent vs zero? (the pitfall 2.1 smoking gun)
  Q7  Do loop routes exist -- same stop_id twice in one trip? (grain, 7.6)

Usage:
    python profiling/profile_feed.py --data-root data
    python profiling/profile_feed.py --data-root data --json > profile.json
"""

from __future__ import annotations

import argparse
import gzip
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterator


def read_decoded(root: Path, feed_type: str) -> Iterator[dict]:
    base = root / "decoded" / feed_type
    if not base.exists():
        return
    for f in sorted(base.rglob("*.jsonl.gz")):
        with gzip.open(f, "rt") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    yield json.loads(line)


def _agency_of(row: dict) -> str:
    """Best-effort agency extraction.

    511's regional feed prefixes entity/trip IDs with an agency code. If that
    convention does not hold in practice, this collapses to 'ALL' and the
    profile is still useful -- just not split by agency.
    """
    for key in ("trip_id", "entity_id", "route_id"):
        val = row.get(key)
        if isinstance(val, str) and ":" in val:
            return val.split(":", 1)[0]
    return "ALL"


def _pct(n: int, d: int) -> float:
    return round(100.0 * n / d, 1) if d else 0.0


def profile_vehicle_positions(root: Path) -> dict:
    by_agency: dict[str, Counter] = defaultdict(Counter)
    status_dist: dict[str, Counter] = defaultdict(Counter)
    ts_seen: dict[str, set] = defaultdict(set)
    total = 0

    for row in read_decoded(root, "vehicle_positions"):
        total += 1
        ag = _agency_of(row)
        c = by_agency[ag]
        c["rows"] += 1
        for fld in (
            "current_status",
            "current_stop_id",
            "current_stop_sequence",
            "latitude",
            "vehicle_report_ts",
            "trip_id",
            "service_date",
            "vehicle_id",
        ):
            if row.get(fld) is not None:
                c[f"has_{fld}"] += 1
        if row.get("current_status"):
            status_dist[ag][row["current_status"]] += 1
        # Q3: does each vehicle's own clock advance?
        if row.get("vehicle_id") and row.get("vehicle_report_ts_epoch"):
            ts_seen[ag].add((row["vehicle_id"], row["vehicle_report_ts_epoch"]))

    out = {"total_rows": total, "by_agency": {}}
    for ag, c in sorted(by_agency.items()):
        n = c["rows"]
        stopped = status_dist[ag].get("STOPPED_AT", 0)
        out["by_agency"][ag] = {
            "rows": n,
            "pct_current_status": _pct(c["has_current_status"], n),
            "pct_stop_id": _pct(c["has_current_stop_id"], n),
            "pct_stop_sequence": _pct(c["has_current_stop_sequence"], n),
            "pct_position": _pct(c["has_latitude"], n),
            "pct_vehicle_ts": _pct(c["has_vehicle_report_ts"], n),
            "pct_trip_id": _pct(c["has_trip_id"], n),
            "pct_service_date": _pct(c["has_service_date"], n),
            "status_distribution": dict(status_dist[ag].most_common()),
            "pct_stopped_at": _pct(stopped, n),
            "distinct_vehicle_ts_pairs": len(ts_seen[ag]),
            # THE decision: can resolver A (agency's own arrival call) fire?
            "resolver_A_available": (
                _pct(c["has_current_status"], n) > 50
                and _pct(c["has_current_stop_sequence"], n) > 50
                and stopped > 0
            ),
        }
    return out


def profile_trip_updates(root: Path) -> dict:
    by_agency: dict[str, Counter] = defaultdict(Counter)
    stop_rel: dict[str, Counter] = defaultdict(Counter)
    trip_rel: dict[str, Counter] = defaultdict(Counter)
    fanout: dict[str, list] = defaultdict(list)
    seen_entities: dict[str, set] = defaultdict(set)
    # Q7: (agency, trip_id, service_date) -> {stop_id: set of stop_sequences}
    # NOTE: must track DISTINCT sequences, not observation counts -- the same
    # trip appears in every poll, so counting observations makes every trip
    # look like a loop. A real loop is one stop_id at two stop_sequences.
    trip_stops: dict[tuple, dict[str, set]] = defaultdict(lambda: defaultdict(set))
    total = 0

    for row in read_decoded(root, "trip_updates"):
        total += 1
        ag = _agency_of(row)
        c = by_agency[ag]
        c["rows"] += 1

        # Q6: the pitfall 2.1 smoking gun
        d = row.get("arrival_delay_s")
        if d is None:
            c["arrival_delay_absent"] += 1
        elif d == 0:
            c["arrival_delay_zero"] += 1
        else:
            c["arrival_delay_nonzero"] += 1

        u = row.get("arrival_uncertainty")
        if u is None:
            c["uncertainty_absent"] += 1
        elif u == 0:
            c["uncertainty_zero"] += 1  # observed, not predicted -> resolver C
        else:
            c["uncertainty_nonzero"] += 1

        for fld in ("arrival_time", "departure_time", "stop_sequence", "stop_id", "service_date"):
            if row.get(fld) is not None:
                c[f"has_{fld}"] += 1

        if row.get("stop_schedule_relationship"):
            stop_rel[ag][row["stop_schedule_relationship"]] += 1
        if row.get("trip_schedule_relationship"):
            trip_rel[ag][row["trip_schedule_relationship"]] += 1

        eid = row.get("entity_id")
        if eid and eid not in seen_entities[ag]:
            seen_entities[ag].add(eid)
            if row.get("n_stop_time_updates"):
                fanout[ag].append(row["n_stop_time_updates"])

        if row.get("trip_id") and row.get("stop_id") and row.get("stop_sequence") is not None:
            trip_stops[(ag, row["trip_id"], row.get("service_date"))][
                row["stop_id"]
            ].add(row["stop_sequence"])

    # Q7: loop detection -- one stop_id occupying two distinct stop_sequences
    # within the same trip. This is what breaks a stop_id-based grain.
    loops: Counter = Counter()
    trips_seen: Counter = Counter()
    for (ag, _trip, _sd), stops in trip_stops.items():
        trips_seen[ag] += 1
        if any(len(seqs) > 1 for seqs in stops.values()):
            loops[ag] += 1

    out = {"total_rows": total, "by_agency": {}}
    for ag, c in sorted(by_agency.items()):
        n = c["rows"]
        fo = fanout[ag]
        out["by_agency"][ag] = {
            "rows": n,
            "pct_arrival_time": _pct(c["has_arrival_time"], n),
            "pct_stop_sequence": _pct(c["has_stop_sequence"], n),
            "pct_service_date": _pct(c["has_service_date"], n),
            "delay": {
                "absent": c["arrival_delay_absent"],
                "zero": c["arrival_delay_zero"],
                "nonzero": c["arrival_delay_nonzero"],
                "pct_absent": _pct(c["arrival_delay_absent"], n),
                # If this is high, a naive decoder would have reported all of
                # them as on-time. This number is the value of pitfall 2.1.
            },
            "uncertainty": {
                "absent": c["uncertainty_absent"],
                "zero": c["uncertainty_zero"],
                "nonzero": c["uncertainty_nonzero"],
                "pct_zero": _pct(c["uncertainty_zero"], n),
            },
            "stop_schedule_relationship": dict(stop_rel[ag].most_common()),
            "trip_schedule_relationship": dict(trip_rel[ag].most_common()),
            "fanout": {
                "trip_updates_seen": len(fo),
                "mean_stops_per_update": round(sum(fo) / len(fo), 1) if fo else 0,
                "max_stops_per_update": max(fo) if fo else 0,
            },
            "trips_with_repeated_stop_id": loops.get(ag, 0),
            "trips_seen": trips_seen.get(ag, 0),
            # resolver C viability: are there settled (uncertainty=0) records?
            "resolver_C_available": c["uncertainty_zero"] > 0,
        }
    return out


def profile_freshness(root: Path) -> dict:
    """Q3: is the archive advancing, or are we storing the same bytes?"""
    out = {}
    for feed_type in ("trip_updates", "vehicle_positions"):
        base = root / "raw" / feed_type
        if not base.exists():
            continue
        files = sorted(base.rglob("*.pb.gz"))
        hashes = [f.name.split("-", 1)[1].split(".")[0] for f in files if "-" in f.name]
        out[feed_type] = {
            "polls_archived": len(files),
            "distinct_payloads": len(set(hashes)),
            "duplicate_payload_pct": _pct(len(hashes) - len(set(hashes)), len(hashes)),
        }
    return out


def render_text(prof: dict) -> str:
    L = []
    A = L.append
    A("=" * 72)
    A("GTFS-RT FEED PROFILE  --  Week 0 evidence for ADR-002")
    A("=" * 72)

    fr = prof.get("freshness", {})
    if fr:
        A("\n## Q3  Feed freshness")
        for ft, v in fr.items():
            A(
                f"  {ft:20s} polls={v['polls_archived']:5d}  "
                f"distinct={v['distinct_payloads']:5d}  "
                f"dup={v['duplicate_payload_pct']}%"
            )
            if v["duplicate_payload_pct"] > 20:
                A("      ^ high duplicate rate: polling faster than the feed updates")

    vp = prof.get("vehicle_positions", {})
    if vp.get("total_rows"):
        A(f"\n## Q1  Resolver availability  (vehicle_positions, n={vp['total_rows']})")
        A(f"  {'agency':10s} {'status%':>8s} {'stop_id%':>9s} {'seq%':>7s} "
          f"{'STOPPED_AT%':>12s}  resolver_A")
        for ag, v in vp["by_agency"].items():
            A(
                f"  {ag:10s} {v['pct_current_status']:>8} {v['pct_stop_id']:>9} "
                f"{v['pct_stop_sequence']:>7} {v['pct_stopped_at']:>12}  "
                f"{'YES' if v['resolver_A_available'] else 'no'}"
            )

    tu = prof.get("trip_updates", {})
    if tu.get("total_rows"):
        A(f"\n## Q6  Delay: absent vs zero  (trip_updates, n={tu['total_rows']})")
        A("  A naive decoder reports 'absent' rows as on-time. This is the cost.")
        A(f"  {'agency':10s} {'absent':>9s} {'zero':>8s} {'nonzero':>9s} {'absent%':>9s}")
        for ag, v in tu["by_agency"].items():
            d = v["delay"]
            A(
                f"  {ag:10s} {d['absent']:>9} {d['zero']:>8} "
                f"{d['nonzero']:>9} {d['pct_absent']:>9}"
            )

        A("\n## Q2  Settled predictions (uncertainty=0 => observed, resolver C)")
        for ag, v in tu["by_agency"].items():
            u = v["uncertainty"]
            A(
                f"  {ag:10s} zero={u['zero']:>7} ({u['pct_zero']}%)  "
                f"resolver_C={'YES' if v['resolver_C_available'] else 'no'}"
            )

        A("\n## Q4  schedule_relationship distribution (exclusion rules)")
        for ag, v in tu["by_agency"].items():
            A(f"  {ag:10s} stop={v['stop_schedule_relationship'] or '{}'}")
            A(f"  {'':10s} trip={v['trip_schedule_relationship'] or '{}'}")

        A("\n## Q5  Fan-out (volume sizing)")
        for ag, v in tu["by_agency"].items():
            f = v["fanout"]
            A(
                f"  {ag:10s} mean={f['mean_stops_per_update']} "
                f"max={f['max_stops_per_update']} stops/update"
            )

        A("\n## Q7  Loop routes (grain decision: stop_sequence vs stop_id)")
        for ag, v in tu["by_agency"].items():
            n = v["trips_with_repeated_stop_id"]
            tot = v.get("trips_seen", 0)
            flag = "  <-- grain MUST be stop_sequence" if n else ""
            A(f"  {ag:10s} {n}/{tot} trips revisit a stop_id{flag}")

    A("\n" + "=" * 72)
    return "\n".join(L)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", default="data")
    ap.add_argument("--json", action="store_true", help="emit raw JSON")
    args = ap.parse_args()

    root = Path(args.data_root)
    if not (root / "decoded").exists():
        print(
            f"No decoded archive at {root/'decoded'}.\n"
            "Run the poller first:  python -m ingest.poller.poller --once"
        )
        return 1

    prof = {
        "freshness": profile_freshness(root),
        "vehicle_positions": profile_vehicle_positions(root),
        "trip_updates": profile_trip_updates(root),
    }

    if args.json:
        print(json.dumps(prof, indent=2))
    else:
        print(render_text(prof))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
