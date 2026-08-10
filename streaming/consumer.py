"""Arrival resolver: vehicle positions -> derived arrival events.

This is where the project stops moving data around and starts producing the
fact GTFS-Realtime refuses to state: "vehicle X reached stop Y at time T."

RESOLVER A -- POSITION STATE TRANSITION
---------------------------------------
A vehicle reports `current_status` alongside `current_stop_sequence`. When it
reports STOPPED_AT sequence 25, the agency's own AVL system is asserting the
vehicle is physically at that stop. That is the highest-quality arrival signal
available, because we are reading the agency's determination rather than
inferring one.

Measured on the live regional feed: available for 15 of 28 operators. Muni
populates it 99.2% of the time.

THE ARRIVAL IS THE *FIRST* SIGHTING, NOT THE LAST
-------------------------------------------------
A vehicle sitting at a stop reports STOPPED_AT on every poll while it waits.
Taking the last such observation would measure DEPARTURE; taking any middle one
measures nothing in particular. The arrival is the first poll in which the
vehicle appears STOPPED_AT at that stop -- so the resolver must track what it
saw previously, which is why it holds per-vehicle state.

This is also exactly why message keys matter. All observations of one trip must
arrive in order, on one partition, or "first sighting" is meaningless.

TIMESTAMP CHOICE
----------------
`vehicle_report_ts` -- measured on board -- is the event time, never our poll
clock. A stale feed republishing an old position would otherwise manufacture a
brand-new arrival at the wrong moment (pitfalls 1.1, 12.2).

WHAT THIS DOES NOT DO
---------------------
Resolvers B (geofence) and C (prediction settlement) are not implemented.
C is nearly unusable on this feed anyway -- only 4 of 28 operators emit any
settled predictions. B needs GTFS-Static stop coordinates, a separate data
source. Every emitted event is therefore stamped `arrival_method` so the
provenance is explicit rather than assumed, and adding resolvers later does not
invalidate rows already written.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, Producer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from streaming.contracts import TOPIC_ARRIVAL_EVENTS, TOPIC_VEHICLE_POSITIONS

log = logging.getLogger("resolver")

STOPPED_AT = "STOPPED_AT"


@dataclass
class TripState:
    """What we last saw for one (service_date, trip_id)."""

    last_status: str | None = None
    last_stop_sequence: int | None = None
    # Stop sequences already emitted for this trip. Prevents re-emitting while
    # the vehicle sits at the stop across multiple polls, and makes a replay
    # of the same input converge to the same output (pitfall 3.7).
    emitted: set[int] = field(default_factory=set)


@dataclass
class Stats:
    consumed: int = 0
    arrivals: int = 0
    suppressed_repeat: int = 0
    skipped_no_trip: int = 0
    skipped_no_status: int = 0
    skipped_no_sequence: int = 0
    skipped_no_event_ts: int = 0
    by_agency: dict = field(default_factory=lambda: defaultdict(int))

    def as_dict(self) -> dict:
        d = self.__dict__.copy()
        d["by_agency"] = dict(sorted(self.by_agency.items(), key=lambda kv: -kv[1]))
        return d


def agency_of(trip_id: str | None, route_id: str | None) -> str:
    """511 regional IDs are prefixed `AGENCY:`. Falls back to UNKNOWN."""
    for v in (trip_id, route_id):
        if isinstance(v, str) and ":" in v:
            return v.split(":", 1)[0]
    return "UNKNOWN"


class ArrivalResolver:
    def __init__(self, stats: Stats):
        self.state: dict[tuple[str, str], TripState] = {}
        self.stats = stats

    def process(self, ev: dict) -> dict | None:
        """One vehicle position -> an arrival event, or None."""
        self.stats.consumed += 1

        trip = ev.get("trip") or {}
        trip_id = trip.get("trip_id")
        service_date = trip.get("service_date")

        # Without a trip identity there is no grain to attach an arrival to.
        if not trip_id:
            self.stats.skipped_no_trip += 1
            return None

        status = ev.get("current_status")
        if status is None:
            # This agency does not populate current_status -- resolver A cannot
            # fire. Counted, not silently dropped: the count IS the measurement
            # of resolver coverage.
            self.stats.skipped_no_status += 1
            return None

        seq = ev.get("current_stop_sequence")
        if seq is None:
            self.stats.skipped_no_sequence += 1
            return None

        event_ts = ev.get("vehicle_report_ts")
        if event_ts is None:
            # No producer timestamp means no trustworthy event time, and using
            # our poll clock instead would fabricate one.
            self.stats.skipped_no_event_ts += 1
            return None

        # service_date may be absent; keep the trip separable anyway.
        key = (service_date or "nodate", trip_id)
        st = self.state.get(key)
        if st is None:
            st = self.state[key] = TripState()

        arrival = None
        if status == STOPPED_AT:
            if seq in st.emitted:
                # Still parked at a stop we already recorded.
                self.stats.suppressed_repeat += 1
            else:
                # FIRST sighting at this stop -> this is the arrival.
                st.emitted.add(seq)
                self.stats.arrivals += 1
                agency = agency_of(trip_id, trip.get("route_id"))
                self.stats.by_agency[agency] += 1

                envelope = ev.get("envelope") or {}
                arrival = {
                    # Grain: (service_date, trip_id, stop_sequence). Never
                    # stop_id -- 7 of 28 operators revisit a stop_id within one
                    # trip (pitfall 7.6).
                    "service_date": service_date,
                    "trip_id": trip_id,
                    "stop_sequence": seq,
                    "stop_id": ev.get("current_stop_id"),
                    "route_id": trip.get("route_id"),
                    "direction_id": trip.get("direction_id"),
                    "agency": agency,
                    "vehicle_id": ev.get("vehicle_id"),
                    # Event time, from the vehicle. Not our clock.
                    "actual_arrival_ts": event_ts,
                    "actual_arrival_ts_epoch": ev.get("vehicle_report_ts_epoch"),
                    # --- provenance. Without these, agency silently correlates
                    # with label quality and any model learns our code
                    # (pitfall 7.2).
                    "arrival_method": "stopped_at",
                    "arrival_confidence": "high",
                    "arrival_method_agreement_s": None,  # only one method fired
                    # The sampling interval bounds how precise this timestamp
                    # can possibly be. Carried per row so a cadence change does
                    # not invalidate older rows (pitfall 7.3).
                    "poll_interval_s": envelope.get("poll_interval_s"),
                    "resolver_version": "1.0.0",
                    "decoder_version": envelope.get("decoder_version"),
                    "contract_version": envelope.get("contract_version"),
                    "resolved_at": datetime.now(timezone.utc).isoformat(),
                    "prev_status": st.last_status,
                }

        st.last_status = status
        st.last_stop_sequence = seq
        return arrival


def main() -> int:
    ap = argparse.ArgumentParser(description="Resolve arrivals from vehicle positions")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--group", default="arrival-resolver-v1")
    ap.add_argument(
        "--from-beginning",
        action="store_true",
        help="read the topic from offset 0 rather than resuming",
    )
    ap.add_argument(
        "--idle-timeout-s",
        type=float,
        default=10.0,
        help="exit after this many seconds with no new messages (0 = run forever)",
    )
    ap.add_argument("--agency", default="", help="only resolve this agency prefix")
    ap.add_argument("--out", default="", help="also append arrivals to this JSONL file")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s"
    )

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "auto.offset.reset": "earliest" if args.from_beginning else "latest",
            # Manual commits. Auto-commit acknowledges messages on a timer,
            # which can mark a record consumed BEFORE its arrival was actually
            # produced -- losing it on a crash. Committing after processing
            # gives at-least-once, which is what the idempotent grain expects
            # (pitfall 3.7).
            "enable.auto.commit": False,
            "max.poll.interval.ms": 600000,
        }
    )
    consumer.subscribe([TOPIC_VEHICLE_POSITIONS])

    producer = Producer(
        {"bootstrap.servers": args.bootstrap, "enable.idempotence": True,
         "linger.ms": 20, "queue.buffering.max.messages": 200_000}
    )

    stats = Stats()
    resolver = ArrivalResolver(stats)
    out_fh = open(args.out, "a") if args.out else None

    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))

    log.info("resolver started group=%s topic=%s", args.group, TOPIC_VEHICLE_POSITIONS)
    last_msg = time.time()
    started = time.time()

    while not stop["flag"]:
        msg = consumer.poll(1.0)
        if msg is None:
            if args.idle_timeout_s and time.time() - last_msg > args.idle_timeout_s:
                log.info("idle for %.0fs, stopping", args.idle_timeout_s)
                break
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                log.error("consume error: %s", msg.error())
            continue

        last_msg = time.time()
        try:
            ev = json.loads(msg.value())
        except json.JSONDecodeError as e:
            log.error("undecodable message at offset %s: %s", msg.offset(), e)
            continue

        if args.agency:
            tid = (ev.get("trip") or {}).get("trip_id") or ""
            if not tid.startswith(args.agency + ":"):
                continue

        arrival = resolver.process(ev)
        if arrival is not None:
            payload = json.dumps(arrival, separators=(",", ":"))
            producer.produce(
                TOPIC_ARRIVAL_EVENTS,
                key=f"{arrival['service_date']}:{arrival['trip_id']}".encode(),
                value=payload.encode(),
            )
            if out_fh:
                out_fh.write(payload + "\n")
            producer.poll(0)

        if stats.consumed % 20000 == 0:
            consumer.commit(asynchronous=True)
            log.info("consumed=%d arrivals=%d", stats.consumed, stats.arrivals)

    producer.flush(30)
    try:
        consumer.commit(asynchronous=False)
    except Exception:
        pass  # nothing to commit
    consumer.close()
    if out_fh:
        out_fh.close()

    result = stats.as_dict()
    result["elapsed_s"] = round(time.time() - started, 1)
    result["distinct_trips_tracked"] = len(resolver.state)
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        log.info("done: %s", json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
