"""Prediction-accuracy aggregator: the project's useful output.

QUESTION ANSWERED
-----------------
"When an agency says a bus will arrive in N minutes, how wrong is it?"

This is answerable because the two halves come from INDEPENDENT sources:

  * the prediction  -- from TripUpdates, the agency telling us what it expects
  * the actual      -- derived by us from VehiclePositions, watching the vehicle
                       report STOPPED_AT

That independence is the whole reason the number means anything. If we had
derived arrivals from the predictions themselves (resolver C, "prediction
settlement"), this comparison would be measuring the agency against itself and
would produce an impressively small error that means nothing. That is the
leakage trap in CLAUDE.md 7.3, and it is avoided here by construction rather
than by care: our labels come from position data, our features from prediction
data, and the two never touch.

METRICS
-------
For each (arrival, prediction) pair:

    error_s     = predicted_arrival - actual_arrival
                  positive => the agency predicted LATER than reality
    lead_time_s = actual_arrival - prediction_issued
                  how far in advance the prediction was made

Errors are bucketed by lead time, because a 30-second-out prediction and a
20-minute-out prediction are not the same product and averaging them together
hides the thing a rider cares about.

Reported per bucket: count, median absolute error, MEAN SIGNED error, p90.
The signed mean is the important one -- it is the BIAS. Random error averages
out across a route; a systematic 40-second optimism does not, and it is exactly
what makes riders miss buses.

MEASUREMENT BIAS -- READ BEFORE QUOTING ANY NUMBER
--------------------------------------------------
It is tempting to describe the poll interval as "+/-60s of noise". That is
wrong in a way that matters, and conflates three distinct effects. Measured on
SF Muni over the sample window:

1. COVERAGE (not precision). 90.3% of stop-events are caught in exactly one
   poll, meaning typical dwell is far shorter than the 120s poll interval. We
   therefore MISS most stop events entirely -- about 21% are captured. That is
   a sample-size problem, not an accuracy problem.

2. ONE-SIDED OFFSET. A vehicle is only observable as STOPPED_AT between its
   arrival and its departure, so a detected timestamp always falls at or AFTER
   the true arrival, never before. The offset is bounded by dwell, not by the
   poll interval -- small for ordinary stops, but strictly non-negative. Our
   derived arrivals are therefore biased LATE by a small amount.

3. SELECTION BIAS -- the one that actually threatens the conclusion. Capture
   probability is proportional to dwell time, so we preferentially observe
   LONG-dwell stops: terminals, layovers and timepoints where vehicles wait on
   purpose. Median observed dwell among multi-poll sightings is 353s. Those
   stops are not representative of ordinary stops, and they are exactly where
   schedule-holding behaviour distorts arrival semantics.

Consequence for the headline number: the negative bias below (agency predicts
EARLIER than we observe) is partly real and partly effects 2 and 3. It should
be read as an upper bound on agency optimism, not a point estimate. Shortening
the poll interval attacks all three at once, which is the strongest argument
for the 511 rate-limit increase.

STREAMING SHAPE, AND ITS LIMIT
------------------------------
This consumes both topics and joins them in memory across the whole replay
window. That is honest for a bounded 39-minute replay and is what makes the
demo reproducible. A production version would need a windowed join with
watermarks and eviction, since the buffer here grows with the replay length.
Stated as a known limitation rather than papered over.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import statistics
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, KafkaError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from streaming.contracts import TOPIC_ARRIVAL_EVENTS, TOPIC_TRIP_UPDATES

log = logging.getLogger("aggregator")

# Lead-time buckets in seconds. Boundaries chosen around how riders actually
# use predictions: "is it here?", "do I leave now?", "do I have time?".
LEAD_BUCKETS = [
    (0, 120, "0-2 min"),
    (120, 300, "2-5 min"),
    (300, 600, "5-10 min"),
    (600, 1200, "10-20 min"),
    (1200, 10**9, "20+ min"),
]


def bucket_for(lead_s: float) -> str | None:
    for lo, hi, label in LEAD_BUCKETS:
        if lo <= lead_s < hi:
            return label
    return None


def hour_of(iso_ts: str) -> int | None:
    """Local hour of day. Converted from UTC exactly once, here in the serving
    layer -- truncating hour-of-day in UTC would shift every bar of an
    hourly chart by 7-8 hours (pitfall 8.4)."""
    try:
        dt = datetime.fromisoformat(iso_ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone().hour
    except (ValueError, TypeError):
        return None


class Aggregator:
    def __init__(self, agency: str | None = None):
        self.agency = agency or None
        # grain -> SET of (issued_epoch, predicted_arrival_epoch).
        #
        # A set, not a list, and that is a correctness choice rather than a
        # micro-optimisation. Replaying the same archive into Kafka twice
        # delivers every prediction twice; with a list, each arrival would then
        # pair with two identical predictions and every count in the output
        # would double. Distributional statistics (bias, median, percentiles)
        # survive that unharmed, which is exactly what makes it dangerous --
        # the numbers still look right while n silently lies.
        #
        # The same (grain, issued_at, predicted_at) tuple IS the same
        # observation, so collapsing duplicates is semantically correct and
        # makes the aggregate idempotent under at-least-once delivery, matching
        # the guarantee the resolver already provides via its grain dedup.
        self.predictions: dict[tuple, set[tuple[int, int]]] = defaultdict(set)
        self.arrivals: list[dict] = []
        self._arrival_keys: set[tuple] = set()
        self.stats = {
            "predictions_buffered": 0,
            "arrivals_seen": 0,
            "arrivals_matched": 0,
            "arrivals_unmatched": 0,
            "pairs": 0,
            "pairs_dropped_negative_lead": 0,
            "duplicate_predictions_collapsed": 0,
            "duplicate_arrivals_collapsed": 0,
        }

    def add_prediction(self, ev: dict) -> None:
        trip = ev.get("trip") or {}
        tid = trip.get("trip_id")
        seq = ev.get("stop_sequence")
        predicted = ev.get("arrival_time_epoch")
        issued = ev.get("trip_update_ts_epoch") or (ev.get("envelope") or {}).get(
            "feed_header_ts_epoch"
        )
        if not tid or seq is None or not predicted or not issued:
            return
        if self.agency and not tid.startswith(self.agency + ":"):
            return
        key = (trip.get("service_date"), tid, seq)
        before = len(self.predictions[key])
        self.predictions[key].add((issued, predicted))
        if len(self.predictions[key]) > before:
            self.stats["predictions_buffered"] += 1
        else:
            self.stats["duplicate_predictions_collapsed"] += 1

    def add_arrival(self, ev: dict) -> None:
        if self.agency and ev.get("agency") != self.agency:
            return
        # Same reasoning as predictions: a redelivered arrival is the same
        # event, and counting it twice would double its weight in the output.
        k = (ev.get("service_date"), ev.get("trip_id"), ev.get("stop_sequence"),
             ev.get("actual_arrival_ts_epoch"))
        if k in self._arrival_keys:
            self.stats["duplicate_arrivals_collapsed"] += 1
            return
        self._arrival_keys.add(k)
        self.arrivals.append(ev)
        self.stats["arrivals_seen"] += 1

    def join(self) -> list[dict]:
        """Pair each arrival with every prediction issued before it."""
        pairs = []
        for a in self.arrivals:
            key = (a.get("service_date"), a.get("trip_id"), a.get("stop_sequence"))
            preds = self.predictions.get(key)
            actual = a.get("actual_arrival_ts_epoch")
            if not preds or not actual:
                self.stats["arrivals_unmatched"] += 1
                continue
            matched = False
            for issued, predicted in sorted(preds):
                lead = actual - issued
                # A prediction issued AFTER the vehicle already arrived is not
                # a prediction -- it is a correction, and scoring it would
                # flatter the agency. Excluded, and counted so the exclusion
                # is visible.
                if lead <= 0:
                    self.stats["pairs_dropped_negative_lead"] += 1
                    continue
                b = bucket_for(lead)
                if b is None:
                    continue
                pairs.append(
                    {
                        "agency": a.get("agency"),
                        "route_id": a.get("route_id"),
                        "trip_id": a.get("trip_id"),
                        "stop_sequence": a.get("stop_sequence"),
                        "stop_id": a.get("stop_id"),
                        "hour": hour_of(a.get("actual_arrival_ts") or ""),
                        "lead_time_s": lead,
                        "lead_bucket": b,
                        "error_s": predicted - actual,
                        "abs_error_s": abs(predicted - actual),
                        "poll_interval_s": a.get("poll_interval_s"),
                        "arrival_method": a.get("arrival_method"),
                    }
                )
                matched = True
            if matched:
                self.stats["arrivals_matched"] += 1
            else:
                self.stats["arrivals_unmatched"] += 1
        self.stats["pairs"] = len(pairs)
        return pairs


def summarize(pairs: list[dict], group_keys: list[str]) -> list[dict]:
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for p in pairs:
        groups[tuple(p.get(k) for k in group_keys)].append(p)

    out = []
    for key, rows in sorted(groups.items(), key=lambda kv: (str(kv[0]))):
        errs = [r["error_s"] for r in rows]
        abserrs = sorted(r["abs_error_s"] for r in rows)
        rec = dict(zip(group_keys, key))
        rec.update(
            {
                "n": len(rows),
                # Signed mean = BIAS. The headline number.
                "mean_error_s": round(statistics.fmean(errs), 1),
                "median_error_s": round(statistics.median(errs), 1),
                "median_abs_error_s": round(statistics.median(abserrs), 1),
                "p90_abs_error_s": round(abserrs[int(0.9 * (len(abserrs) - 1))], 1),
                "pct_within_60s": round(
                    100 * sum(1 for e in abserrs if e <= 60) / len(abserrs), 1
                ),
                "pct_within_180s": round(
                    100 * sum(1 for e in abserrs if e <= 180) / len(abserrs), 1
                ),
            }
        )
        out.append(rec)
    return out


def consume_all(bootstrap: str, group: str, agg: Aggregator, idle_s: float) -> None:
    c = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 600000,
        }
    )
    c.subscribe([TOPIC_TRIP_UPDATES, TOPIC_ARRIVAL_EVENTS])
    last = time.time()
    n = 0
    while True:
        msg = c.poll(1.0)
        if msg is None:
            if time.time() - last > idle_s:
                break
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                log.error("consume error: %s", msg.error())
            continue
        last = time.time()
        n += 1
        try:
            ev = json.loads(msg.value())
        except json.JSONDecodeError:
            continue
        if msg.topic() == TOPIC_TRIP_UPDATES:
            agg.add_prediction(ev)
        else:
            agg.add_arrival(ev)
        if n % 200000 == 0:
            log.info(
                "consumed=%d predictions=%d arrivals=%d",
                n, agg.stats["predictions_buffered"], agg.stats["arrivals_seen"],
            )
    c.close()
    log.info("consumed %d messages total", n)


def main() -> int:
    ap = argparse.ArgumentParser(description="Prediction accuracy aggregator")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--group", default="")
    ap.add_argument("--agency", default="SF", help="agency prefix; empty = all")
    ap.add_argument("--idle-timeout-s", type=float, default=15.0)
    ap.add_argument("--outdir", default="outputs")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s"
    )

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    agg = Aggregator(args.agency)
    consume_all(
        args.bootstrap,
        args.group or f"aggregator-{int(time.time())}",
        agg,
        args.idle_timeout_s,
    )

    pairs = agg.join()
    if not pairs:
        log.error("no (arrival, prediction) pairs -- did the producer and "
                  "resolver run first?")
        return 1

    by_lead = summarize(pairs, ["lead_bucket"])
    # Keep bucket order meaningful rather than alphabetical.
    order = {label: i for i, (_, _, label) in enumerate(LEAD_BUCKETS)}
    by_lead.sort(key=lambda r: order.get(r["lead_bucket"], 99))

    by_route_hour = summarize(pairs, ["route_id", "hour", "lead_bucket"])
    by_route = summarize(pairs, ["route_id"])

    poll_intervals = {p["poll_interval_s"] for p in pairs if p["poll_interval_s"]}
    noise = max(poll_intervals) / 2 if poll_intervals else None

    for name, rows in (
        ("prediction_error_by_lead_time.csv", by_lead),
        ("prediction_error_by_route_hour.csv", by_route_hour),
        ("prediction_error_by_route.csv", by_route),
    ):
        p = outdir / name
        with p.open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        log.info("wrote %s (%d rows)", p, len(rows))

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "agency": args.agency or "ALL",
        "method": {
            "actual_arrival": "derived from VehiclePositions current_status="
                              "STOPPED_AT (resolver A)",
            "prediction": "TripUpdates arrival.time, as published by the agency",
            "independence": "labels from position data, predictions from "
                            "TripUpdates -- no shared provenance, so this is "
                            "not measuring the agency against itself",
        },
        "counts": agg.stats,
        "measurement_bias": {
            "poll_interval_s": sorted(poll_intervals),
            "coverage": (
                "~21% of stop events captured; 90.3% of captured events appear "
                "in exactly one poll, so typical dwell << 120s poll interval. "
                "This limits sample size, not timestamp accuracy."
            ),
            "one_sided_offset": (
                "A vehicle is observable as STOPPED_AT only between arrival and "
                "departure, so derived arrival times fall at or after the true "
                "arrival -- never before. Bias is LATE, bounded by dwell."
            ),
            "selection_bias": (
                "Capture probability scales with dwell, so long-dwell stops "
                "(terminals, layovers, timepoints) are over-represented. Median "
                "observed dwell among multi-poll sightings: 353s."
            ),
            "interpretation": (
                "The negative bias below means the agency predicts EARLIER than "
                "we observe. It is partly real and partly the two effects above, "
                "so treat it as an upper bound on agency optimism rather than a "
                "point estimate."
            ),
        },
        "by_lead_time": by_lead,
        "top_routes_by_volume": sorted(by_route, key=lambda r: -r["n"])[:10],
    }
    (outdir / "prediction_error_summary.json").write_text(json.dumps(summary, indent=2))
    log.info("wrote %s", outdir / "prediction_error_summary.json")

    print("\n" + "=" * 68)
    print(f"PREDICTION ACCURACY  --  agency={args.agency or 'ALL'}")
    print("=" * 68)
    print(f"{'lead time':>12s} {'n':>7s} {'bias':>8s} {'med|err|':>9s} "
          f"{'p90':>7s} {'<60s':>7s} {'<180s':>7s}")
    for r in by_lead:
        print(f"{r['lead_bucket']:>12s} {r['n']:>7d} {r['mean_error_s']:>+8.0f} "
              f"{r['median_abs_error_s']:>9.0f} {r['p90_abs_error_s']:>7.0f} "
              f"{r['pct_within_60s']:>6.1f}% {r['pct_within_180s']:>6.1f}%")
    print(f"\nbias = mean signed error. NEGATIVE means the agency predicts")
    print(f"EARLIER than observed, i.e. vehicles arrive later than promised.")
    print(f"Caveat: derived arrivals are biased LATE (a vehicle is visible as")
    print(f"STOPPED_AT only after it arrives) and over-sample long-dwell stops,")
    print(f"so treat this as an UPPER BOUND on agency optimism.")
    print("=" * 68)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
