# Real-Time Transit Reliability Lakehouse

**Project overview — August 2026**

---

## What it is

A streaming data platform that measures on-time performance for Bay Area transit
(BART, Muni, AC Transit, Caltrain) in near-real-time, and a public dashboard on
top of it showing delay distributions by route, stop, hour, and day.

The dashboard is the visible artifact. The less visible half is that the tables
behind it are designed to be a correct feature source for machine learning —
specifically an arrival-time prediction model and an agentic analytics layer
that queries the same warehouse. Building both on one substrate is the point:
the pipeline is designed knowing what the model will need, rather than the model
being retrofitted onto whatever the pipeline happened to produce.

## Why this data, honestly

Transit data isn't interesting because it arrives fast. It's interesting because
it arrives **continuously, out of order, duplicated, and referencing a schedule
that changes underneath you.** Those are the problems that justify a streaming
stack; "the events are frequent" would not be.

The sources are GTFS-Realtime (protobuf, polled on a cadence — vehicle positions,
trip updates, service alerts) and GTFS-Static (the published schedule, republished
daily, materially changing three or four times a year).

## The central problem

**GTFS-Realtime does not contain "actual arrival time."**

It contains *predictions* (trip updates) and *GPS positions* (vehicle positions).
Nowhere does a feed say "the train reached stop X at time T." That fact has to be
derived, and there are three ways to derive it:

1. **Position state transition** — the vehicle reports `STOPPED_AT` for a stop.
   The agency's own AVL system made the determination. Best signal when it's
   populated, which varies by agency.
2. **Geofencing** — nearest approach of a GPS trace to the stop's coordinates.
   Works everywhere, but measures the moment of closest approach rather than the
   moment of stopping, so it is systematically biased early.
3. **Prediction settlement** — take the last prediction issued before the vehicle
   passed the stop and treat it as observed. This is what MTC's own published
   dataset does. Good for rail, weaker for buses in traffic.

This matters more than it first appears. Delay = actual − scheduled, and scheduled
is exact. So **every bit of error in the arrival derivation lands directly in the
model's target variable.** Worse, the *bias* is what hurts, not the noise: random
error mostly averages out, but a geofence that fires fifteen seconds early shifts
the entire delay distribution in a way nothing downstream can detect or correct.

And because which method is available depends on which fields each agency
populates, a naive implementation ends up with agency-correlated label bias — a
model that appears to have learned agency effects when it has actually learned
our own code.

**The approach:** don't pick one method. Run a precedence ladder and stamp every
row with how it was resolved (`arrival_method`, `arrival_confidence`,
`method_agreement_seconds`). Downstream consumers can then filter to
high-confidence rows, and where two methods both fire, their median disagreement
gives a free bias estimate to calibrate against. It also means we can benchmark
against MTC's published stop-observations dataset rather than claiming a ground
truth we don't have.

---

## Architecture

```
GTFS-RT ──▶ Poller ──▶ Kafka ──▶ Spark Structured Streaming
(protobuf)   (archive)  (log)      │
                                   ▼
                    ┌──── Delta Lakehouse ────┐
                    │ BRONZE  raw + ingest_ts │
                    │ SILVER  typed, deduped  │
                    │ GOLD    trip_stop_events│◀── SCD2 schedule dimension
                    └────────────┬────────────┘
                                 │
                 dbt marts · Dagster · FastAPI + dashboard
                                 │
                        feeds the ML projects
```

Two design decisions carry most of the weight:

**The schedule is a Type 2 slowly-changing dimension.** To answer "was this trip
late?" for a date last spring, you must join against the schedule *in effect on
that date*, not today's. This is the single most commonly skipped correctness
problem in projects of this kind, and it's the reason the fact table is trustworthy
for historical analysis at all.

**The gold fact table is append-only, event-timestamped, and reconstructable
as-of any past moment.** Every row carries `event_ts` (when the thing happened)
strictly separate from `ingest_ts` (when we saw it). Combined with idempotent
writes and Delta time travel, that lets the downstream model compute features
"as of prediction time T" without leakage — which is the property that makes the
whole shared-substrate approach work.

---

## Current status

**Built and tested: the ingest floor.** An archiving poller and a feed profiler.
About 1,400 lines, 11 passing tests, runnable today.

Deliberately no Kafka or Spark yet, for three reasons. GTFS-RT history cannot be
re-fetched, so every hour without an archiver is permanently lost data — that
clock is the only one running. The Kafka schema should be designed against
observed field population rather than guesses, or we'll be evolving it under
compatibility constraints by week three. And the decoder is what every later
layer inherits, so it's worth getting right in isolation.

The decoder's load-bearing detail: GTFS-RT is proto2, so an absent field is
genuinely distinguishable from a zero — but only via explicit presence checks.
A naive read returns `0` for both "exactly on time" and "no information," which
would inject a large spike of fabricated on-time records into the delay
distribution. Plausible-looking, undetectable downstream, and corrupting to every
metric and label. Same trap applies to `current_status`, where an unset field
naive-reads as `IN_TRANSIT_TO`.

**Immediate next step** is to run the poller across a service peak and profile the
result. That answers which resolvers are actually available per agency, what the
`schedule_relationship` distribution looks like (our exclusion rules), the real
fan-out ratio for storage sizing, and whether any trips revisit a stop — which
confirms the fact grain must key on `stop_sequence` rather than `stop_id`.

**Roadmap after that:** Kafka → Spark bronze/silver → SCD2 dimension and the
as-of join → dbt marts with tests and freshness SLAs → Dagster orchestration →
Kubernetes → public dashboard. Roughly 60–90 focused hours.

---

## Where help is genuinely useful

- **The resolver ladder.** Once the profile is in, deciding precedence and
  calibrating the bias between methods is a self-contained, high-value problem.
- **Benchmarking against MTC's stop-observations dataset.** Independent
  validation of our derived arrivals against a published derivation.
- **The SCD2 versioning grain.** Whole-feed versus per-trip versioning is a real
  design fork with different correctness and complexity tradeoffs, and it's
  better decided by two people than one.
- **Timezone edge cases.** GTFS service days run past midnight (`25:14:00` is a
  valid scheduled time) and two days a year have 23 or 25 hours. Both break naive
  parsing silently.

## Running it

```bash
tar -xzf transit-lakehouse.tar.gz && cd transit-lakehouse
make venv && source .venv/bin/activate
make fixture && make profile     # synthetic data, no API key needed
```

A 511 API key (free, from 511.org/open-data/token) is needed for live polling.
Note the default allowance is 60 requests/hour, which caps polling at one cycle
per two minutes — and since poll cadence directly quantizes every arrival
timestamp, requesting an increase is worth doing early.
