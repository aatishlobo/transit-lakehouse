# transit-lakehouse

**Measuring how wrong Bay Area transit predictions actually are — through a Kafka streaming pipeline.**

> Data provided by [511.org](http://www.511.org) — Metropolitan Transportation Commission.

---

## The problem

Transit agencies publish two live feeds: **predictions** ("the bus will reach
stop 22 at 15:07") and **GPS positions** ("bus 4821 is here, now"). Neither ever
states *"bus 4821 arrived at stop 22 at 15:09."*

That fact has to be **derived**. And because delay = actual − scheduled, with
scheduled published and exact, **every error in the derivation lands directly in
the final number** — nothing downstream can correct it.

This project derives arrivals from vehicle state transitions, then uses them to
answer a question nobody publishes an answer to:

> **When an agency says the bus arrives in *N* minutes, how wrong is it?**

The answer is only meaningful because the two halves come from **independent
feeds** — predictions from TripUpdates, actual arrivals from VehiclePositions.

## The result

SF Muni, 25,501 (prediction, arrival) pairs:

```
   lead time       n     bias  med|err|     p90    <60s   <180s
     0-2 min     509      -11        13      52   92.7%  100.0%
     2-5 min    4603      -46        50     112   59.5%   98.4%
     5-10 min   4897      -66        70     172   43.3%   91.2%
    10-20 min   8919      -98       102     257   31.1%   77.0%
      20+ min   6573     -142       146     375   23.0%   59.3%
```

Short-horizon predictions are excellent; accuracy degrades steadily with
horizon. The bias is negative throughout — vehicles arrive **later** than
promised — and grows to 2.4 minutes at long horizons.

**Important caveat, stated up front:** our derived arrivals are biased late and
over-sample long-dwell stops, so this is an **upper bound** on agency optimism,
not a point estimate. See [Limitations](#limitations).

A bounded ML model then corrects those predictions, cutting error **14.7%** below
the agency's own ETA.

---

## Architecture

```
                     511 regional GTFS-Realtime          511 GTFS-Static
                     (protobuf, 60 req/hour)             (68 MB zip)
                              |                                |
                    poller (exactly 1 replica)      fetch_static (content-addressed)
                    archive-before-decode                       |
                              v                                 |
                    data/raw/  [SYSTEM OF RECORD]        data/static/
                              |                                 |
              +---------------+---------------+                 |
              v                               v                 |
     Pydantic contract                 spark/bronze             |
              |                        decode + explode         |
              v                               v                 v
        Kafka topics  -> resolver      Delta bronze      spark/dim_schedule
        (24h, delete)     |                  v            SCD2, 3.87M rows
                          |            Delta silver              |
                          |                  v                   |
                          +--------> gold/fct_stop_arrival       |
                                     idempotent MERGE            |
                                            |                    |
                                            +----> AS-OF JOIN <--+
                                                        v
                                              gold/fct_stop_otp
                                                        v
                                                 dbt marts (17 tests)
                                                        v
                                          SQLite export -> FastAPI + dashboard

        orchestrated by Dagster (dbt is an ASSET EDGE, not a schedule offset)
```

**Prediction accuracy vs on-time performance.** These are different questions and
the project answers both. *Prediction accuracy* compares the derived arrival to
what the agency predicted; it needs only the realtime feeds. *On-time
performance* compares the derived arrival to the published timetable, which
requires GTFS-Static, an SCD2 dimension, and an as-of join so a trip resolves
against the schedule that was in force on its service date.

**Three structural decisions**

| Decision | Rejected alternative | Cost |
|---|---|---|
| Archive is the system of record; Kafka is transport with 24h retention | Kafka as durable storage | Replay needed to rebuild topics |
| Arrivals from vehicle positions only (resolver A) | Prediction settlement, far better coverage | ~21% capture, only 15 of 28 operators |
| SCD2 dimension + as-of join | Join to the current schedule | A dimension that grows, and a slower join |

The third is the one that looks like over-engineering and is not: joining facts
to the *current* schedule silently recomputes every historical number each time
an agency republishes a timetable.

## Quick start

Two environments, deliberately separate. Spark 4 needs Java 17+ and Python
<=3.12; the ingest service is 3.13 and pins protobuf for the ML stack.

```bash
make venv          # ingest + serving      (python3, protobuf pinned)
make venv-spark    # Spark/Delta/dbt/Dagster (python3.12 + JDK 21)
```

**The local Kafka demo** (no API key, deterministic replay):

```bash
make demo          # replay -> Kafka -> resolver -> metrics -> checks
```

**The lakehouse**, from the archive through to the dashboard:

```bash
make static        # fetch GTFS-Static (needs API_511_KEY)
make full          # bronze -> silver -> gold -> dim -> otp -> marts -> export
make serve         # dashboard on http://localhost:8000
```

**Tests**

```bash
make test          # 97 tests: decode, contracts, resolver, ML leakage, DST
make lake-test     # 19 tests: lakehouse invariants + SCD2 semantics
make dbt-test      # 17 dbt tests
make k8s-validate  # manifests parse
```

## Repository layout

Organised by pipeline stage rather than a single `src/`:

| Path | Contents |
|---|---|
| `ingest/poller/decode.py` | presence-aware GTFS-RT decoding — **the correctness core** |
| `ingest/poller/poller.py` | fetch, archive-first, rate budget, stale detection |
| `ingest/static/` | 511 GTFS-Static fetch, content-addressed archive |
| `streaming/` | Pydantic contracts, Kafka producer/consumer, resolver, aggregator |
| `spark/session.py` | UTC, Delta, pinned retention, one shared metastore |
| `spark/bronze.py` | archive → Delta; reuses `decode.py` unmodified |
| `spark/silver.py` | typed, deduplicated, quality-flagged |
| `spark/gold.py` | `fct_stop_arrival`, idempotent MERGE on the grain |
| `spark/scheduled_time.py` | GTFS noon-minus-12h arithmetic; both DST transitions |
| `spark/dim_schedule.py` | SCD2 schedule dimension |
| `spark/fct_otp.py` | as-of join → true on-time performance |
| `dbt/` | 4 models, 17 tests, marts for serving |
| `orchestration/definitions.py` | Dagster assets + asset checks |
| `serving/` | SQLite export, FastAPI, dashboard |
| `k8s/` | Strimzi, KEDA, CronJob, single-replica poller |
| `ml/` | feature build, training, inference for the AI element |
| `evaluation/` | acceptance checks |
| `tests/` | 116 tests across two interpreters |

## Correctness: what this project is actually about

Five invariants, each of which corrupts data **silently** if broken. None throw.

**Absent is not zero.** GTFS-Realtime is proto2: a field can be genuinely absent,
which differs from being zero. `delay = 0` means *exactly on time*; absent means
*no information*. Read naively, protobuf returns `0` for both.

**Measured: 44.2% of records carry no delay at all.** Written the obvious way,
this project would report ~54,000 records per poll as perfectly on time — a
fabricated spike that looks entirely plausible and corrupts every downstream
metric. Verified against raw protobuf across 408,149 rows with zero violations.

**The grain is `(service_date, trip_id, stop_sequence)`,** never `stop_id`. Seven
of 28 operators run trips revisiting the same stop; a `stop_id` grain merges two
real events into one.

**`service_date` comes from the trip, never from a clock.** Service days run past
midnight. Relatedly, the raw archive partitions on `ingest_dt` — a single payload
contains trips from *several* service dates, so service date is a property of a
row, not a file.

**Event time comes from the producer, never our poll clock.** A stale feed would
otherwise manufacture observations that never happened.

**Timestamps stored UTC, converted once at the end.** Truncating hour-of-day in
UTC shifts every bar of an hourly chart by 7–8 hours while leaving totals
correct.

Full register: [`docs/PITFALLS.md`](docs/PITFALLS.md) — 60 known failure modes.

---

## Validation

```bash
make evaluate
```

| Check | Result |
|---|---|
| `null_zero_discrimination` | 408,149 rows vs raw protobuf, **0 violations** |
| `contract_validation` | 245,701 valid, 0 invalid |
| `grain_uniqueness` | 6,046 arrivals, 6,046 distinct keys |
| `idempotent_resolution` | two runs, bit-for-bit identical |
| `provenance_complete` | 0 missing fields |
| `event_time_not_ingest_time` | 0 collisions |

Every check carries a **row-count floor** and reports `UNMEASURED` rather than
`PASS` when handed too little data — a check that passes on an empty table is
worse than no check.

---

## Optional extension: controlled method comparison

*This is the single labelled extension beyond the required minimum. Full write-up
in `report.pdf` §10.*

Four methods for predicting a vehicle's arrival, evaluated on **one input, one
temporal split, one named metric** (MAE in seconds). Three baselines and one
learned model.

**Exact steps** — no API key, no Kafka, no network. Runs from committed data:

```bash
make venv && source .venv/bin/activate
make train
```

~10 seconds. Input `ml/data/features_sample.csv.gz` (168,160 rows, committed).

**Expected output** — console table, and the same figures saved to
`ml/artifacts/training_report.json`:

| Method | MAE (s) | RMSE (s) | bias (s) | within 60 s |
|---|---|---|---|---|
| trust the agency unchanged | 194.7 | 380.9 | -152.3 | 30.9% |
| add one global mean residual | 215.1 | 358.2 | +80.1 | 14.9% |
| add mean residual per lead-time bucket | 206.3 | 358.9 | +76.1 | 22.6% |
| **learned correction** | **168.5** | **308.9** | **+45.3** | **31.8%** |

**Saved artifacts, both submitted:** `ml/artifacts/training_report.json` (metrics,
split definition, and every feature accepted or rejected with the reason) and
`ml/artifacts/prediction_correction_model.joblib`.

**The result worth reading twice:** the model beats the agency by 13.5%, but
*both naive bias corrections lose to doing nothing.* Residuals are heavily
right-skewed — at a 20-minute horizon the mean is +295 s while the median is
+174 s — so adding the mean overcorrects the typical vehicle to accommodate the
rare one. The project's own headline finding, applied naively, makes predictions
worse. That is precisely why the model learns a *conditional* correction rather
than a constant.

These figures come from the committed sample. The report's §7.2 quotes 166.6 s
from the full 1.34 M-row local archive, which is 137 MB and not submitted; the
1.9 s gap is the cost of shipping a reviewable subset, noted so a reproduced
168.5 s reads as confirmation.

---

## Limitations

Stated plainly, because several affect how the headline number should be read.

**Coverage is ~21% of stop events.** Vehicles dwell for seconds; we sample every
120 seconds (a 60 req/hour quota). 90.3% of captured events appear in exactly one
poll.

**Derived arrivals are biased late.** A vehicle is observable as `STOPPED_AT`
only between arriving and departing, so timestamps fall at or after true arrival
— never before.

**Long-dwell stops are over-represented.** Capture probability scales with dwell
time, so terminals and layovers dominate. Median observed dwell among multi-poll
sightings: 353 seconds. **Together these mean the measured bias is an upper
bound.**

**One operator, six days.** SF Muni only; no seasonal or holiday variation.

**The ML model is afternoon/evening only** — 95.8% of training data falls in
hours 14–19, because the archiving machine sleeps overnight.

**Scope taken deliberately:** a plain-Python consumer rather than Spark
Structured Streaming, and prediction accuracy rather than schedule-based on-time
performance (which would require GTFS-Static). Both are documented deviations
from the original proposal.

---

## Data and licensing

Source data is provided by 511.org under the **511 Data Disseminator Agreement**.
Full terms, schema, rate limits and rights in [`DATA_SOURCE.md`](DATA_SOURCE.md).

**This repository is private.** §2(c) of the agreement requires securing written
acceptance of its terms before providing the data to third parties, and
`data/replay_sample/` contains verbatim 511 protobuf payloads rather than
derivative works. Access is granted individually.

No credentials are committed. `.env` and all logs are gitignored; the API key is
redacted at source in poller output.

---

## Documentation

| Document | Purpose |
|---|---|
| [`docs/PIPELINE_WALKTHROUGH.md`](docs/PIPELINE_WALKTHROUGH.md) | **Start here** — what actually happens to the data, stage by stage |
| [`docs/ONBOARDING.md`](docs/ONBOARDING.md) | Full catch-up guide for a new contributor |
| [`DATA_SOURCE.md`](DATA_SOURCE.md) | Source, schema, rights, limitations |
| [`AI_USAGE.md`](AI_USAGE.md) | The AI element, and AI-assisted development disclosure |
| [`CLAUDE.md`](CLAUDE.md) | Project constitution — invariants and rationale |
| [`docs/PITFALLS.md`](docs/PITFALLS.md) | 60 known failure modes, tiered |
| [`docs/reports/`](docs/reports/) | Day-by-day build log, written for non-specialists |

---

*MSDS 682 — Data Stream Processing, University of San Francisco, Summer 2026.*
