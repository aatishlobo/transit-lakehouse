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

## Quick start

**Requires:** Python 3.11+, Docker running. **No API key needed.**

```bash
make venv && source .venv/bin/activate
make demo
```

`make demo` starts Kafka, replays 39 minutes of real archived transit data
(1.67 M records), derives arrivals, produces metrics, and runs acceptance
checks — about **2 minutes 15 seconds** cold.

Expected output ends with:

```
passed=6  failed=0  unmeasured=0  info=1
```

Results land in `outputs/`; validation evidence in `evaluation/`.

### Other commands

```bash
make test          # 83 unit tests (~2s)
make evaluate      # acceptance checks only
make train         # retrain the ML model from committed data
make predict       # demo one corrected ETA
make kafka-down    # stop Kafka
make poll-bg       # start the live archiver (needs a 511 key)
make poll-status
```

---

## Architecture

```
511 GTFS-Realtime  ──▶  poller  ──▶  data/raw/     [archive: the system of record]
   (protobuf)                            │
                                         ▼
                            replay producer ──▶ Pydantic contract
                                         │            │ invalid
                                         ▼            ▼
                                   Kafka topics    dead-letter
                                         │
                        ┌────────────────┴────────────────┐
                        ▼                                 ▼
                 arrival resolver                  prediction stream
              (STOPPED_AT transitions)                     │
                        │                                  │
                        └──────────────┬───────────────────┘
                                       ▼
                                  aggregator ──▶ outputs/  [metrics]
                                       │
                                       ▼
                              ML correction model ──▶ corrected ETAs
```

**The archive is the system of record, not Kafka.** Topics expire after 24 hours
deliberately; GTFS-Realtime has no history endpoint, so anything not archived is
lost permanently. Replay always runs from disk.

**Live and replay share one code path.** The poller only ever writes to disk;
everything downstream only ever reads from disk. They differ solely in whether
new files keep appearing — so a reviewer exercises the real pipeline.

---

## Repository layout

Code is organised by pipeline stage rather than in a single `src/`:

| Path | Contents |
|---|---|
| `ingest/poller/decode.py` | presence-aware GTFS-RT decoding — **the correctness core** |
| `ingest/poller/poller.py` | fetch, archive-first, rate budget, stale detection |
| `streaming/contracts.py` | Pydantic event contracts, partition key |
| `streaming/producer.py` | archive → Kafka replay producer |
| `streaming/consumer.py` | arrival resolver |
| `streaming/aggregator.py` | prediction-accuracy metrics |
| `ml/` | feature build, training, inference for the AI element |
| `evaluation/` | acceptance checks |
| `tests/` | 83 unit tests |
| `profiling/` | Week-0 feed profiler |
| `scripts/` | poller supervisor, replay-sample generator |
| `data/replay_sample/` | 13 MB committed sample — the offline review path |
| `outputs/` | produced metrics |
| `docs/` | onboarding, pitfall register, daily build reports |

### Course requirement → location

| Requirement | Where |
|---|---|
| Data source documented | `DATA_SOURCE.md` |
| Validated event contract | `streaming/contracts.py`, `tests/test_contracts.py` |
| Producer / replay script | `streaming/producer.py` |
| Kafka topics | `docker-compose.yml` (4 topics) |
| Consumer / stream processor | `streaming/consumer.py`, `streaming/aggregator.py` |
| Useful output | `outputs/` — 4 CSVs + summary JSON |
| Validation / evaluation | `evaluation/run_acceptance_checks.py` |
| Repeatable tests | `tests/` — 83 tests |
| Bounded AI element | `ml/`, documented in `AI_USAGE.md` |
| Sample / replay data | `data/replay_sample/` |
| Pinned dependencies | `requirements.txt` |
| One run command | `make demo` |

---

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
