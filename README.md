# transit-lakehouse

**Deriving transit arrival times that no agency publishes — then grading the
schedule and the predictions against them.**

> Data provided by [511.org](http://www.511.org) — Metropolitan Transportation
> Commission (MTC).

---

## The problem

Transit agencies publish two live feeds through GTFS-Realtime:

- **TripUpdates** — *predictions*: "the vehicle on trip X will reach stop 22 at 15:07"
- **VehiclePositions** — *GPS observations*: "vehicle 4821 is at this coordinate, now"

Neither ever states **"vehicle 4821 arrived at stop 22 at 15:09."** The fact of
arrival is not published by anyone. It has to be **derived**.

That matters more than it first looks. Delay is `actual − scheduled`, and
`scheduled` is published and exact. So **every error in deriving `actual` lands
directly in the answer**, and nothing downstream can detect or correct it. The
danger is *bias*, not noise: random error averages out across a route, while a
systematic offset shifts an entire distribution invisibly.

This project derives arrivals from vehicle state transitions, then answers two
questions nobody publishes an answer to:

| Question | Needs |
|---|---|
| When an agency says *N* minutes, how wrong is it? | realtime feeds only |
| Is the vehicle on time against the **published timetable**? | GTFS-Static + SCD2 dimension + as-of join |

---

## Architecture

Three tiers. The **archive** is the system of record; everything else is
rebuildable from it.

### 1 · Ingest — the only irreplaceable part

```
   511 GTFS-Realtime                        511 GTFS-Static
   protobuf · 60 req/hr                     68 MB zip
          │                                        │
          │  poller — EXACTLY 1 replica            │  content-addressed:
          │  archive bytes BEFORE decoding         │  unchanged feed = no-op
          ▼                                        ▼
   ┌──────────────────────┐               ┌──────────────────────┐
   │  data/raw/           │               │  data/static/        │
   │  ingest_dt=…/*.pb.gz │               │  fetched_dt=…/*.zip  │
   └──────────────────────┘               └──────────────────────┘
        SYSTEM OF RECORD — GTFS-RT has no history endpoint.
        A poll not taken is gone permanently.
```

### 2 · Two paths out of the archive

```
   ┌───────────── STREAMING PATH ─────────────┐   ┌──── LAKEHOUSE PATH ────┐
   │                                          │   │                        │
   │  Pydantic contract  ──► dead-letter      │   │  Spark Structured      │
   │         │               (invalid)        │   │  Streaming             │
   │         ▼                                │   │  readStream + ckpt     │
   │  Kafka · 4 topics · 3 partitions         │   │         │              │
   │  key = service_date:trip_id              │   │         ▼              │
   │  cleanup=delete · 24 h retention         │   │   BRONZE  (Delta)      │
   │         │                                │   │   decode + explode     │
   │         ▼                                │   │         │              │
   │  arrival resolver                        │   │         ▼              │
   │  STOPPED_AT transitions                  │   │   SILVER  (Delta)      │
   │         │                                │   │   typed · deduped      │
   │         ▼                                │   │         │              │
   │  prediction-accuracy metrics             │   │         ▼              │
   │                                          │   │   GOLD  fct_stop_      │
   └──────────────────────────────────────────┘   │        arrival         │
                                                  │   idempotent MERGE     │
     Kafka is TRANSPORT, not storage.             └────────┬───────────────┘
     24 h retention makes it impossible to                 │
     drift into treating a topic as a database.            │
```

### 3 · Schedule join, marts, serving

```
   staging/gtfs_stop_schedule  ──►  dbt snapshot  ──►  dim_stop_schedule
   (Spark: CSV → Delta)             STRATEGY=check     SCD2 · 3.87 M rows
                                                              │
   gold/fct_stop_arrival ────────────► AS-OF JOIN ◄────────────┘
                                    valid_from ≤ as_of < valid_to
                                            │
                                            ▼
                                   gold/fct_stop_otp
                                   join conservation ASSERTED
                                            │
                                            ▼
                                dbt marts · 17 data tests
                                  (UTC → local, exactly once)
                                            │
                                            ▼
                            SQLite export ──► FastAPI + dashboard
                                              labels its own staleness

        ── all of it orchestrated by Dagster ──
        dbt is an ASSET EDGE, never a schedule offset
```

**Why that last line matters.** The tempting design is two cron schedules with
an offset — ingest at `:00`, dbt at `:30`. That works until ingest runs long,
and then dbt reads a half-written dimension and produces marts that are wrong
without being *obviously* wrong. Nothing errors; the numbers just move.

---

## Five decisions worth defending

| Decision | Rejected alternative | What it cost |
|---|---|---|
| Archive is the system of record; Kafka expires in 24 h | Kafka as durable storage | Rebuilding topics needs a replay |
| Arrivals from **positions only** (resolver A) | Prediction settlement — far better coverage | ~21 % capture; only 15 of 28 operators |
| Grain is `(service_date, trip_id, stop_sequence)` | Keying on `stop_id` | Wider key; 7 operators revisit a stop mid-trip |
| **SCD2** dimension + as-of join | Join to the *current* schedule | A dimension that grows, and a slower join |
| At-least-once + idempotent MERGE | Kafka transactions / exactly-once | Duplicates must be provably harmless |

The fourth looks like over-engineering and is not. Joining facts to the current
schedule silently rewrites history every time an agency republishes a timetable.

---

## Quick start

Two environments, deliberately separate. Spark 4 needs **Java 17+** and Python
≤ 3.12; the ingest service runs 3.13 and pins protobuf for the ML stack.

```bash
make venv          # ingest + serving        (python3, protobuf pinned)
make venv-spark    # Spark/Delta/dbt/Dagster (python3.12 + JDK 21)
```

**Kafka demo** — deterministic replay, no API key:

```bash
make demo          # replay → Kafka → resolver → metrics → acceptance checks
```

**Lakehouse** — archive through to dashboard:

```bash
make static        # fetch GTFS-Static            (needs API_511_KEY)
make full          # bronze → silver → gold → snapshot → otp → marts → export
make serve         # dashboard at http://localhost:8000
```

**Streaming bronze** — the same decode, driven by `readStream`:

```bash
make bronze-stream            # availableNow: consume the backlog, then stop
make bronze-stream-continuous # stay up, pick up each new poll
```

**Tests**

```bash
make test          # 116 tests: decode, contracts, resolver, ML leakage, DST
make lake-test     # lakehouse invariants + SCD2 semantics
make dbt-test      # 17 dbt data tests
make k8s-validate  # manifests parse
```

---

## Correctness: what this project is actually about

Six invariants. Each corrupts data **silently** if broken — none of them throw.

**1 · Absent is not zero.** GTFS-Realtime is proto2: a field can be genuinely
absent, which differs from being zero. `delay = 0` means *exactly on time*;
absent means *no information*. Read naively, protobuf returns `0` for both.

> **Measured: 44.2 % of records carry no delay at all.** Written the obvious
> way, this project would report ~54,000 records per poll as perfectly on time —
> a fabricated punctuality spike that looks entirely plausible. Verified against
> raw protobuf across 408,149 rows, zero violations.

**2 · The grain is `(service_date, trip_id, stop_sequence)`** — never `stop_id`.
Seven of 28 operators run trips revisiting the same stop; a `stop_id` grain
merges two real events into one.

**3 · `service_date` comes from the trip, never from a clock.** Service days run
past midnight. Relatedly the raw archive partitions on `ingest_dt`: one payload
contains trips from *several* service dates, so service date is a property of a
**row**, not a file.

**4 · Event time comes from the producer, never our poll clock.** A stale feed
would otherwise manufacture observations that never happened.

**5 · Timestamps stored UTC, converted exactly once** — in `stg_stop_otp`, at
the serving boundary. Truncating hour-of-day in UTC shifts every bar of an
hourly chart by 7–8 hours while leaving totals correct.

**6 · Scheduled times use the GTFS noon-minus-12h anchor.** Not midnight —
midnight can be skipped or repeated by a DST transition, noon never is. Pinned
by tests on **both** 2026 transitions, because the naive version errs in
*opposite directions* on each.

Full register: [`docs/PITFALLS.md`](docs/PITFALLS.md) — 60 known failure modes.

---

## Results

**797,943 derived arrivals** across 27 service days (5–31 Aug 2026) and 11 operators.

### On-time performance vs the published timetable

Derived arrival compared to the schedule version in force that service day.
"On time" is asymmetric — 60 s early to 5 min late — because an early bus and a
late bus are different failures, and an early one you miss entirely.

| Operator | Arrivals | Median | On time | Early | Late |
|---|---:|---:|---:|---:|---:|
| SA | 2,781 | +121 s | 61.7 % | 12.0 % | 26.3 % |
| WH | 11,546 | +112 s | 59.6 % | 20.2 % | 20.2 % |
| CC | 28,897 | +95 s | 56.4 % | 25.8 % | 17.8 % |
| MA | 11,303 | +102 s | 53.0 % | 22.8 % | 24.2 % |
| SC | 173,889 | +32 s | 52.8 % | 31.5 % | 15.8 % |
| FS | 1,779 | −5 s | 52.6 % | 36.3 % | 11.2 % |
| SF | 419,838 | +66 s | 50.7 % | 27.9 % | 21.4 % |
| AC | 60,702 | +124 s | **32.0 %** | 29.7 % | **38.3 %** |
| SB | 2,151 | −384 s | 20.0 % | 75.0 % | 5.0 % |
| AM | 1,185 | −858 s | 4.7 % | 84.7 % | 10.5 % |

The bus operators read credibly. **SB and AM do not** — see the open issue under
[Limitations](#limitations--read-before-quoting-any-number) before quoting any
blended figure.

Note that roughly a quarter of arrivals are *more than a minute early*. Since the
labels are biased **late**, the true early rate is higher still — that is schedule
padding, and it is why every mart reports early and late separately rather than
folding them into one "off-schedule" number.

### Prediction accuracy

How wrong the agency's own ETA is (SF Muni, 25,501 prediction–arrival pairs):

```
   lead time       n     bias  med|err|     p90    <60s   <180s
     0–2 min     509      -11        13      52   92.7%  100.0%
     2–5 min    4603      -46        50     112   59.5%   98.4%
     5–10 min   4897      -66        70     172   43.3%   91.2%
    10–20 min   8919      -98       102     257   31.1%   77.0%
      20+ min   6573     -142       146     375   23.0%   59.3%
```

Short-horizon predictions are excellent; accuracy degrades steadily with
horizon, and the bias is negative throughout — vehicles arrive **later** than
promised.

A bounded ML model corrects those predictions, cutting error **13.5 %** below the
agency's own ETA (168.5 s vs 194.7 s MAE), beating three explicit baselines.

---

## Limitations — read before quoting any number

**Coverage is ~21 % of stop events.** Vehicles dwell for seconds; the 60 req/hr
quota caps polling at 120 s. 85.7 % of captured arrivals appear in exactly one poll.

**Derived arrivals are biased late.** A vehicle is observable as `STOPPED_AT`
only *between* arriving and departing, so timestamps fall at or after true
arrival — never before.

**Long dwells are over-represented.** Capture probability scales with dwell
time, so terminals and layovers are oversampled.

**Vehicle clocks drift.** 3.7 % of arrivals carry a vehicle timestamp *ahead* of
the poll that observed them, by up to 97 s.

> Effects 2–4 push the same direction, so every bias figure here is an **upper
> bound on agency optimism**, not a point estimate.

**⚠️ Open issue — rail operators.** SB (−384 s median, 75.0 % early) and AM
(−858 s, 84.7 % early) report impossible early arrivals while every bus operator
looks credible. A median arrival fourteen minutes ahead of schedule is not a real
transit pattern. Because the arrival side of the pipeline is shared with the
operators that look fine, this is almost certainly schedule matching on rail
`trip_id`s rather than a resolver fault. **Report per-operator with the anomaly
called out; do not quote a blended on-time figure until it is resolved.**

**The ML model is an afternoon model.** 95.8 % of training rows fall in hours
14–19, because the archiving machine sleeps overnight.

---

## Repository layout

| Path | Contents |
|---|---|
| `ingest/poller/decode.py` | presence-aware GTFS-RT decoding — **the correctness core** |
| `ingest/poller/poller.py` | fetch, archive-first, rate budget, stale detection |
| `ingest/static/` | 511 GTFS-Static fetch, content-addressed archive |
| `streaming/` | Pydantic contracts, Kafka producer/consumer, resolver, aggregator |
| `spark/bronze_stream.py` | **Structured Streaming** bronze — `readStream` + checkpoint |
| `spark/bronze.py` · `silver.py` · `gold.py` | batch medallion, idempotent MERGE |
| `spark/scheduled_time.py` | GTFS noon-minus-12h arithmetic; both DST transitions |
| `spark/stage_gtfs.py` | GTFS CSV → Delta staging for the dbt snapshot |
| `spark/fct_otp.py` | as-of join → true on-time performance |
| `dbt/snapshots/` | **SCD2 schedule dimension** (`strategy='check'`) |
| `dbt/models/` | 4 models, 17 data tests |
| `orchestration/definitions.py` | Dagster assets + asset checks |
| `serving/` | SQLite export, FastAPI, dashboard |
| `k8s/` | Strimzi, KEDA lag autoscaling, single-replica poller |
| `ml/` | feature build, training, inference (leakage defences) |
| `tests/` | 116 tests across two interpreters |

---

## Data and licensing

Source: **511 SF Bay Open Data**, Metropolitan Transportation Commission.
Governed by the 511 Data Disseminator Agreement. §5(b) attribution is met here,
in `DATA_SOURCE.md`, and programmatically in every generated artifact.

Full documentation of schema, rights, rate limits and replay:
[`DATA_SOURCE.md`](DATA_SOURCE.md).

---

## Documentation

| Doc | Contents |
|---|---|
| [`docs/PITFALLS.md`](docs/PITFALLS.md) | 60-item failure register, tiered S1/S2/S3 |
| [`docs/PIPELINE_WALKTHROUGH.md`](docs/PIPELINE_WALKTHROUGH.md) | end-to-end explainer |
| [`AI_USAGE.md`](AI_USAGE.md) | the bounded AI element + verification |
| [`docs/decisions/`](docs/decisions/) | ADRs |
