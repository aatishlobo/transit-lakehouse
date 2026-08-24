# CLAUDE.md

Context for agents working in this repo. Read fully before the first edit.

---

## 1. What this project is

**DE#1 — Real-Time Transit Reliability Lakehouse.**

A streaming data platform measuring on-time performance for Bay Area transit
(BART, SF Muni, AC Transit, Caltrain), producing a public dashboard + read API,
built on Kafka → Spark Structured Streaming → Delta lakehouse → dbt → Dagster →
Kubernetes.

It is **not** primarily a dashboard project. The dashboard is the demo-able
artifact; the real deliverable is a gold fact table that is simultaneously
(a) correct for historical analysis and (b) a leak-free feature source for
downstream ML. See §7 — the portfolio strategy is load-bearing on design
decisions, not decoration.

**Owner:** graduate MSDS student, working data engineer (AWS/GCP/Databricks,
PySpark, Delta, dbt in production coursework and internship). Calibrate
explanations accordingly — no need to explain what a watermark or a MERGE is.
Do explain non-obvious GTFS domain semantics.

---

## 2. The central problem (read this before touching anything)

**GTFS-Realtime does not contain "actual arrival time."** It contains
*predictions* (`TripUpdates`) and *GPS positions* (`VehiclePositions`). The fact
"vehicle reached stop X at time T" must be **derived**.

Delay = actual − scheduled. Scheduled is exact. Therefore **every bit of error in
the arrival derivation lands directly in the downstream model's target variable.**

Three derivation methods, none authoritative:

| Method | Signal | Weakness |
|---|---|---|
| **A. Position state transition** | `current_status == STOPPED_AT` + `stop_id` | Not all agencies populate it |
| **B. Geofence** | Nearest approach of GPS trace to stop coords | Systematically biased *early* — measures closest approach, not stopping |
| **C. Prediction settlement** | Last `StopTimeUpdate` before pass; `uncertainty == 0` | Inherits the agency's prediction error; weak for buses in traffic |

**Design decision: do not pick one.** Run a precedence ladder and stamp
provenance on every row:

```
actual_arrival_ts           timestamp
arrival_method              stopped_at | geofence | tripupdate_settled | none
arrival_confidence          enum/float
arrival_method_agreement_s  |A - C| where both fired; nullable
poll_interval_s             sampling granularity at that moment
```

Why this shape:
- Label noise becomes **filterable** downstream rather than hidden.
- Where two methods both fire, their median disagreement is a **free bias
  estimate** to calibrate the weaker method against.
- Resolver mix varies by agency → without provenance, `agency` silently
  correlates with label bias and any model "learns" our own code.

**Bias matters more than variance here.** Random ±10s averages out. A geofence
firing 15s early shifts the whole delay distribution undetectably.

---

## 3. Non-negotiable invariants

Violating any of these silently corrupts data. None of them throw. If a change
would break one, stop and raise it rather than working around it.

**3.1 — Absent ≠ zero.** GTFS-RT is proto2. `HasField()` distinguishes "delay is
0" (on time) from "delay is absent" (no information). The naive attribute read
returns `0` for both. Every scalar read goes through `_get()` in
`ingest/poller/decode.py`. Never bypass it. Same trap on `current_status`: unset
naive-reads as `2` = `IN_TRANSIT_TO`, fabricating transit state.
`tests/test_decode.py::test_absent_delay_is_not_zero` is the load-bearing test in
this repo.

**3.2 — Grain is `(service_date, trip_id, stop_sequence)`.** Not `stop_id`. Loop
and circulator routes serve the same `stop_id` twice in one trip; a stop_id grain
collapses two distinct events into one. `stop_id` is an attribute.

**3.3 — `service_date` comes from `TripDescriptor.start_date`, never derived from
a timestamp.** GTFS service days run past midnight; `stop_times.arrival_time` of
`25:14:00` is valid and means 1:14am the following calendar day.

Corollary, measured 2026-08-05: **a payload has no service date at all.** One
live poll carried `start_date` 20260805 (58,971 rows), 20260806 (263 rows,
already past midnight), and 2,858 rows with no `start_date`; vehicle_positions
in the same payload ranged back to 20260730. Service date is a property of a
ROW. The raw archive therefore partitions on `ingest_dt=` (UTC date of the
poll — a genuine payload property), and true `service_date` partitioning starts
in bronze, after the explode. This was originally mislabeled `service_dt` while
filled from the poll clock, which filed every poll from 17:00 PT to midnight
under tomorrow's date.

**3.4 — `event_ts` ≠ `ingest_ts`, and both are stored.** Event time comes from
the producer (`vehicle.timestamp`, `trip_update.timestamp`), never from our poll
clock. There are four candidate timestamps (feed header, vehicle report, poll,
resolved arrival) — name columns unambiguously and document which is which.

**3.5 — All timestamps stored UTC.** `spark.sql.session.timeZone=UTC`. Convert to
`America/Los_Angeles` exactly once, in the serving layer. Hour-of-day truncation
in UTC produces an OTP-by-hour chart shifted 7–8 hours — the most user-visible
possible bug.

**3.6 — Scheduled-time arithmetic order.** Localize `service_date` midnight in
`America/Los_Angeles` → add duration-from-service-day-start → convert to UTC.
Doing it in UTC breaks on both DST transitions. Unit tests must pin both dates.

**3.7 — Idempotent writes on the gold grain.** At-least-once delivery +
idempotent MERGE, deliberately *not* exactly-once. Replays and duplicate
deliveries must converge to the same table.

**3.8 — Never bare `VACUUM` on gold.** Delta's default
`deletedFileRetentionDuration` is 7 days; the ML contract (§7) requires time
travel across the training window. Set `delta.logRetentionDuration` and
`delta.deletedFileRetentionDuration` explicitly and document the storage cost.

**3.9 — Archive raw bytes before decoding.** GTFS-RT history is unfetchable. A
decode failure quarantines the payload; it never loses it.

**3.10 — The poller is exactly one replica.** It is not a Kafka consumer, has no
lag, and N replicas means N× API calls against a rate-limited source. Scale
consumers on lag via KEDA; never the poller. (This corrects an error in the
original spec.)

---

## 4. Current state

**Built, tested, runnable: the full vertical, ingest to dashboard.**

```
ingest/poller/decode.py     presence-aware GTFS-RT decoding    <- correctness core
ingest/poller/poller.py     fetch, archive-first, rate budget, stale detection
ingest/static/fetch_static.py  511 GTFS-Static, content-addressed archive
streaming/                  Pydantic contracts, Kafka producer/consumer, resolver
spark/session.py            UTC, Delta, pinned retention, one metastore
spark/bronze.py             archive -> Delta, reuses decode.py unmodified
spark/silver.py             typed, deduped, quality-flagged
spark/gold.py               fct_stop_arrival, idempotent MERGE on the grain
spark/scheduled_time.py     GTFS noon-minus-12h arithmetic (pitfall 3.6)
spark/dim_schedule.py       SCD2 schedule dimension
spark/fct_otp.py            as-of join -> true on-time performance
dbt/                        4 models, 17 tests
orchestration/definitions.py Dagster asset graph + 3 asset checks
serving/                    FastAPI + dashboard over an exported SQLite
k8s/                        Strimzi, KEDA, CronJob, single-replica poller
ml/                         prediction-correction model (168.5s vs 194.7s MAE)
```

**Test counts:** 97 in the ingest venv (83 + 14 scheduled-time), 19 in the
Spark venv (13 lakehouse + 6 SCD2), 17 dbt tests.

**Measured on the current lake:** 36,753 arrivals across 13 agencies, 3,867,322
schedule rows, 99.5% as-of join match rate, ~49.6% on-time overall.

**Two environments, deliberately.** `.venv` is Python 3.13 for ingest/serving
and keeps the protobuf pin. `.venv-spark` is Python 3.12 + JDK 21 for
Spark 4/Delta 4/dbt/Dagster. Do not merge them.

**Not started:** DE#5's Flink consumer, MLE#1's online service, A/B layer,
RAG assistant. Spark reads are batch; the Structured Streaming read is the next
increment, and bronze was kept deliberately dumb so its checkpoint can survive
that change.

## 5. Immediate next step

The Week 0 gate is **closed**. The feed profile was run, resolver A confirmed
(15 of 28 operators publish `current_status`; Muni 99.2%, Caltrain 0%), and the
grain confirmed on `stop_sequence` (7 operators revisit a `stop_id` within one
trip). Findings live in section 6 of `docs/report.md`.

**Now open, in priority order:**

1. **Request a 511 rate-limit increase.** Still the highest-value single
   change: 60 req/hour caps polling at 120s, and that one constraint drives
   coverage (~21%), timestamp precision, and selection bias toward long dwells
   simultaneously. It is an email.
2. **Collect a second GTFS-Static snapshot.** The SCD2 dimension has exactly one
   version, so its versioning is proven only by unit tests, not by production
   data. A second snapshot makes the as-of join demonstrably load-bearing.
3. **Switch the Spark reads to Structured Streaming.** Bronze was written dumb
   and stable precisely so its checkpoint can survive the change.
4. **Benchmark against MTC's stop-observation dataset** (section 10) to convert
   "no ground truth" into "agrees with a reference implementation to within X".

## 6. Roadmap

| Stage | Content | Notes |
|---|---|---|
| **Now** | Ingest + archive | Done |
| **Next** | Kafka + Schema Registry | Schema informed by the profile, not guesses |
| | Spark bronze/silver | Bronze job must be **dumb and stable** — checkpoints couple to query plan, so all volatile logic lives in silver/gold |
| **Then** | SCD2 schedule dimension + as-of join | **The crown jewel.** 7 S1 pitfalls cluster here. Project stops being a toy at this stage |
| | Arrival resolver | Implements §2; benchmark against MTC's published stop-observations dataset |
| **After** | dbt marts, tests, freshness SLA | Incremental lookback must be tied to the streaming watermark in config so they can't drift |
| | Dagster orchestration | dbt must not run mid-static-ingest — model as asset edge, not schedule offset |
| | K8s (Strimzi + KEDA) | KEDA on consumer lag, not CPU; cap replicas at partition count |
| | Serving: FastAPI + dashboard | Small always-on tier; heavy infra stays ephemeral |
| **Last** | Freeze gold schema, finish ADRs | Gate before MLE#1 starts |

~60–90 focused hours total.

**Build ethos: thin slice first.** A working ugly vertical is resume-able the day
it works; a half-built ambitious thing is not. Do not add agencies, or depth,
before one narrow path is end-to-end.

**Anti-goal: ADRs instead of working code.** Ten ADRs and no pipeline is worth
less than an ugly pipeline and two ADRs.

---

## 7. Portfolio strategy — why design decisions here have downstream stakes

This project is the **shared substrate** for a multi-project portfolio. Design
choices that look like over-engineering in isolation are paying for reuse. An
agent should weight the ML contract heavily when making tradeoffs.

### The project graph

**Data Engineering**
- **DE#1 — this project.** Real-time transit reliability lakehouse.
- **DE#2** — CDC pipeline (Debezium / Kafka Connect). *Punches above its weight.*
- **DE#3** — medallion ELT with data contracts + quality gates.
- **DE#4** — Terraform-provisioned data platform (IaC spine). *Punches above its
  weight.* Provisions cloud infra for DE#1.
- **DE#5** — Flink windowed streaming analytics. **Reads DE#1's Kafka topics
  directly** for a stream-native OTP metric.

**Machine Learning Engineering**
- **MLE#1** — real-time arrival-time prediction service. **Consumes DE#1's gold
  table.** The point-in-time contract below exists for this.
- **MLE#2** — agentic analytics layer (RAG + agents, text-to-SQL). **Queries
  DE#1's gold tables; retrieves over the `Alerts` feed.** *Hottest
  signal-per-hour in the whole portfolio.*
- **MLE#3** — real-time object detection + tracking. Standalone flagship.
- **MLE#4** — two-stage recommender with contextual bandit. Standalone flagship.
- **MLE#5** — distributed LoRA fine-tuning + vLLM inference optimization. *Also
  hot.* Shares the DE#1/DE#4 infra substrate.

**Net effect: DE#1 + DE#4 yield roughly ten resume lines from about four real
builds.** Table-stakes capabilities whose absence is disqualifying — lakehouse,
dbt, orchestration — all land in DE#1.

### The DE↔ML contract (`docs/architecture.md`)

MLE#1 predicts arrival delay. For it to be trainable **without leakage**, gold
must satisfy three properties. These are requirements, not preferences:

1. **Append-only with a true event timestamp.** `event_ts` ≠ `ingest_ts` (§3.4).
   Training on `ingest_ts` leaks.
2. **Stable grain + idempotent identity.** One row per
   `(service_date, trip_id, stop_sequence)`. Combined with idempotent writes and
   Delta time travel, MLE#1 can reconstruct "what did this table look like as of
   prediction time T." **This is the single most valuable correctness property in
   the entire portfolio** — and §3.8 (`VACUUM`) is how it gets destroyed by
   accident.
3. **Context columns computed causally.** Upstream delay on the same trip,
   rolling route delay, hour/day/holiday — all must be computable strictly
   *before* the prediction point. Provide as as-of aggregates in gold, or leave
   raw material for MLE#1 — but **document which**, so training/serving skew is a
   decision rather than an accident.

### 7.3 — The leakage trap specific to this project

If `actual_arrival_ts` is derived from resolver C (settled TripUpdate
predictions), and MLE#1 also uses TripUpdate predictions as *features*, the model
learns to copy the agency's prediction. Result: an excellent MAE that means
nothing.

Defenses: (a) the agency's own prediction is a **baseline to beat**, not a
feature — unless deliberately building a prediction-correction model, in which
case say so loudly; (b) prefer position-derived labels for training rows so label
and features have independent provenance.

This is subtle, project-specific, and the sort of thing that gets caught in an
interview if it isn't caught first.

### Framing priorities

Per USF MSDS panel advice: **prioritize end-to-end, demo-able products** — live
deployed apps, interactive dashboards, publicly accessible outputs — over
pipeline-only or notebook-only work. When a tradeoff arises between internal
elegance and something a recruiter can click, choose the clickable thing.

Corollary tension to resolve deliberately: the public demo must be **always-on
and cheap** (static frontend + small API over a compact aggregate table), while
Kafka/Spark/K8s stay **ephemeral**. Which means the dashboard must degrade
gracefully and *label its own staleness* — unlabeled stale data on a live demo
reads as a broken toy.

---

## 8. Working agreements

**Testing**
- Every correctness invariant in §3 gets a test. Prefer tests that fail loudly
  over comments that hope.
- `tests/make_fixture.py` generates synthetic multi-agency data with deliberately
  *different* field population — use it; don't require live API access to test.
- Tests that pass on empty tables are worse than no tests. Include row-count
  floors.

**Invariant harness** — encode rather than remember. These belong in dbt tests or
Dagster asset checks and cover most S1 pitfalls:
1. Grain uniqueness on `(service_date, trip_id, stop_sequence)`
2. Join conservation — fact row count identical before/after the as-of join
3. Idempotence — reprocessing a bronze window twice yields identical gold
4. Referential integrity — every gold `trip_id` resolves to exactly one dim
   version; orphan count tracked and trended
5. Timezone anchors — both DST transitions + a past-midnight scheduled time
6. Coverage — per-agency-per-hour observed-stop counts, alert on drops
7. Watermark liveness — monitored as a metric, not inferred from stream health
8. Delay distribution stability — catches fake on-time spikes, resolver changes,
   and composition changes at once
9. Null-zero discrimination — assert no delay column holds `0` where the source
   field was absent

**Documentation**
- Significant decisions become one-page ADRs in `docs/decisions/`.
- Write the ADR *before* building where feasible — it forces the reasoning to be
  real.
- The DE↔ML contract lives in `docs/architecture.md`.

**Code**
- Comments explain *why*, especially where a line encodes a correctness
  invariant. Reference pitfall numbers.
- Prefer explicit nulls over defaulted zeros, everywhere.
- Schema evolution off on production write paths — `mergeSchema` silently accepts
  typo'd columns.

**Things to raise rather than silently work around**
- Any change touching §3.
- Any use of `stop_id` as an identity rather than an attribute.
- Any timestamp arithmetic not going through the §3.6 order.
- Any feature or aggregate whose window could include the prediction point.
- Adding a second agency before one works end-to-end.

---

## 9. Commands

```bash
make venv        # isolated venv (do not share env with mlflow/spark)
make test        # pytest
make fixture     # synthetic archive, no API key needed
make profile     # Week 0 evidence generator
make poll-once   # single poll cycle (needs API_511_KEY)
make poll        # continuous poller
```

Env: copy `.env.example` → `.env`. Key from https://511.org/open-data/token.
Use the **consolidated regional feed** (`agency=RG`) — one poll covers all Bay
Area agencies, which is what makes the rate limit survivable. Pull GTFS-static
from 511 too, never from an agency directly; the RT feeds match 511's static IDs
and are not guaranteed to match an agency's own.

---

## 10. Reference

- `docs/PITFALLS.md` — full 60-item register, tiered S1/S2/S3, organized by
  layer. **Consult before starting any new layer.** The S1s are the ones that
  produce plausible wrong numbers rather than crashes.
- `README.md` — quickstart and the Week 0 decision workflow.
- `docs/decisions/` — ADRs.
- MTC / Interline publish monthly **stop-observation datasets** derived from the
  same regional feed. Their method is public. This is our validation baseline —
  it converts "we have no ground truth" into "we benchmarked against a reference
  implementation."
