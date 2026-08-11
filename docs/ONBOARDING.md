# Onboarding — transit-lakehouse

**For a teammate joining cold.** Read this end to end before touching code. It
takes about 25 minutes and will save you a day.

Written 10 August 2026. Course deliverable due **Friday 14 August**.

---

## Table of contents

1. [The 60-second version](#1-the-60-second-version)
2. [The problem, properly explained](#2-the-problem-properly-explained)
3. [Get it running in 5 minutes](#3-get-it-running-in-5-minutes)
4. [What exists — a guided tour of the code](#4-what-exists--a-guided-tour-of-the-code)
5. [The invariants you must not break](#5-the-invariants-you-must-not-break)
6. [What we learned from the real data](#6-what-we-learned-from-the-real-data)
7. [The AI element](#7-the-ai-element)
8. [Course requirements: done vs remaining](#8-course-requirements-done-vs-remaining)
9. [Where you can pick up](#9-where-you-can-pick-up)
10. [Things that will bite you](#10-things-that-will-bite-you)
11. [Glossary](#11-glossary)

---

## 1. The 60-second version

We measure how late Bay Area transit actually is, by deriving arrival times that
no agency publishes, and we do it through a Kafka streaming pipeline.

**Status:** the whole path works end to end. One command (`make demo`) takes raw
archived transit data through Kafka, derives arrivals, produces metrics, and runs
acceptance checks — in about two minutes, with no API key.

**Numbers so far:** 83 tests passing, 6 acceptance checks passing, 1.1 GB of live
data archived over 6 days, and a machine-learning model that improves the
agency's own ETA accuracy by 14.7%.

**What's left:** three documents, a report, and a presentation. No major code.

---

## 2. The problem, properly explained

### Transit agencies do not publish when buses arrive

They publish two live feeds:

- **TripUpdates** — *predictions*. "Bus on trip X will reach stop 22 at 15:07."
- **VehiclePositions** — *GPS pings*. "Bus 4821 is at this lat/lon, right now."

Nowhere does any feed say *"bus 4821 arrived at stop 22 at 15:09."* That fact has
to be **derived**.

### Why that makes the project hard rather than tedious

Delay = actual − scheduled. The scheduled time is published and exact. So **every
error in our derived "actual" lands directly in the final delay number**, and
nothing downstream can correct it.

There are three ways to derive an arrival, none authoritative:

| Method | Signal | Weakness |
|---|---|---|
| **A — position state** | Vehicle reports `current_status = STOPPED_AT` at a stop | Only some agencies publish it |
| **B — geofence** | Nearest approach of the GPS trail to the stop | Measures closest approach, not stopping — biased early |
| **C — prediction settlement** | Take the last prediction before the bus passed, call it truth | Inherits the agency's own errors |

**We implement A only**, and we stamp every derived arrival with *which method
produced it*. That provenance is not bookkeeping: which method is available
varies by agency, so without it, "which agency" silently becomes a proxy for
"how accurate our data is" — and any later analysis would discover agency
differences that are really artifacts of our own code.

### What we actually produce

Since we can derive real arrivals, we can grade the agency's own predictions:

> *"When Muni says the bus arrives in 10 minutes, how wrong is it?"*

That's the deliverable. And the answer is only meaningful because the two halves
come from **different feeds** — predictions from TripUpdates, actuals from
VehiclePositions. Had we used method C, we'd be comparing the agency's
predictions against its own predictions and getting a beautiful, meaningless
number.

---

## 3. Get it running in 5 minutes

**Prerequisites:** Python 3.11+, Docker Desktop running. No API key needed.

```bash
git clone <repo-url> && cd transit-lakehouse
make venv && source .venv/bin/activate
make demo
```

`make demo` does all of this:

1. starts Kafka in Docker and creates 4 topics
2. replays 39 minutes of real archived transit data into Kafka (1.67M records)
3. derives arrival events from vehicle positions
4. joins them against predictions and writes metrics
5. runs the acceptance checks

Takes about 2 minutes 15 seconds cold. You should see:

```
   lead time       n     bias  med|err|     p90    <60s   <180s
     0-2 min     509      -11        13      52   92.7%  100.0%
     ...
passed=6  failed=0  unmeasured=0  info=1
```

Other useful commands:

```bash
make test            # 83 unit tests, ~2 seconds
make evaluate        # acceptance checks only
make train           # retrain the ML model from the shipped sample
make predict         # demo one corrected ETA
make kafka-down      # stop Kafka (safe — it holds nothing irreplaceable)
make poll-status     # is the live archiver running?
```

If `make demo` fails, it's almost always Docker not running. Check with
`docker info`.

---

## 4. What exists — a guided tour of the code

~5,300 lines across four layers. Read them in this order.

### Layer 1 — Ingest (`ingest/poller/`)

**`decode.py`** — *the correctness core. Read this first.*

Turns raw protobuf into flat rows. Its entire reason for existing is one rule:
**an absent field is not zero.** More on this in §5; it's the single most
important idea in the repo.

**`poller.py`** — fetches from 511 every 2 minutes and writes to disk.

Key behaviours: archives raw bytes *before* attempting to decode (a decode bug is
recoverable, a missed poll is not); enforces the rate limit client-side; detects
stale feeds; redacts the API key from logs; retries dead connections while still
counting each attempt against the budget.

**`scripts/run_poller.sh`** — supervisor with auto-restart and a single-instance
lock.

### Layer 2 — Streaming (`streaming/`)

**`contracts.py`** — Pydantic models defining what a valid event looks like.
Every record is validated before publishing; failures go to a dead-letter topic
rather than crashing the pipeline. Also defines the Kafka message key.

**`producer.py`** — reads the archive, validates, publishes to Kafka.
Merges both feeds into one time-ordered stream (see §10 for why that matters).

**`consumer.py`** — *the arrival resolver.* Watches vehicles change state and
emits an arrival on the **first** `STOPPED_AT` sighting at each stop.

**`aggregator.py`** — joins derived arrivals against agency predictions and
writes the metrics in `outputs/`.

### Layer 3 — Machine learning (`ml/`)

**`build_features.py`** — builds the training table from the archive, two-pass to
bound memory.
**`train.py`** — trains and evaluates against three baselines, temporal split.
**`predict.py`** — inference, with fallback behaviour enforced in code.

### Layer 4 — Validation

**`tests/`** — 83 unit tests across 6 files.
**`evaluation/run_acceptance_checks.py`** — 6 checks against real produced data.
**`profiling/profile_feed.py`** — the Week-0 feed profiler that produced §6.

### Documents you should also read

| File | What it is |
|---|---|
| `CLAUDE.md` | Project constitution — invariants, roadmap, rationale |
| `docs/PITFALLS.md` | 60 known failure modes, tiered by severity |
| `docs/reports/*.md` | Day-by-day build log, written for non-specialists |
| `docs/OVERVIEW.md` | Short project summary |

The daily reports are genuinely the fastest way to understand *why* things are
the way they are. Start with `2026-08-05`.

---

## 5. The invariants you must not break

Every one of these corrupts data **silently**. None throw. If a change would
break one, stop and raise it rather than working around it.

### 5.1 — Absent is not zero. This is the big one.

GTFS-Realtime uses protobuf version 2, where a field can be genuinely *absent*,
which is different from being *zero*.

- `delay = 0` means **exactly on time**.
- `delay` absent means **no information**.

The trap: the natural way to read the field returns `0` for both. You must
explicitly ask "was this field set?"

**Measured on real data: 44.2% of records have no delay at all.** Written the
obvious way, this project would have reported ~54,000 records per poll as
perfectly on time — a fabricated spike that looks completely plausible and
corrupts every downstream number.

Everything goes through `_get()` in `decode.py`. Never bypass it.
`tests/test_decode.py::test_absent_delay_is_not_zero` is the load-bearing test.

### 5.2 — The grain is `(service_date, trip_id, stop_sequence)`

Not `stop_id`. Loop routes visit the same stop twice in one trip — 7 of 28
operators do this, one on 23 of its 29 trips. A `stop_id` grain silently merges
two real events into one.

### 5.3 — `service_date` comes from the trip, never from a clock

Transit service days run past midnight; a 00:40 trip usually belongs to the
*previous* service date. Deriving it from a timestamp is wrong.

Related: the raw archive partitions on `ingest_dt` (when *we* downloaded it), not
service date — because a single downloaded file contains trips from **several**
service dates. Service date is a property of a row, not a file.

### 5.4 — Event time comes from the producer, not our clock

`vehicle_report_ts` is when the vehicle measured its position. Our download time
is recorded separately as `ingest_ts` and is never used as event time.

### 5.5 — All timestamps stored UTC, converted once at the end

Truncating hour-of-day in UTC shifts every bar of an hourly chart by 7–8 hours
while leaving every total correct. Conversion happens once, in `aggregator.py`.

### 5.6 — The poller runs as exactly one instance

Counter-intuitive but important: it's not a Kafka consumer, has no lag to divide,
and two copies simply make twice the API calls against a 60/hour limit — getting
the token throttled. Enforced by a lock file, not just documented.

### 5.7 — Kafka is transport, not storage

Topics expire after 24 hours *on purpose*. The permanent record is `data/raw/`.
Replays run from disk. Never treat a topic as a database.

### 5.8 — Never commit secrets, and check the error paths

The API key leaked into `poller.log` because `requests` puts query parameters in
exception messages. It was caught one command before a public push. Logs are now
gitignored and the key is redacted at source.

---

## 6. What we learned from the real data

These came from profiling the live feed and are why several decisions look the
way they do.

**The regional feed carries 28 operators**, not the 4 the project originally
assumed (BART, Muni, AC Transit, Caltrain).

**44.2% of records have no delay information.** Nineteen of 28 operators never
publish it at all — AC Transit sent 23,469 consecutive records with delay absent.

**Method A works for only 15 of 28 operators.** Muni publishes the needed fields
99.2% of the time; Caltrain, 0%.

**Method C is effectively dead** — only 4 of 28 operators emit settled
predictions, at trivial volume. Muni never publishes uncertainty at all. This is
why the resolver implements A only.

**We capture roughly 21% of stop events.** Buses dwell at stops for seconds; we
sample every 120 seconds. 90.3% of captured stop events appear in exactly one
poll. This is a coverage limitation, and it's the strongest argument for
requesting a higher rate limit.

**Our derived arrivals are biased late, and over-sample long-dwell stops.** A
vehicle is only visible as `STOPPED_AT` between arriving and departing, so a
detected timestamp always falls at or after the true arrival. And capture
probability scales with dwell time, so terminals and layovers are
over-represented — median observed dwell among multi-poll sightings is 353
seconds.

**Consequence: the headline result is an upper bound, not a point estimate.**
Muni's predictions are systematically optimistic, but part of that measured
optimism is our own measurement bias. Say this out loud in the report; don't let
someone else find it.

---

## 7. The AI element

The course requires one bounded AI component. Ours is a **prediction-correction
model**: given the agency's ETA plus context, predict how wrong it will be.

```
target = actual_arrival − agency_predicted_arrival
```

Doing nothing equals predicting zero, so "trust the agency" is an exact baseline.

**Results** (1.0M training rows, 336k tested, strictly forward in time):

```
                            MAE     RMSE     bias
baseline_agency           195.2    381.9   -154.0
baseline_global_bias      215.1    358.2    +78.5
baseline_horizon_bias     206.7    359.2    +74.8
model                     166.6    305.6    +43.7      ← 14.7% better
```

Note both naive bias corrections are **worse** than trusting the agency —
residuals are heavily right-skewed, so adding the mean overcorrects the typical
case. The model earns its place by learning a horizon-, hour- and route-dependent
correction.

**Three things to understand if you're asked about this:**

**It's declared a correction model deliberately.** Using the agency's forecast as
a feature is normally leakage. It's safe here for a structural reason: labels come
from VehiclePositions, features from TripUpdates. Different feeds. The label
cannot contain the feature.

**`lead_time` is deliberately excluded.** It's used in evaluation and would be a
natural copy-paste into the features — but `lead_time = actual − issued` contains
the target. It would produce a spectacular score and a model that cannot run.

**We removed a feature that improved the score.** `dow` (day of week) cut MAE by
10 seconds, but Monday appears *only* in the test window — 31% of test rows had a
value the model never saw. Those rows silently inherit Wednesday's correction, so
the gain was a coincidence of bin placement. A number you can't account for isn't
a result.

**Known limitation:** 95.8% of training data falls in hours 14–19, because the
laptop running the archiver sleeps overnight. This is effectively an
afternoon/evening model.

---

## 8. Course requirements: done vs remaining

### Done

| Requirement | Where |
|---|---|
| Data source documented | `docs/` + §6 here (needs formalising, see below) |
| Validated event contract | `streaming/contracts.py` |
| Producer / replay script | `streaming/producer.py` |
| Kafka topics | `docker-compose.yml`, 4 topics |
| Consumer / stream processor | `streaming/consumer.py`, `aggregator.py` |
| Useful output | `outputs/` — 4 CSVs + summary JSON |
| Validation evidence | `evaluation/` — 6 acceptance checks |
| Repeatable tests | `tests/` — 83 tests |
| Bounded AI element | `ml/` — model, artifact, metrics |
| Sample / replay data | `data/replay_sample/` — 13 MB, 39 min |
| Pinned dependencies | `requirements.txt` |
| One run command | `make demo` |
| No credentials committed | verified |

### Remaining

| Item | Owner | Notes |
|---|---|---|
| `DATA_SOURCE.md` | **available** | Source, owner, link, access, rights, schema, rate limits, replay |
| `AI_USAGE.md` | **available** | AI task, input/output, accepted/rejected, verification, limitations, fallback |
| `README.md` rewrite | **available** | Currently describes Week 0 only — badly out of date |
| `report.pdf` | | Thu |
| Presentation | | Fri |

The three documents are the critical path. All three are writing tasks, not code.

---

## 9. Where you can pick up

Ordered by value, and by how independent they are.

**1. `DATA_SOURCE.md`** *(highest value, fully self-contained)*
Everything you need is in `docs/reports/2026-08-05-session-report.md` §6 and
`profiling/profile_feed.py` output. Must cover: source and owner (511.org / MTC),
link, access model (free key), rights and redistribution terms — **check the 511
data agreement, we haven't** — schema, rate limits, and how replay works.

**2. `AI_USAGE.md`** *(high value, self-contained)*
Must cover: the task AI owns, representative input/output, what was accepted or
rejected, how it was verified, limitations and fallback. §7 above plus
`ml/artifacts/training_report.json` has the substance. It should cover *both* the
model and disclosure of AI-assisted development.

**3. `README.md` rewrite** *(needed for submission)*
Must map every course-required item to its location, since our layout uses
`ingest/`+`streaming/`+`ml/` rather than `src/`.

**4. Optional: GTFS-Static and true on-time performance**
Currently we measure prediction accuracy (actual vs *predicted*). With the static
schedule we could measure true OTP (actual vs *scheduled*). Real scope — needs a
new data source and an as-of join. Only if everything else is done.

**5. Optional: a second agency**
Everything is scoped to SF Muni. VTA (`SC`) had 1,486 arrivals in the sample and
would be the natural second. Don't start this before the documents are finished.

---

## 10. Things that will bite you

**Docker isn't running.** Most `make demo` failures. Check `docker info`.

**Kafka's two addresses.** Containers reach it as `kafka:29092`, your Mac as
`localhost:9092`. A client doesn't keep the address it dialed — it asks the
broker where to connect and uses *that*. Get it wrong and things hang with no
error. Already configured; don't "simplify" it.

**Replaying twice doubles your counts.** It used to. The aggregator now dedupes,
but if numbers look inflated, run `make kafka-down` first for a clean topic.

**The archiver stops when the laptop sleeps.** Coverage is 95.8% concentrated in
hours 14–19 for exactly this reason. If you take over archiving, plug in and run
`caffeinate -dimsu`.

**`make fixture` used to delete the live archive.** Fixed — it writes to
`fixture_data/` now — but be aware `data/` holds 1.1 GB of unrepeatable history.
`make clean` no longer touches it, and deleting it requires
`make clean-archive CONFIRM=yes`.

**Don't use `stop_id` as an identity.** See §5.2. It's an attribute.

**Don't add a `Retry` adapter to the poller's HTTP session.** It seems obviously
right and would break the rate budget: urllib3 retries *below* our counter, so
three silent retries would make four requests counted as one, and the token gets
throttled. The retry is hand-rolled at the level where every attempt is counted.

**Tests that pass on empty data.** Every acceptance check has a row-count floor
and reports `UNMEASURED` rather than `PASS` when handed too little data. Keep
that property if you add checks.

---

## 11. Glossary

| Term | Meaning |
|---|---|
| **GTFS-Realtime** | The standard format agencies publish live transit data in |
| **Protobuf** | Compact binary format; needs a schema to read |
| **511 / MTC** | Our data source; the Bay Area transport agency that runs it |
| **Kafka** | Software carrying streams of events between programs |
| **Topic / partition / key** | A named stream / a lane within it / what decides the lane |
| **Producer / consumer** | Program that writes to / reads from Kafka |
| **Dead-letter topic** | Where records that fail validation go for inspection |
| **Grain** | The fields uniquely identifying one row |
| **Provenance** | Recorded information about *how* a value was derived |
| **Resolver** | Our code that derives arrivals from raw signals |
| **`STOPPED_AT`** | Status meaning a vehicle is presently at a stop |
| **Dwell time** | How long a vehicle sits at a stop |
| **Lead time / horizon** | How far ahead a prediction was made |
| **Bias** | Mean *signed* error — systematic push in one direction |
| **Idempotent** | Doing it twice has the same effect as once |
| **Replay** | Pushing archived history through the system as if live |
| **Acceptance check** | A property verified against real produced data |

---

## Questions worth asking rather than guessing

- Anything touching §5.
- Any use of `stop_id` as an identity.
- Any new model feature — is it knowable at prediction time?
- Adding a second agency before the documents are done.

Everything here is reproducible from the committed data. If a number in this
document doesn't match what you get, that's a finding — raise it.
