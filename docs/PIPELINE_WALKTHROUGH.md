# How this project actually works

**A stage-by-stage walkthrough, following one real vehicle from the 511 feed to
a finished number.**

If the reports told you *why* decisions were made and the onboarding guide told
you *what exists*, this document tells you **what actually happens to the data**.

We will follow a single real record the entire way. Every number below is taken
from the committed sample data — nothing is invented.

**Our vehicle:** cable car number 1, running the Powell–Mason line
(`route SF:PM`), on trip `SF:12086593_M11`, on 6 August 2026.

---

## 0. What we are computing, and why it takes nine stages

The question is simple: **when Muni says a vehicle arrives in N minutes, how
wrong is it?**

To answer it you need two things side by side:

| | Where it comes from |
|---|---|
| what the agency **predicted** | published directly in the feed |
| what **actually happened** | **not published by anyone** |

The second one is the problem. No transit agency publishes "the cable car
arrived at 14:54:31." We have to work it out from GPS breadcrumbs — and that
derivation is what most of this pipeline exists to do.

So the pipeline splits into three jobs:

1. **Capture and keep** the raw feed (stages 1–3), because it is unrepeatable.
2. **Derive the actual arrival** from GPS state changes (stages 4–6).
3. **Compare** it against what was predicted, and learn from the gap (stages 7–9).

---

## The pipeline at a glance

```
   [1] 511 feed          the raw source, protobuf over HTTP
        |
   [2] poller            fetch every 120s, write bytes to disk FIRST
        |
   [3] data/raw/         the archive — the permanent record
        |
   [4] producer          decode, validate, publish
        |
   [5] Kafka             4 topics, keyed by trip
        |
        +----[6] arrival resolver     GPS states -> "it arrived at 14:54:31"
        |            |
        +----[7] aggregator           join arrivals against predictions
                     |
                [8] outputs/          the metrics
                     |
                [9] ML model          learn the correction
```

---

## Stage 1 — The source: what 511 actually gives us

511.org publishes two live feeds covering all 28 Bay Area operators. We poll
both.

**Feed A — `TripUpdates`. The agency's predictions.**

> "On trip `SF:12086593_M11`, the vehicle will reach stop sequence 4 at 14:53:54."

**Feed B — `VehiclePositions`. Where vehicles are right now.**

> "Cable car 1 is at latitude 37.80, longitude −122.41, and its status is
> `IN_TRANSIT_TO` stop sequence 4."

Both arrive as **protobuf** — a compressed binary format. Roughly 540 KB per
poll of trip updates, which unpacks into about 80,000 individual rows.

Two things about this source shape everything downstream:

**It is a snapshot, not a change log.** Every poll restates *everything*
currently happening. The same trip reappears every two minutes with updated
predictions. Nothing tells you what changed.

**There is no history.** You can ask what is happening now. You cannot ask what
happened yesterday. If you were not polling at 14:54 on 6 August, that moment is
gone permanently.

That second fact is the single most important constraint in the project.

---

## Stage 2 — The poller: fetching, and the one rule

`ingest/poller/poller.py` runs continuously. Every 120 seconds it:

1. requests both feeds from 511,
2. **writes the raw bytes straight to disk**,
3. *then* tries to decode them.

### Why that ordering matters

The intuitive design is fetch → decode → save the result. We do the opposite,
deliberately.

Think about which mistakes you can undo:

- **A decoding bug?** Recoverable. The original bytes are on disk; fix the code
  and reprocess.
- **A poll you never took?** Gone forever. There is no history endpoint.

So the rule is: **make the irreversible step the cheap one.** Save first, think
later. If decoding fails, the payload goes to a quarantine folder rather than
being discarded.

### The other job: not getting cut off

Our API key allows **60 requests per hour**. Two feeds every 120 seconds is
exactly 60 — no headroom at all.

The poller counts its own requests rather than waiting to be rejected, because
being throttled would stop the archive, and archive gaps cannot be backfilled.

**This quota is also why our data has limits.** Polling every 120 seconds means
we only ever see a vehicle's position every two minutes. That interval is the
precision limit on every arrival time we derive, so it gets recorded on every
single row as `poll_interval_s` — an error bar attached to the data itself.

---

## Stage 3 — The archive: the thing everything else is built on

```
data/raw/vehicle_positions/ingest_dt=2026-08-06/1786053271-a3f9c1d2.pb.gz
data/raw/trip_updates/ingest_dt=2026-08-06/1786053167-e2ab8dac.pb.gz
```

Each file is one poll of one feed: the exact bytes 511 sent, gzipped. The name
carries the poll timestamp and a hash of the contents.

**This archive is the system of record.** Not Kafka, not the database. If
everything else were deleted, we could rebuild it from these files. If *these*
were deleted, no amount of code could recover them.

The folder is named `ingest_dt` — the date **we downloaded it** — and that
precision is deliberate. It originally said `service_dt` (the transit service
day), which was wrong in an interesting way: a single downloaded file contains
trips from several different service days. One real poll held trips dated 5
August, trips dated 6 August that had already run past midnight, and 2,858 rows
with no date at all.

**Service date belongs to each row, not to the file.** So the file is labelled
with the only date that genuinely describes it: when we fetched it.

---

## Stage 4 — The producer: from disk into the pipeline

Now the archived bytes get pushed through the system. `streaming/producer.py`
reads the files back, decodes them, checks each record, and publishes.

### 4a. Decoding — where the project's founding bug lives

Protobuf lets a field be **genuinely absent**, which is different from being
zero:

| Field state | Meaning |
|---|---|
| `delay = 0` | the vehicle is **exactly on time** |
| `delay` absent | the agency **told us nothing** |

The trap: reading the field the natural way returns `0` for **both**. You have
to explicitly ask "was this field actually set?"

We measured the consequence: **44.2% of records carry no delay value at all.**
Written the obvious way, this project would have labelled 54,638 records per
sample as *perfectly on time* — a fabricated spike of good performance that
looks entirely plausible and poisons every number computed afterwards.

Everything routes through one guarded reader in `decode.py`. This is why the
project exists in the shape it does.

### 4b. Validation — the event contract

Each decoded record is checked against a schema (`streaming/contracts.py`)
before it is allowed onto the pipeline. Anything failing goes to a
**dead-letter topic** — a side-channel for bad records — so one malformed row
cannot crash the run or vanish silently.

Here is our cable car's prediction record, as it enters Kafka:

```json
{
  "envelope": {
    "feed_header_ts":   "2026-08-06T21:52:20+00:00",
    "ingest_ts":        "2026-08-06T21:52:24+00:00",
    "poll_interval_s":  120
  },
  "trip": {
    "trip_id":      "SF:12086593_M11",
    "route_id":     "SF:PM",
    "service_date": "20260806"
  },
  "stop_sequence":       4,
  "stop_id":             "16072",
  "arrival_time_epoch":  1786053234,      // predicted 14:53:54
  "arrival_delay_s":     -553,
  "arrival_uncertainty": null             // absent, NOT zero
}
```

Note the two timestamps. `feed_header_ts` is when the *agency* produced this;
`ingest_ts` is when *we* received it. They are kept strictly separate — using
our clock as if it were the agency's would invent observations that never
happened.

### 4c. Replaying in the right order

The archive stores each feed in its own folder. Replaying one folder then the
other would deliver 39 minutes of predictions before the first GPS ping — and
the resolver, which correlates the two in time, would find nothing at all.

So the producer merges both feeds into a **single stream sorted by capture
time**, reproducing the interleaving that actually happened:

```
14:52:20  trip_updates       (predictions)
14:52:43  vehicle_positions  (cable car 1 approaching stop 4)
14:54:25  trip_updates
14:54:31  vehicle_positions  (cable car 1 STOPPED at stop 4)
```

---

## Stage 5 — Kafka: the conveyor belt

Kafka carries records between programs. Four topics:

| Topic | Carries |
|---|---|
| `gtfsrt.trip_updates.v1` | predictions |
| `gtfsrt.vehicle_positions.v1` | GPS observations |
| `gtfsrt.dead_letter.v1` | records that failed validation |
| `transit.arrival_events.v1` | arrivals we derive |

Each topic is split into 3 **partitions** — parallel lanes, so multiple readers
can work at once.

### The key, and why it decides everything

Every message carries a key:

```
key = "20260806:SF:12086593_M11"      (service date : trip)
```

Kafka guarantees order **within one lane**, not across lanes. The key decides
the lane. So keying on the trip guarantees every observation of our cable car
lands in the same lane, in the order it happened.

That is not tidiness — it is a correctness requirement, and the next stage is
why.

*(The date is in the key because trip IDs repeat every day. Yesterday's
`SF:12086593_M11` is a different journey.)*

**Kafka is not our storage.** Topics discard data after 24 hours on purpose.
Kafka moves data; the archive keeps it.

---

## Stage 6 — The arrival resolver: the heart of the project

`streaming/consumer.py` reads GPS observations and produces the fact nobody
publishes.

### How we know a vehicle arrived

Each GPS record includes `current_status`, which is one of `IN_TRANSIT_TO`,
`INCOMING_AT`, or `STOPPED_AT`, plus which stop it refers to. When Muni's
onboard system reports `STOPPED_AT stop sequence 4`, the agency's own equipment
is asserting the vehicle is physically there.

**Our cable car, in the real data:**

```
14:52:43   IN_TRANSIT_TO   stop_sequence 4     approaching
14:54:31   STOPPED_AT      stop_sequence 4     <-- IT ARRIVED
14:56:50   STOPPED_AT      stop_sequence 6
14:59:55   STOPPED_AT      stop_sequence 10
```

The resolver watches that transition and emits:

> **Trip `SF:12086593_M11` arrived at stop sequence 4 at 14:54:31.**

That sentence exists nowhere in the source data. We derived it.

### The subtle part: first sighting, not last

A vehicle sitting at a stop reports `STOPPED_AT` on *every* poll while it waits.
If you took the **last** such report, you would be measuring **departure**. If
you took a middle one, you would be measuring nothing in particular.

Only the **first** is the arrival.

Which means the resolver has to remember what it saw before — it keeps a small
running state per trip. And *that* is why the message key matters: if those four
observations arrived out of order, "first" would be meaningless.

### What gets recorded

```json
{
  "service_date":       "20260806",
  "trip_id":            "SF:12086593_M11",
  "stop_sequence":      4,
  "stop_id":            "16072",
  "route_id":           "SF:PM",
  "actual_arrival_ts":  "2026-08-06T21:54:31+00:00",
  "arrival_method":     "stopped_at",
  "arrival_confidence": "high",
  "poll_interval_s":    120
}
```

Those last three fields are **provenance** — not just the answer, but how we got
it and how precise it can possibly be. Only 15 of 28 operators publish
`current_status`, so without recording the method, *which agency a vehicle
belongs to* would silently determine *how good its data is*, and any later
analysis would find "agency differences" that were really artifacts of our own
code.

---

## Stage 7 — The aggregator: putting the two halves together

`streaming/aggregator.py` reads both the arrivals we derived and the predictions
from the feed, and matches them on `(service_date, trip_id, stop_sequence)`.

**For our cable car at stop 4, actual arrival 14:54:31:**

| prediction issued | predicted arrival | lead time | error |
|---|---|---|---|
| 14:52:20 | 14:53:54 | 131 s | **−37 s** |
| 14:54:25 | 14:55:05 | 6 s | **+34 s** |

Read that first row: two minutes before the cable car actually arrived, Muni
predicted 14:53:54. It turned up at 14:54:31 — **37 seconds later than
promised.**

That single pair is one data point. The pipeline produces **25,501 of them**.

### Two rules that make the number honest

**Only predictions issued *before* the arrival count.** A "prediction" published
after the vehicle already arrived is a correction, not a forecast. Scoring it
would count hindsight as foresight. 40 such pairs were excluded.

**The two halves must come from different feeds.** Our actual comes from GPS;
the prediction comes from TripUpdates. There is a tempting shortcut — treat the
last prediction before a vehicle passes *as* the actual arrival — and it would
have quietly destroyed the project, because we would then be comparing the
agency's predictions against the agency's predictions and reporting a
wonderfully small error that means nothing at all.

---

## Stage 8 — Outputs: the finished answer

Grouped by how far ahead each prediction was made:

```
   lead time       n     bias   median |err|   within 60s
     0-2 min     509      -11        13 s        92.7%
     2-5 min    4603      -46        50 s        59.5%
     5-10 min   4897      -66        70 s        43.3%
    10-20 min   8919      -98       102 s        31.1%
      20+ min   6573     -142       146 s        23.0%
```

**How to read this:**

- Close to arrival, Muni is excellent — 13 seconds off, typically.
- The further ahead, the worse: at 20+ minutes, only 23% land within a minute.
- **`bias` is negative everywhere.** Negative means predicted *earlier* than
  reality — vehicles arrive **later** than promised, exactly like our cable car's
  37 seconds.

Two numbers are reported because they answer different questions. Median error
says *how far off, typically*. Bias says *which direction, systematically* — and
bias is the one that hurts riders, because it never averages out.

**One honest caveat.** We only ever see a vehicle every 120 seconds, and we can
only spot it as `STOPPED_AT` while it is still standing there. That means we
catch roughly 21% of stops, we tend to notice them slightly *after* they happen,
and we disproportionately catch long stops like terminals. All of that nudges
the measured lateness upward — so these figures are an **upper bound** on how
optimistic Muni is, not a precise measurement.

---

## Stage 9 — The model: learning the correction

The final stage asks: since the errors are *systematic*, can we predict them?

The model predicts the **residual** — how wrong the agency's ETA will turn out
to be — which is then added back:

```
corrected ETA  =  agency ETA  +  predicted correction
```

Predicting zero is identical to trusting the agency, so the comparison is exact:

| approach | average error |
|---|---|
| trust the agency | 195.2 s |
| always add the average lateness | 215.1 s |
| add the average for that lead time | 206.7 s |
| **the model** | **166.6 s** — 14.7% better |

Notice rows 2 and 3: **the naive fixes make things worse.** Most vehicles are a
little late but a few are enormously late, so adding the *average* overcorrects
the typical case. The model earns its place by learning corrections that depend
on the lead time, the hour, and the route.

Its features are strictly things knowable when the prediction was issued. And if
it ever fails to beat the plain baseline, the code serves the agency's original
ETA unchanged — a model that quietly makes predictions worse is worse than no
model.

---

## The whole journey, in one table

Following our cable car:

| Stage | What exists at this point |
|---|---|
| 1. 511 feed | binary protobuf, ~540 KB |
| 2. poller | those bytes, saved to disk unread |
| 3. archive | `1786053271-a3f9c1d2.pb.gz` |
| 4. producer | decoded, validated JSON records |
| 5. Kafka | keyed `20260806:SF:12086593_M11` |
| 6. resolver | **"arrived at stop 4 at 14:54:31"** |
| 7. aggregator | "predicted 14:53:54, so 37 s late" |
| 8. outputs | one of 25,501 rows in the summary |
| 9. model | "on this route, at this hour, add ~90 s" |

---

## What `make demo` actually runs

```
make demo
 ├─ docker compose up          start Kafka, create 4 topics
 ├─ streaming.producer         archive -> Kafka   (1.67M records, ~45 s)
 ├─ streaming.consumer         GPS -> arrivals    (6,046 derived)
 ├─ streaming.aggregator       join -> outputs/   (25,501 pairs)
 └─ evaluation/                6 acceptance checks
```

About two minutes, no API key. Stages 1–3 already happened when we collected the
data; the demo replays from the archive.

---

## Where each stage lives

| Stage | File |
|---|---|
| 1–2 poller | `ingest/poller/poller.py` |
| decoding | `ingest/poller/decode.py` ← most important file |
| 3 archive | `data/raw/`, sample in `data/replay_sample/` |
| 4 producer | `streaming/producer.py` |
| contract | `streaming/contracts.py` |
| 5 Kafka | `docker-compose.yml` |
| 6 resolver | `streaming/consumer.py` |
| 7 aggregator | `streaming/aggregator.py` |
| 8 outputs | `outputs/` |
| 9 model | `ml/` |
| checks | `evaluation/run_acceptance_checks.py` |

---

## If you remember four things

1. **No agency publishes arrival times.** Everything here exists to derive them
   from GPS state changes.
2. **An absent field is not a zero.** 44% of records have no delay value;
   treating those as "on time" would have invented a mountain of good
   performance that never existed.
3. **Order matters.** An arrival is the *first* moment a vehicle reports being
   stopped — which only means something if observations arrive in sequence.
   That is why messages are keyed by trip.
4. **The answer is only credible because the halves are independent.** Predicted
   comes from one feed, actual from another. Derive one from the other and the
   result becomes a very impressive way of comparing the agency to itself.
