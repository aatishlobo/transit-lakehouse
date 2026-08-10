# Session Report — 8 August 2026

**Project:** transit-lakehouse (DE#1) · **Session:** Saturday evening

Friday built the pipes. Today put data through them, and — for the first time —
produced the thing this entire project exists to produce: a statement that a
specific vehicle arrived at a specific stop at a specific time.

As before, this assumes no prior knowledge; terms are defined on first use, with
a glossary at the end.

---

## 1. Where we were

By the end of Friday we had:

- an **archiver** saving live transit data to disk every two minutes,
- **Kafka** running locally in Docker with four configured topics,
- an **event contract** defining what a valid message looks like.

What we did *not* have was anything that moved data from the disk into Kafka, or
anything that read it out the other end. The pipes existed; nothing flowed.

Overnight everything survived: Kafka still running, the archiver at 639 polls
(674 MB), 31 tests passing. Two things worth noting from the logs:

- **25 connection retries recovered.** Friday's bug fix — where a sleeping
  laptop killed the network connection and silently cost a poll — did its job 25
  times.
- **The rate budget hit zero six times.** We poll two feeds every two minutes,
  which is exactly 60 requests per hour against a limit of exactly 60. There is
  no headroom, so any hiccup makes the poller pause and wait.

---

## 2. What today added — the shape of the whole thing

Your course requires this specific path:

```
data source → validated contract → producer → Kafka → consumer → useful output → evidence
```

Today built the **producer** and the **consumer**, which completes it end to end.
Two new programs:

| Program | Job |
|---|---|
| `streaming/producer.py` | Read archived files, validate every record, publish to Kafka |
| `streaming/consumer.py` | Read from Kafka, figure out when vehicles actually arrived |

---

## 3. The producer — replaying history into Kafka

### What it does

The archiver saved thousands of compressed files to disk. The producer reads
them back, decodes them, checks each record against the contract, and publishes
each one as a Kafka message.

This is called **replay**: taking recorded history and pushing it through the
system as though it were happening live. It's how your grader will run the
project — no API key needed, no network, and identical results every time.

### Design choice 1: replay and live share the same code path

There's an obvious shortcut here that we deliberately didn't take.

The tempting design is: have the live archiver publish *straight* to Kafka, and
treat replay as a separate testing tool. Two problems. First, you now maintain
two code paths that will drift apart. Second — and worse — the path your grader
runs would be the one that gets *less* attention, because you spend your time on
the "real" one.

So: **the archiver only ever writes to disk, and everything downstream only ever
reads from disk.** Live and replay differ in exactly one respect — whether new
files are still appearing. Your reviewer therefore runs the genuine pipeline,
not a simulation of it.

### Design choice 2: both feeds are merged into one time-ordered stream

This one is subtle and it matters enormously.

The archive keeps each feed in its own folder — predictions in one, GPS pings in
another. The naive replay reads one folder then the other: publish all 39
minutes of predictions, then all 39 minutes of GPS.

That would break everything downstream. The arrival detector correlates the two
feeds *in time*. Handed every prediction before the first GPS ping, it would
have nothing to match against and would derive precisely zero arrivals.

So the producer merges polls from both feeds into a single stream sorted by when
they were captured, recreating the interleaving that actually happened:

```
14:52:11  trip_updates
14:52:15  vehicle_positions
14:54:11  trip_updates
14:54:16  vehicle_positions
   ...
```

**Replay must reproduce the sequence of observation, not just its contents.**

### Design choice 3: bad records are set aside, not dropped and not fatal

Every record is validated against Friday's contract. Failures have three
possible fates:

- **Crash the program.** One malformed record stops the entire pipeline.
- **Silently skip it.** The pipeline survives, but the evidence is gone and you
  will never know how much you lost.
- **Send it to a dead-letter topic.** The pipeline keeps running *and* the bad
  record is preserved for inspection.

We do the third. A **dead-letter topic** is a separate stream reserved for
records that couldn't be processed, kept with the error message and a timestamp.

### Design choice 4: idempotent publishing

**Idempotence** means doing something twice has the same effect as doing it once.

Networks are unreliable. A producer sends a message, gets no acknowledgement,
and resends — but the first one may have arrived and only the *acknowledgement*
was lost. Now the message exists twice.

Normally you'd catch that in review, because two identical records look
suspicious. Not here: **this feed legitimately repeats itself.** The same trip
reappears in every poll with updated predictions. An accidental duplicate is
indistinguishable from a real repeated observation.

Turning on `enable.idempotence` makes Kafka tag each message so the broker
recognises and discards a resend. Duplicates are prevented at the source rather
than cleaned up later.

### Smaller settings, briefly

- **`acks: all`** — wait for confirmation the message is safely stored before
  considering it sent.
- **`linger.ms: 20`** — wait 20ms to batch messages together. Massively faster
  than sending each individually; replay doesn't care about 20ms.
- **`compression: lz4`** — compress batches. These payloads are text-heavy and
  compress well.
- **Bigger send buffer** — one prediction poll explodes into ~80,000 records.
  The default buffer stalls constantly.
- **`--speed`** — replay faster or slower than real time. `--speed 0` fires
  everything immediately (what a grader wants); `--speed 1` reproduces the
  original two-minute spacing.

### Result

```
polls:              39
rows:               1,668,118
published:          1,668,118
dead-lettered:      0
delivery failures:  0
elapsed:            77.1 seconds   (21,646 rows/second)
```

Every one of 1.67 million records passed validation. The 39-minute sample
expands to 1.6 million prediction records and 38,701 GPS records — the ~40×
difference is because one trip carries dozens of upcoming stops, while a vehicle
has only one position.

---

## 4. The consumer — deriving actual arrivals

This is the heart of the project.

### The problem, restated

No transit agency publishes "the bus arrived at 3:09." We have to work it out.

### The method used: watching status change

Each GPS record carries a field called `current_status`, which one of several
values — including `STOPPED_AT`, meaning *this vehicle is presently at a stop* —
alongside which stop it refers to.

When Muni's onboard system says `STOPPED_AT stop sequence 25`, that's the
agency's own equipment asserting the vehicle is physically there. That's the
best available signal, because we're reading their determination rather than
guessing from coordinates.

From Wednesday's profiling: this is available for 15 of 28 operators. Muni fills
it in 99.2% of the time.

### The critical detail: the arrival is the FIRST sighting

A vehicle sitting at a stop reports `STOPPED_AT` on *every* poll while it waits:

```
poll 1:  approaching stop 25
poll 2:  STOPPED_AT stop 25     ← the arrival
poll 3:  STOPPED_AT stop 25     ← still there
poll 4:  approaching stop 26
```

Taking the *last* `STOPPED_AT` would measure the **departure**. Taking a middle
one measures nothing meaningful. Only the *first* is the arrival.

Which means the program must remember what it saw before. It keeps a small
record per trip: last status, last stop, and which stops have already been
reported. Today that suppressed 3,759 repeat sightings.

### Why Friday's key design was load-bearing

This is where Friday's message-key decision earns its keep.

"First sighting" only means something if observations arrive **in order**. Kafka
guarantees ordering within one partition (lane), not across them. Friday we
keyed every message on `service_date:trip_id`, which forces all observations of
one trip into one lane, in sequence.

Had we skipped that, poll 3 could be processed before poll 2, and the program
would record the arrival at the wrong moment — or miss it entirely. Friday's
verification that **71 distinct keys occupied zero split lanes** was checking
precisely this precondition.

### Which timestamp counts

Each arrival is stamped with `vehicle_report_ts` — the time the *vehicle*
recorded its position — never the time we downloaded the data. If the feed
served a stale position, using our download time would invent a brand-new
arrival at a moment nothing happened.

### Provenance on every row

Each arrival event records not just *what* but *how*:

```json
{
  "service_date":    "20260806",
  "trip_id":         "SF:12053041_M11",
  "stop_sequence":   47,
  "stop_id":         "13550",
  "route_id":        "SF:1",
  "agency":          "SF",
  "vehicle_id":      "5885",
  "actual_arrival_ts": "2026-08-06T21:52:43+00:00",

  "arrival_method":     "stopped_at",
  "arrival_confidence": "high",
  "poll_interval_s":    120,
  "resolver_version":   "1.0.0"
}
```

Those bottom four fields are the design decision. There are three possible ways
to derive an arrival, of differing quality, and **which ones are available
depends on the operator**. If we recorded only the final timestamp, then *which
agency a bus belongs to* would secretly determine *how accurate its arrival time
is* — and any later analysis would discover "agency differences" that were
really artifacts of our own code.

Recording the method makes that visible and filterable. `poll_interval_s` is
there because an arrival timestamp can never be more precise than the interval
that observed it — it's the built-in error bar.

### Every skip is counted

Records that can't produce an arrival aren't silently discarded; each reason is
tallied:

| Reason | Count |
|---|---|
| No trip identity | 7,969 |
| No `current_status` (operator doesn't publish it) | 4,087 |
| No stop sequence | 634 |
| No vehicle timestamp | 0 |
| Repeat sighting, already recorded | 3,759 |

That second row *is* the measurement of how many operators support this method.
Discarding those quietly would destroy the evidence.

### Manual bookmarking

Kafka readers track their position with an **offset** — a bookmark. By default
it saves automatically on a timer, which can mark a record "done" *before* its
arrival was actually written, losing it in a crash. We commit only after
processing. That yields at-least-once delivery, which the design absorbs because
re-processing the same input produces the same output — proven below.

### Result

```
consumed:            38,701 vehicle positions
arrivals derived:     6,046
trips tracked:        2,199
elapsed:                16 seconds
```

Across 17 operators:

| Agency | Arrivals | | Agency | Arrivals |
|---|---|---|---|---|
| SF (Muni) | 3,088 | | 3D | 106 |
| SC (VTA) | 1,486 | | ST | 41 |
| AC Transit | 507 | | VC | 35 |
| CC | 277 | | FS | 29 |
| MA | 202 | | others | 60 |
| WH | 115 | | | |
| GG | 108 | | | |

---

## 5. Validation

Two acceptance checks, both of which your course asks for as evidence.

**Grain uniqueness.** Every arrival should be uniquely identified by
(service date, trip, stop sequence). Result: **6,046 arrivals, 6,046 distinct
keys** — no collisions.

**Idempotence.** Re-processing identical input must produce identical output —
otherwise replays and crash-recovery quietly corrupt the data. The resolver was
run twice from scratch against the same input and the outputs compared record by
record:

```
run 1 arrivals = 6,046
run 2 arrivals = 6,046
IDENTICAL      = True
```

Bit-for-bit identical. This is demonstrable, not asserted.

---

## 6. Three honest limitations

**We only catch about 21% of stop events.** SF Muni averages 4.0 arrivals per
trip over 39 minutes — about 19 polls. Buses pause at stops for seconds; we
sample every 120 seconds. Most arrivals happen invisibly between polls.

This is not a bug, it's the sampling floor, and it's the strongest possible
argument for the rate-limit increase: faster polling directly means more
arrivals captured *and* more precise timestamps. It belongs in your report
stated plainly.

**3.0% of arrivals have no service date** (182 of 6,046), concentrated in three
small operators — 3D (106), ST (41), VC (35). Muni is clean at 0 of 3,088. Since
service date is part of the identifying key, those records are weakly
identified.

**255 arrivals have no stop ID.** The sequence number is present so the record
is still valid, but the human-readable stop is missing.

---

## 7. "How is this working without a Confluent cluster?"

Worth recording, because the naming genuinely confuses people.

**Apache Kafka is free, open-source software** maintained by the Apache Software
Foundation. That's what's running on the laptop — the container is the official
`apache/kafka:3.9.0` image. It is a real Kafka broker, not an imitation.

**Confluent** is a company (founded by Kafka's original authors) that sells a
hosted version and add-on tooling. Useful; not required.

The Python library we use is called `confluent-kafka` — written by Confluent,
open source, and it is only a **client**. The Kafka wire protocol is a public
standard, so any client talks to any broker, the same way any browser talks to
any web server.

**What running locally costs us:** no Schema Registry (Confluent's service that
centrally enforces schema compatibility — we substituted Pydantic contracts plus
a version stamp on each message, which validates but doesn't centrally enforce);
no replication or failover; no multi-machine consumer scaling.

**Why it's still the right call:** your course explicitly permits a local
Kafka setup, and your proposal chose the non-cloud review path. A `docker
compose up` that works on any machine is a *stronger* review path than a cloud
cluster you must keep alive and share access to. And the code is portable —
pointing at a real cluster means changing a server address and adding
credentials; the producer, consumer, contracts and resolver are untouched.

---

## 8. Design decisions, collected

| Decision | Alternative rejected | Why |
|---|---|---|
| Poller writes only to disk; everything reads from disk | Poller publishes to Kafka directly | One code path instead of two that drift; grader runs the real one |
| Merge both feeds in time order | Replay one feed then the other | The resolver correlates feeds in time; sequential replay derives nothing |
| Dead-letter bad records | Crash, or silently skip | Pipeline survives *and* evidence is preserved |
| Idempotent producer | Default at-least-once | This feed legitimately repeats; accidental duplicates would be invisible |
| Arrival = first `STOPPED_AT` | Last sighting | Last sighting measures departure, not arrival |
| Stamp method/confidence on every row | Record only the timestamp | Otherwise agency silently correlates with data quality |
| Count every skip reason | Filter silently | The counts *are* the coverage measurement |
| Manual offset commits | Auto-commit | Auto-commit can acknowledge before the work is done |
| Event time from the vehicle | Our download time | Stale feeds would fabricate arrivals |

---

## 9. Where things stand

**Complete:** the full required path — source → contract → producer → Kafka →
consumer → output — runs end to end from committed sample data with one command
and no API key. Two acceptance checks pass. 31 tests still green.

**Gap I want to flag:** today's two new programs have no unit tests of their own.
The pipeline is verified *end to end* (idempotence, grain uniqueness, 1.67M
records validated), but the producer and resolver lack the focused tests the
older code has. That's first on tomorrow's list, and it's a course requirement —
"repeatable tests, metrics, acceptance checks."

**Tomorrow (Sunday):** turn 6,046 arrival events into the actual deliverable —
prediction-accuracy and delay metrics by route and hour, written to `outputs/` —
plus tests for the producer and resolver, and the acceptance checks formalised
in `evaluation/`.

---

## Glossary (additions)

| Term | Meaning |
|---|---|
| **Replay** | Pushing recorded history through the system as if it were live |
| **Producer / Consumer** | A program that writes to / reads from Kafka |
| **Dead-letter topic** | A separate stream for records that failed processing |
| **Idempotence** | Doing something twice has the same effect as doing it once |
| **Offset** | A consumer's bookmark — how far it has read |
| **Commit (an offset)** | Saving that bookmark so a restart resumes correctly |
| **At-least-once** | Delivery that may repeat but never loses; safe when processing is idempotent |
| **`acks`** | How many confirmations a producer waits for before considering a message sent |
| **Grain** | The set of fields uniquely identifying one row (here: service date + trip + stop sequence) |
| **Provenance** | Recorded information about *how* a value was derived |
| **`STOPPED_AT`** | GTFS-Realtime status meaning a vehicle is presently at a stop |
| **Schema Registry** | A Confluent service enforcing schema compatibility centrally — not used here |
| **Apache Kafka vs Confluent** | The free open-source software vs a company selling hosted Kafka and tooling |
