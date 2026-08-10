# Session Report — 7 August 2026

**Project:** transit-lakehouse (DE#1) · **Session:** Friday afternoon/evening

Yesterday's work got transit data *onto disk*. Today's work built the machinery
to move that data *through a pipeline*. This report explains every piece,
assuming no prior knowledge. Terms are defined on first use, with a glossary at
the end.

---

## 1. Where we were, and what today needed to add

At the end of the last session we had a working **archiver**: a program that
asks 511.org for live transit data every two minutes and saves it to the hard
drive. That's genuinely useful — it's a growing pile of history that can never
be re-fetched — but it is only half a system. The data just sits there.

Your course requires a specific shape of project:

```
data source → validated contract → producer → Kafka → consumer → useful output → evidence
```

Today built the first four boxes. Tomorrow builds the consumer.

---

## 2. What Kafka is, in plain terms

**Kafka is software that moves streams of events between programs.**

The easiest way to understand it is by what it replaces. Suppose program A
produces data and program B needs it. The obvious approach is for A to call B
directly. This breaks in predictable ways: if B is down, the data is lost; if B
is slow, A is stuck waiting; and if a program C later also wants the data, A
has to be modified.

Kafka sits between them. A writes into Kafka and moves on. B reads at its own
pace. C can start reading later without A knowing C exists.

The mental model that matters: **Kafka is a notebook that only gets appended
to.** Producers write new lines at the end. Readers each keep their own
bookmark. Nobody erases lines, and one reader falling behind doesn't affect
anyone else.

### The vocabulary

| Term | Meaning |
|---|---|
| **Broker** | One running Kafka server. We run one; production runs several. |
| **Topic** | A named stream, like a table name. We have four. |
| **Partition** | A topic is split into numbered lanes so several readers can work in parallel. |
| **Key** | A label on each message that decides which lane it goes into. |
| **Producer** | A program that writes messages. |
| **Consumer** | A program that reads messages. |
| **Offset** | A reader's bookmark — how far through a partition it has read. |

### Why *this* project needs Kafka

Being honest about this matters, because the answer isn't "for safekeeping."

1. **Your course requires it.** A Kafka path is a graded requirement.
2. **Fan-out.** The archiver writes once; multiple different programs can read
   the same stream independently.
3. **Ordering — the real technical reason.** This one deserves explanation
   because tomorrow's work depends entirely on it.

We need to figure out when a bus *actually arrived* at a stop. The way we do
that is by watching a vehicle change state over consecutive observations:

```
poll 1:  bus 4821 — approaching stop 14
poll 2:  bus 4821 — STOPPED_AT stop 14      ← it arrived, here
poll 3:  bus 4821 — approaching stop 15
```

That only works if we see those three observations **in order**. Kafka
guarantees ordering *within a single partition* but not across partitions. So
if bus 4821's observations were scattered across three lanes and read
simultaneously, we might see poll 3 before poll 2 and conclude it never
stopped. Keys solve this: all messages with the same key always land in the
same lane.

### What Kafka is *not*, here

**Kafka is not our archive.** Messages here expire after 24 hours by design.
The permanent record is the files on disk. This is deliberate and is worth
being able to say out loud: it's a common and expensive mistake to treat a
Kafka topic as long-term storage, discover the retention window quietly deleted
last month's data, and have no way to get it back.

---

## 3. What Docker is, and why we used it

Installing Kafka normally means installing Java, downloading Kafka, editing
configuration files, and dealing with everything specific to your machine. If it
works on your laptop, there is no guarantee it works on your grader's.

**Docker packages software together with everything it needs to run** — its own
miniature operating system, libraries, configuration, all of it — into a
**container**. A container behaves identically wherever it runs.

Two terms:

- An **image** is the packaged template (like a recipe). We use `apache/kafka:3.9.0`
  — the official image, version pinned so it can't change under us.
- A **container** is a running copy of an image.

**`docker-compose.yml`** is a file describing which containers to run and how
they connect. One command — `docker compose up` — starts everything.

For your submission this matters directly: the grader needs to run your project
without a day of setup. "Install Docker, run one command" is a review path that
actually works.

---

## 4. Walking through `docker-compose.yml`

The file defines two services.

### Service 1: `kafka` — the broker

```yaml
image: apache/kafka:3.9.0
ports:
  - "9092:9092"
```

Runs Kafka and makes port 9092 reachable from your Mac. A **port** is a numbered
door on a machine; 9092 is Kafka's conventional one.

**KRaft mode.** Older Kafka needed a second piece of software called ZooKeeper
just to track cluster state — two containers, twice the configuration, twice the
confusion. Kafka 3.3+ manages this itself using a built-in consensus mechanism
called KRaft. One container instead of two.

**Replication factor 1.** In production, every message is copied to 3+ machines
so a hardware failure loses nothing. We keep one copy. That would be reckless
if Kafka were our system of record — but it isn't. The disk archive is. Losing
the Kafka log costs us a replay, not data.

### The listener configuration — the trap worth understanding

This caused the one real failure today and is the most common Kafka-in-Docker
problem, so it's worth explaining properly.

**The surprise:** when a program connects to Kafka, it does *not* keep using
the address it dialed. It connects once, asks "where are your brokers?", and
Kafka replies with its **advertised address** — which the client then
reconnects to. So the address Kafka *announces* matters more than the address
you connected to.

The problem is that two different audiences need two different addresses:

- Another **container** reaches Kafka by the name `kafka` (Docker's internal
  networking).
- A program on **your Mac** reaches it at `localhost`.

Advertise only `localhost`, and containers dial *themselves* and hang forever.
Advertise only `kafka`, and programs on your Mac can't resolve the name.

Today's first attempt hit exactly this: the setup container connected fine, was
told "I'm at localhost:9092", tried its own localhost, found nothing, and
retried in a loop.

**The fix — two named listeners:**

```yaml
KAFKA_LISTENERS:            INTERNAL://:29092,EXTERNAL://:9092,CONTROLLER://:9093
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
```

Kafka now listens on two doors and tells each audience the right one.

### Other settings, and why

**`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`** — by default, asking for a topic
that doesn't exist silently *creates* it. That sounds convenient and is a
debugging nightmare: misspell a topic name and your consumer sits reading a
brand-new empty topic forever, with no error. Turning it off means a typo fails
immediately.

**`KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0`** — Kafka normally waits 3 seconds
before letting a new reader start, in case more readers are about to join. With
one reader that's just 3 seconds of a demo looking broken.

**A healthcheck.** Docker needs to know when Kafka is genuinely *ready*, not
merely started. Ours asks Kafka to list its topics — a request only a working
broker can answer. Checking whether the port is open would report success too
early, and the next step would fail confusingly.

### Service 2: `kafka-init` — creating the topics

This container waits for Kafka to be healthy, creates four topics with explicit
settings, prints them, and exits. It's not a server; it's a setup step that runs
and finishes.

---

## 5. The four topics, and every setting explained

| Topic | Partitions | Retention | Purpose |
|---|---|---|---|
| `gtfsrt.trip_updates.v1` | 3 | 24 hours | Predictions from agencies |
| `gtfsrt.vehicle_positions.v1` | 3 | 24 hours | GPS pings |
| `gtfsrt.dead_letter.v1` | 1 | 7 days | Records that failed validation |
| `transit.arrival_events.v1` | 3 | 7 days | Our derived arrivals (built tomorrow) |

### Why 3 partitions, chosen rather than defaulted

**Partition count is effectively permanent.** Which lane a message goes to is
computed from its key — roughly `hash(key) % number_of_lanes`. Change the number
of lanes and that arithmetic changes, so a key that used to go to lane 1 now
goes to lane 2. Its history is split across two lanes, and the ordering
guarantee we depend on breaks *retroactively*, for data already written.

So it has to be decided up front. Three is enough to demonstrate real
parallelism while staying easy to reason about on one broker.

### Why `cleanup.policy=delete`, never `compact`

Kafka offers two ways to reclaim space:

- **delete** — throw away messages older than the retention window.
- **compact** — keep only the *most recent* message per key, discard older ones.

Compaction sounds appealing ("just keep the current state of each trip") and
would be catastrophic here. Our data *is* the history: the whole point is
watching a bus move from "approaching" to "stopped" to "departed" over time.
Compaction would keep only the final observation and throw away the transitions
— destroying precisely what the arrival detector reads.

### Why retention is only 24 hours

Deliberately short, to prevent a bad habit. If the topic held 30 days of data,
it would be tempting to treat it as the database. It isn't — the disk archive
is, and replays run from disk. Short retention keeps that boundary honest.

The dead-letter and output topics get 7 days instead: failed records need to
survive long enough for a human to inspect them, and outputs need to outlive the
run that produced them.

### Why the dead-letter topic has 1 partition

Ordering between unrelated broken records is meaningless, and the volume should
be near zero. One lane is correct, not lazy.

---

## 6. The event contract (`streaming/contracts.py`)

### What a "contract" is

A contract is a precise definition of what a message must look like: which
fields exist, what types they are, which may be missing. Every message is
checked against it before being published.

We use **Pydantic**, a Python library that turns such a definition into
automatic validation.

### Why validate at all when we wrote both sides?

A fair question — the same person wrote the producer and the consumer. Three
reasons:

1. **It's a boundary of ownership.** Once a message is on a topic, *any* program
   may read it. The contract is the only statement of what readers may assume.
2. **It catches drift.** If the decoder starts emitting a new field and the
   contract doesn't know about it, that mismatch fails loudly here rather than
   reaching a consumer that quietly ignores it.
3. **It makes bad records routable.** A record that fails validation goes to the
   dead-letter topic and the stream keeps running — instead of crashing the
   pipeline, or being silently skipped.

### The critical detail: Pydantic **version 2** is mandatory

This connects directly to the most important finding from yesterday.

Recall: 44.2% of transit records contain **no delay information at all**, and
that must never be confused with "delay of 0 seconds," which means exactly on
time. Yesterday's decoder handles this correctly.

The danger is that validation libraries exist largely to *fill in missing
values with defaults* — which is exactly the wrong behavior here. Pydantic
version 1 would convert a missing value into a field's default. If that default
were 0, every one of those 54,000+ records per poll would be silently relabeled
"on time," undoing the entire safeguard **at the one place specifically built to
protect it.**

Pydantic v2 preserves the distinction. The requirements file pins v2 with an
explanatory comment, and three tests verify the behavior — including one that
checks the distinction survives conversion to JSON and back through Kafka.

### Three configuration choices

**`strict=True`** — no automatic type conversion. In relaxed mode, the text
`"0"` would be quietly turned into the number `0`. But a value arriving as the
wrong *type* means something upstream changed, and we want to hear about it
rather than have it repaired behind our backs.

**`extra="forbid"`** — an unexpected field is an error, not something to ignore.
This is the drift detector.

**`frozen=True`** — events can't be modified after validation, so what a program
holds always matches what was actually published.

### The message key

```python
def partition_key(service_date, trip_id) -> bytes:
    return f"{service_date}:{trip_id}".encode()
```

Small function, two real decisions.

**Why include the date?** A `trip_id` is only unique *within one day* — the same
ID recurs every morning. Keying on `trip_id` alone would force Tuesday's and
Wednesday's runs of the same trip into the same lane: needless coupling, and
uneven load.

**Why `"nodate:notrip"` instead of no key?** Some records genuinely have no trip
identity — one operator sends no service date at all. A message with *no* key
gets distributed round-robin, scattering those records unpredictably. A fixed
placeholder keeps them together and inspectable.

### Validation result on real data

The contract was tested against a real archived payload: **82,061 rows, all
82,061 valid, zero failures.** Missing values stayed missing; zeros stayed zero.

---

## 7. The replay sample (`scripts/make_replay_sample.py`)

Your submission must include sample data so the grader can run everything
without an API key. This script cuts that sample from the live archive.

**Result:** a 39-minute window from Aug 6, 14:52–15:32 — 20 prediction polls, 19
GPS polls, 13 MB, plus a `MANIFEST.json` recording exactly where it came from.

Three decisions are built into it:

**1. Raw protobuf, not decoded JSON.** Shipping already-decoded data would be
smaller and simpler — and would bypass the decoder entirely, meaning the grader
never runs the absent-vs-zero logic the whole project is built around. The
sample must enter the pipeline through the same door live data does.

**2. A contiguous window, not a random sample.** Arrival detection works by
watching a vehicle move through states across *consecutive* polls. A random
scatter of polls destroys those sequences and the detector finds nothing.
Contiguity is a correctness requirement.

**3. Gap-checked.** The script refuses windows containing gaps longer than five
minutes. A hole in the data produces missed arrivals that look identical to "this
agency doesn't report arrivals" — an invisible failure.

It also matches the GPS feed to the *same* time window as the predictions,
rather than choosing each feed's best window independently, since arrival
detection needs both feeds covering the same minutes.

---

## 8. Proving it works end to end

A smoke test pushed 200 real records through the running Kafka:

| Check | Result |
|---|---|
| Messages produced | 200 |
| Messages consumed | 200 |
| Spread across lanes | 71 / 100 / 29 |
| Distinct keys | 71 |
| **Keys split across more than one lane** | **0** |

That last row is the one that matters. It confirms the ordering guarantee
tomorrow's arrival detector depends on: every observation of a given trip lands
in one lane, in order. Verified, not assumed.

**Tests:** 16 new contract tests were added. The suite is now **31 tests, all
passing**.

---

## 9. Design decisions, collected

| Decision | Alternative rejected | Why |
|---|---|---|
| Kafka as transport, disk as archive | Kafka as storage | Retention silently deletes; unrecoverable data must live on disk |
| 3 partitions, fixed now | Accept the default | Count is effectively permanent; changing it breaks ordering retroactively |
| `delete` retention | `compact` | Compaction keeps only the latest per key — destroys the state transitions we read |
| 24-hour retention | Weeks | Short window prevents treating the topic as a database |
| Auto-create topics off | Default on | A typo would silently create an empty topic instead of erroring |
| Two named listeners | One address | Containers and host machine need different addresses |
| Pydantic v2, strict | v1, or no validation | v1 converts missing → default, undoing the absent-vs-zero invariant |
| `extra="forbid"` | Ignore unknown fields | Turns silent schema drift into a loud failure |
| Key = `date:trip_id` | `trip_id` alone | Trip IDs repeat daily; needless coupling and skew |
| Sample = raw protobuf | Decoded JSON | Decoded data bypasses the most correctness-critical code |
| Sample = contiguous | Random sample | Arrival detection needs consecutive observations |

---

## 10. Where things stand

**Working today:** Kafka running locally in Docker with four configured topics;
a validated event contract proven against 82,061 real records; a committed 13 MB
replay sample; a verified round trip through Kafka with ordering confirmed; 31
passing tests. The archiver is still running — 309 polls collected.

**Course requirements now satisfied:** data source ✓, validated event contract ✓,
Kafka topics ✓, pinned dependencies ✓, sample data ✓.

**Tomorrow (Saturday):** the replay producer — reading archived files, validating
each record, publishing to Kafka, with failures routed to the dead-letter topic —
and the consumer skeleton.

**Sunday:** the arrival detector and the first real output.

---

## Glossary (additions to yesterday's)

| Term | Meaning |
|---|---|
| **Kafka** | Software that carries streams of events between programs |
| **Broker** | One running Kafka server |
| **Topic** | A named stream of messages |
| **Partition** | A numbered lane within a topic; ordering is guaranteed only within one |
| **Key** | A label deciding which partition a message goes to |
| **Producer / Consumer** | A program that writes / reads messages |
| **Offset** | A consumer's bookmark within a partition |
| **Retention** | How long messages are kept before deletion |
| **Compaction** | Keeping only the newest message per key — deliberately avoided here |
| **Dead-letter topic** | Where records that fail validation are sent for inspection |
| **KRaft** | Kafka's built-in cluster management, replacing ZooKeeper |
| **Docker** | Software that packages programs with everything they need to run |
| **Image / Container** | The packaged template / a running copy of it |
| **`docker-compose.yml`** | A file describing which containers to run together |
| **Port** | A numbered connection point on a machine (Kafka uses 9092) |
| **Advertised listener** | The address Kafka *tells clients to use* — not necessarily the one they dialed |
| **Healthcheck** | A repeated test telling Docker when a service is genuinely ready |
| **Pydantic** | A Python library that validates data against a definition |
| **Contract / Schema** | The precise definition of what a message must contain |
| **Protobuf** | The compact binary format 511 publishes data in |
