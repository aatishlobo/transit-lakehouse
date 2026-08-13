# Q&A preparation

**Final presentation, Thursday 13 August, 6:16-6:24pm.** One minute of Q&A, so
realistically two or three questions.

Question 11 is worth reading first: the class used Confluent Cloud and we did
not, so someone is likely to ask.

Each answer below has a **short version** (what you actually say — 20 to 30
seconds) and **if pressed** (depth if they follow up). Say the short version and
stop. Trailing off into detail nobody asked for is how a good answer becomes a
weak one.

The single most valuable habit here: **name the weakness before they do.** Every
answer that concedes something specific reads as competence, not doubt.

---

## 1. Why Kafka at all? You're replaying files from disk — couldn't you just process them directly?

**Short:** For our submitted demo, honestly, we could. Kafka earns its place for
three reasons: the poller writes once and any number of consumers can read
independently; consumers process at their own pace without the producer waiting;
and — the real one — keyed partitioning gives us ordering guarantees per trip,
which our arrival detection depends on. Without ordered delivery per trip we'd
derive arrivals at the wrong moments.

**If pressed:** The architecture also anticipates a second consumer. A windowed
analytics job could read the same topics without touching our code or the
archive. That's the decoupling argument, and it's the honest reason a batch
script wouldn't generalise.

---

## 2. Why three partitions? What happens when you need more?

**Short:** Three was chosen deliberately, not defaulted. It's enough to
demonstrate real parallel consumption on one broker while staying
comprehensible. The important part is that partition count is effectively
permanent: Kafka routes by hashing the key modulo partition count, so adding a
partition changes which partition a key maps to. A trip's history would split
across two partitions and the ordering guarantee would break **retroactively**,
for data already written.

**If pressed:** The migration path is to create a new topic with the new
partition count and reprocess from the archive — which we can do precisely
because the archive, not Kafka, is our system of record. That's a concrete
payoff of that design choice.

---

## 3. Why `cleanup.policy=delete` rather than `compact`?

**Short:** Compaction keeps only the most recent message per key and discards
the rest. Our data *is* the history — the whole method is watching a vehicle
move from `IN_TRANSIT_TO` to `STOPPED_AT` across consecutive observations.
Compaction would keep only the final observation per trip and throw away exactly
the transitions the resolver reads. It would look like an optimisation and
silently destroy the signal.

**If pressed:** Compaction would suit a topic representing current state — "the
latest known position of every vehicle." That's a different product. Ours is an
event log.

---

## 4. What are your delivery guarantees? Exactly-once or at-least-once?

**Short:** At-least-once, deliberately. We enable the idempotent producer, so
the broker de-duplicates retries on the write side. On the read side we commit
offsets manually *after* processing, which means a crash can reprocess a batch
but never skip one. We absorb the duplicates by making the output idempotent —
arrivals are keyed on `(service_date, trip_id, stop_sequence)`, so reprocessing
converges to the same table.

**If pressed:** We chose not to use Kafka transactions. Exactly-once across a
read-process-write cycle adds real complexity, and our grain already makes
duplicates harmless. We proved that rather than assumed it: the acceptance suite
runs the resolver twice over identical input and asserts the outputs are
bit-for-bit identical.

**Worth volunteering:** we found a real duplicate bug this way. Replaying twice
doubled our sample count while leaving every average, median and percentile
identical — the output looked completely correct while `n` silently lied. That's
now deduplicated and regression-tested.

---

## 5. Why is Kafka not your storage layer?

**Short:** GTFS-Realtime has no history endpoint. If we don't capture a moment,
it's gone permanently. So the durable record is the raw archive on disk, and
Kafka topics expire after 24 hours on purpose — a short retention makes it
impossible to drift into treating the topic as a database.

**If pressed:** The specific failure we're avoiding: you build on a topic, the
retention window silently deletes last month, and there's no recovery path. The
poller therefore only ever writes to disk, and everything downstream only ever
reads from disk. Live and replay are literally the same code path.

---

## 6. Your consumer is single-threaded Python. Isn't that a bottleneck? Why not Spark or Flink?

**Short:** Our proposal listed Spark Structured Streaming with a plain-Python
consumer as a sanctioned fallback, and we took the fallback up front rather than
discovering it late. For our volume it's genuinely sufficient — the resolver
processes 38,000 positions in 16 seconds, and the full pipeline runs in about two
minutes.

**If pressed:** The honest limit is the aggregator: it joins across the whole
replay window in memory, which is fine for 39 minutes and would not survive a
day. A production version needs a windowed join with watermarks and state
eviction, which is exactly what Flink or Spark give you for free. That's the
strongest argument for the rewrite, not raw throughput.

**Also true:** because we key by trip, the work is already partitioned correctly.
Scaling out means running up to three consumers in the group with no code change.
Beyond three we'd need more partitions — see question 2.

---

## 7. You have no Schema Registry. How do you handle schema evolution?

**Short:** Pydantic contracts plus a `contract_version` stamped on every event.
That gives us validation and traceability, but not centrally enforced
compatibility — a producer could still publish a breaking change and nothing
external would stop it. That's a real gap.

**If pressed:** Two things partially compensate. `extra="forbid"` means an
unexpected field fails immediately rather than being silently dropped, so
producer-side drift is loud. And failures go to a dead-letter topic rather than
crashing consumers, so a bad deploy degrades instead of halting. But a registry
with BACKWARD compatibility checks is the correct answer at production scale, and
we'd add it before a second team consumed these topics.

---

## 8. One broker, replication factor 1. What breaks in production?

**Short:** Everything about durability. One broker means no failover, and RF=1
means a disk failure loses the log. We accepted that specifically because Kafka
isn't our system of record — losing the log costs us a replay, not data.

**If pressed:** Production needs at least three brokers with RF=3 and
`min.insync.replicas=2`, so a broker can fail without data loss or unavailability.
We'd also need consumer lag monitoring, which we don't have — right now we'd only
notice a stalled consumer by looking. That's the first thing I'd add.

---

## 9. How do you know your derived arrivals are actually correct? You have no ground truth.

**Short:** We don't have ground truth, and we say so. What we have is
provenance: every arrival records the method that produced it, its confidence,
and the poll interval that bounds its precision. We also measured our own bias
rather than assuming it away.

**If pressed:** Three specific distortions, all measured. We capture about 21% of
stop events because vehicles dwell for seconds and we sample every 120 seconds. A
vehicle is only visible as `STOPPED_AT` between arriving and departing, so our
timestamps are biased late, never early. And capture probability scales with
dwell time, so terminals and layovers are over-represented. All three push the
same direction, which is why we report our headline figure as an **upper bound**
on agency optimism rather than a point estimate.

**The external check we'd add:** MTC publishes its own stop-observation dataset
derived from the same feed. Benchmarking against a published independent
derivation would turn "we have no ground truth" into "we agree with a reference
implementation to within X seconds."

---

## 10. What's your biggest weakness, and what would you do differently?

**Short:** Coverage. We poll every 120 seconds because our quota is 60 requests
an hour, and that single constraint causes our three largest problems — we miss
most stop events, our timestamps are imprecise, and we oversample long stops. The
thing I'd do differently is trivial and I'd do it first: email 511 for a rate
limit increase on day one. It's one email, and it attacks all three at once.

**If pressed, other honest answers:**

- **Our ML model is an afternoon model.** 95.8% of its training data falls in
  hours 14–19, because the laptop running the archiver slept overnight. That's a
  collection artifact, and it means the model shouldn't be trusted for morning
  predictions.
- **One operator, six days.** No seasonal, holiday, or weather variation.
- **We never validated against the published schedule.** We measure prediction
  accuracy — actual versus predicted — not true on-time performance against
  GTFS-Static. That needs a slowly-changing schedule dimension and an as-of join,
  which was out of scope.

---

## 11. Everyone else used Confluent Cloud. What are you running?

**Short:** Apache Kafka, the open-source software, in a Docker container on a
laptop. Confluent is a company that sells hosted Kafka and tooling — Kafka itself
is free and Apache-licensed. We are even using the same client library the class
used, `confluent-kafka`; it is just a client, and the Kafka wire protocol is a
public standard, so any client talks to any broker.

**If pressed — what would change to point at Confluent Cloud:** the bootstrap
address plus four authentication lines. `security.protocol=SASL_SSL`, the
mechanism, and an API key and secret. Producer, consumer, contracts, topics,
partitioning and the resolver are all untouched. The design is not tied to a
local broker.

**Why we chose local:** the handout permits a local Kafka-compatible setup, and
our review path is deterministic replay with no API key. A cloud path would
require giving the reviewer working credentials to live resources, and any quota
or expiry problem on their end becomes a grading problem. `docker compose up`
works on any machine with nothing to keep alive.

**What we give up — and the first one is real:** no Schema Registry, so
compatibility is enforced by our Pydantic contracts and a version stamp rather
than centrally; no replication or failover on a single broker; and no managed
monitoring, so we have no consumer lag alerting.

---

# Reserve answers

Questions that are plausible but less likely.

**"Why key on `service_date:trip_id` rather than just `trip_id`?"**
Trip IDs repeat every day — today's and yesterday's runs of the same scheduled
trip share an ID. Keying on trip alone would force different days into one
partition, coupling them needlessly and skewing load.

**"What if a partition gets hot — one route dominating?"**
It's possible; we didn't observe it because trip IDs distribute well. The fix
would be a composite key or a custom partitioner. Worth measuring before solving.

**"How do you handle late or out-of-order data?"**
Within a trip we don't have to — the key guarantees ordering. Across trips we
don't need global ordering. The place it would matter is the windowed join we
don't have yet, which is where watermarks come in.

**"Why not use Kafka Streams or ksqlDB?"**
Kafka Streams is JVM-only and our stack is Python. ksqlDB would have suited the
aggregation well; we chose explicit Python because the arrival resolver is
stateful logic that reads more clearly as code than as SQL.

**"What does the dead-letter topic actually catch?"**
Zero records in our runs — everything validated. It exists because the
alternative to routing bad records aside is either crashing the pipeline on one
malformed row, or dropping it silently and destroying the evidence.

**"How much data are you handling?"**
1.67 million records replayed in about 33 seconds, roughly 50,000 records per
second on a laptop. The live feed is about 80,000 records per poll, every two
minutes.

**"Is this reproducible?"**
Yes — `make demo`, no API key, about two minutes. We verified it from a clean
extract of the submitted zip: fresh virtual environment, 83 tests, full pipeline,
six acceptance checks.

---

# If you don't know an answer

Say so, then say what you'd do to find out. *"I don't know — I'd check X."*
Inventing an answer is the only genuinely bad outcome, and a specific
investigation plan reads better than a confident guess.

# Two things worth working into any answer

1. **44.2% of records carry no delay value.** Read naively, protobuf returns 0
   for both "absent" and "on time" — we'd have reported 54,000 records as
   perfectly punctual.
2. **We deleted a feature that improved our score.** Day-of-week cut error by 10
   seconds, but Monday appeared only in the test window, so the gain came from
   bin placement rather than learned structure.
