# Interview prep: Real-Time Transit Reliability Lakehouse

One-on-one. The goal is not to recite the architecture. It is to demonstrate
that you know **which parts of your own numbers you cannot trust, and why**.
That is the thing most candidates cannot do, and it is the thing this project
is unusually strong on.

---

## 1. The 45-second pitch

> Transit agencies publish predictions and GPS positions, but nobody publishes
> the fact that a bus actually arrived. So you cannot measure reliability
> directly, you have to derive it. I built a streaming lakehouse that derives
> arrivals from vehicle position state transitions, then grades the agencies'
> own predictions against them. The hard part is that delay equals actual minus
> scheduled, and scheduled is exact, so every error in my derivation lands
> directly in the answer. Most of the engineering is about making that error
> measurable instead of invisible.

Then stop. Let them pick the thread.

---

## 2. What is actually built (know this cold)

| Layer | State | Evidence |
|---|---|---|
| Ingest / poller | **built**, 19 days running | 4,874 polls, 1.2 GB, unfetchable history |
| Presence-aware decoder | **built** | 44.2% of records carry no delay value |
| Kafka path | **built** | 4 topics, contracts, resolver, dead-letter |
| Spark bronze/silver/gold on Delta | **built** | idempotent MERGE, 36,753 arrivals, 13 agencies |
| GTFS-Static ingest | **built** | 68 MB regional feed, content-addressed |
| SCD2 schedule dimension | **built** | 3,867,322 rows, versioned on schedule attributes |
| As-of join, true OTP | **built** | 99.5% match, join conservation asserted |
| dbt marts | **built** | 4 models, 17 tests passing |
| Dagster asset graph | **built** | 8 assets, 3 asset checks, dbt as an asset edge |
| FastAPI + dashboard | **built** | SQLite export, labels its own staleness |
| Kubernetes manifests | **written, not deployed** | Strimzi, KEDA, single-replica poller |
| ML correction model | **built** | 168.5 s MAE vs 194.7 s agency baseline |

**Be precise about K8s:** the manifests exist and encode real decisions, but
nothing has been deployed to a cluster. Say that plainly.

### The numbers worth memorising

- **44.2%** of StopTimeUpdate rows carry no delay value. Proto2 returns `0` for
  both absent and on-time, so naive code reports ~54,000 records per poll as
  perfectly punctual.
- **15 of 28** operators publish `current_status`. Muni 99.2%, Caltrain 0%.
- **~21%** of stop events captured, because the quota caps polling at 120 s.
- **3.7%** of arrivals carry a vehicle timestamp *ahead* of the poll that saw
  them, by up to 97 s. Vehicle clock skew.
- **36,753** arrivals, **13** agencies, **3.87 M** schedule rows.
- **99.5%** as-of join match rate, with join conservation asserted not reported.
- **49.6%** on-time overall; AC Transit worst at 32.9%, 40.7% more than 5 min late.
- **26%** of arrivals are more than a minute EARLY -- and since labels are
  biased late, the true figure is higher.
- **13.5%** MAE improvement over the agency's own ETA (168.5 s vs 194.7 s).

## 3. The four stories to steer toward

Each is a decision, the alternative you rejected, and what it cost.

### Story 1: absent is not zero

GTFS-Realtime is proto2, where every scalar is optional and absence is
distinguishable from zero only via `HasField()`. Reading `stu.arrival.delay`
returns `0` for both "on time" and "no information".

**Measured:** 44.2% of rows, 54,638 of 123,735 in one sample. Nineteen of 28
operators never populate the field; AC Transit produced 23,469 consecutive
absent rows.

**Why it matters:** written the obvious way, the project reports a large fake
on-time spike. It looks entirely plausible, it corrupts every downstream metric
and every ML label, and nothing throws.

**The rule:** no scalar is ever read directly; everything goes through a helper
that returns `None` on absence. The load-bearing test asserts absent stays
distinct from zero, and there is a check that re-parses raw protobuf and
compares field *presence* against decoded output across 408,149 rows.

### Story 2: the arrival is the first sighting, not the last

A vehicle waiting at a stop reports `STOPPED_AT` on every poll for the whole
dwell. The last such report measures **departure**; an intermediate one
measures nothing.

**Rejected alternative:** prediction settlement (take the last predicted
arrival before the vehicle passes). It has much better coverage. It is
disqualified because the ML model uses agency predictions as *features* — so
labels derived that way would teach the model to copy the agency's forecast and
produce an excellent, meaningless MAE. **Labels come from positions, features
come from predictions, and the two never mix.**

**Cost, stated plainly:** coverage drops to ~21%, and only 15 of 28 operators
can be resolved at all.

### Story 3: I measured the bias in my own labels

Three distinct effects, deliberately not conflated:

1. **Coverage** — most stop events are missed. A sample-size limit.
2. **One-sided offset** — a vehicle is observable as `STOPPED_AT` only between
   arriving and departing, so a derived timestamp falls at or *after* true
   arrival, never before. Biased **late**.
3. **Selection bias** — capture probability scales with dwell time, so
   terminals and layovers are over-represented.

All three push the same direction, so the headline number is reported as an
**upper bound on agency optimism**, not a point estimate.

New, from the Spark build: **3.7% of arrivals have a vehicle clock ahead of our
poll clock**, up to 97 seconds. A fourth bias source, and one I did not
anticipate — it came out of a test I wrote expecting zero violations.

And from the OTP join: **26% of arrivals are more than a minute early.**
Because the labels are biased *late*, the true early rate is higher still.
That is schedule padding, and it matters because an early bus is a worse
failure than a late one -- you miss it entirely. It is why every mart reports
early and late separately rather than folding them into one "off-schedule"
number.

### Story 4: I deleted a feature that improved the score

Day-of-week cut MAE by 10 seconds. It was removed anyway: over a six-day
archive `dow` is nearly a unique ID per calendar day, and Monday appeared
**only** in the test window — 31.2% of test rows carried a value the model had
never seen, silently inheriting an adjacent day's correction through bin
placement.

> *A number you cannot account for is not a result.*

This is now a general rule in the training code: any low-cardinality feature
whose test values are absent from training is dropped, with the reason logged.

---

## 4. The three decisions most worth volunteering

### DST, and the bug I wrote and then caught

GTFS measures times from **noon minus 12 hours** of the service day, not from
midnight -- because noon is never ambiguous while midnight can be skipped or
repeated by a DST transition. My first implementation subtracted a `timedelta`
from an aware datetime, which is *wall-clock* arithmetic inside the zone and
collapses straight back to naive local midnight. Every scheduled time on a
transition day was an hour off, in **opposite directions** for spring-forward
and fall-back, which is why testing one transition would not have caught it.

Fix: convert to UTC first, then subtract. 14 tests pin both 2026 transitions,
past-midnight times like `25:14:00`, and the 23/24/25-hour service days.

### The as-of join, and why `is_current` is the wrong join key

Agencies republish timetables constantly. Joining facts to the *current*
schedule silently recomputes every historical OTP number every time a new feed
lands -- last month's figures change, nothing errors, and the metric becomes
unauditable. So the dimension is SCD2 with tiling validity windows and the join
predicate is `valid_from <= as_of < valid_to`.

The as-of instant is the **start of the trip's service day**, not the arrival
timestamp, so a trip running past midnight resolves against its own service
day's schedule.

I also had to make an honest call: I began fetching GTFS-Static after the
realtime archive was already running, so no observed schedule version covers
the earlier dates. The first version is seeded at epoch, and every row carries
`valid_from_assumed` so any analysis that cannot tolerate the assumption can
filter it out. Stating uncertainty in the data beats burying it in a README.

### Join conservation as an assertion, not a metric

The as-of join is a LEFT join whose output row count is **asserted** equal to
the input. An inner join would silently drop every arrival whose trip is
missing from the schedule -- and those absences are not random, they
concentrate in added and unscheduled service, which is the service most likely
to be late. The metric would improve because the inconvenient rows vanished.

A changed row count also detects the other failure: two open dimension versions
for one key, which duplicates facts. That is why the SCD2 test asserting exactly
one current version per key exists.

### If asked about Kubernetes

> The manifests are written but nothing is deployed. The decision I'd defend is
> that the **poller is exactly one replica**, with `strategy: Recreate` and a
> PodDisruptionBudget so a rolling update can't briefly run two. It's not a
> Kafka consumer -- no group, no partitions, no lag to divide -- so a second
> replica doesn't add throughput, it doubles API calls against a 60/hour quota
> and gets the token throttled. Consumers scale on lag via KEDA, capped at the
> partition count, because replica four would hold no assignment.

## 5. Likely questions

**"Why Kafka if you're replaying files?"**
Keyed partitioning gives per-trip ordering, which the resolver depends on —
it detects arrivals from state transitions across consecutive observations, so
out-of-order delivery derives arrivals at the wrong moments. Plus the archive
is written once and any number of consumers read independently.

**"Why is Kafka not your storage layer?"**
GTFS-Realtime has no history endpoint. If a moment isn't captured it's gone
permanently, so the durable record is the raw archive on disk and topics expire
after 24 hours *on purpose* — a short retention makes it impossible to drift
into treating a topic as a database.

**"Why three partitions? What if you need more?"**
Partition count is effectively permanent: routing is key-hash modulo partition
count, so adding one changes which partition a key maps to and breaks the
ordering guarantee **retroactively**, for data already written. The migration
is a new topic plus reprocessing from the archive — possible precisely because
the archive, not Kafka, is the system of record.

**"Exactly-once or at-least-once?"**
At-least-once, deliberately, with an idempotent MERGE on the gold grain.
Exactly-once across a read-process-write cycle buys complexity I don't need,
because the grain already makes duplicates harmless. I proved it rather than
assumed it: re-running the merge leaves the row count identical, and that's a
test.

**"How do you know your arrivals are right? You have no ground truth."**
I don't have ground truth, and I say so. What I have is provenance on every
row — method, confidence, and the poll interval that bounds precision — plus a
measurement of my own bias. The external check I'd add is MTC's published
stop-observation dataset, derived from the same feed by an independent method.
That converts "no ground truth" into "agrees with a reference implementation to
within X seconds."

**"What's your biggest weakness?"**
Coverage. The 60 requests/hour quota caps polling at 120 s, and that single
constraint causes the three largest problems at once — missed events, imprecise
timestamps, oversampled long dwells. The fix is one email to 511 requesting a
rate limit increase, and I'd do it first if I started over.

**"What would you do differently?"**
Ask for the rate limit on day one. And I'd have caught the vehicle clock skew
earlier — I only found it because I wrote a test asserting arrivals precede
their poll, expected zero violations, and got 1,376.

---

## 6. Rules for the room

1. **Name the weakness before they do.** Every answer that concedes something
   specific reads as competence.
2. **Lead with the measurement, not the tool.** "44.2% of records have no delay
   value" beats "I used Kafka."
3. **If you don't know, say so and say how you'd find out.** A specific
   investigation plan beats a confident guess. Inventing detail about Dagster or
   K8s is the only genuinely losing move available.
4. **Stop talking after the answer.** Trailing into unasked detail turns a good
   answer into a weak one.
