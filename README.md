# transit-lakehouse — Week 0

Real-time transit reliability lakehouse. This is the ingest floor: an archiving
poller and a feed profiler. No Kafka, no Spark, no Delta yet — deliberately.

## Why the archiver comes first

GTFS-Realtime history cannot be re-fetched. Every hour the poller isn't running
is an hour that is gone permanently. The archive is also the durable store
Kafka is *not* — when silver/gold logic turns out to be wrong in Week 4,
reprocessing runs from these files.

So: get this running today, then build the rest of the stack around a growing
archive rather than an empty one.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env        # then paste your 511 key
set -a && source .env && set +a
make poll-once              # single cycle, verify it works
make poll                   # continuous; leave running
```

Request an API key at https://511.org/open-data/token. Default allowance is
60 requests/hour, which with two feed types caps you at a 120s cadence — email
511 developer resources for an increase early, since it's a round-trip.

## Try it without a key

```bash
make fixture    # synthetic two-agency archive
make profile
```

The fixture deliberately models two agencies with *different* field population,
because that heterogeneity is the main thing Week 0 needs to discover.

## The Week 0 workflow

1. Run the poller for ~3 hours across a service peak.
2. `make profile`
3. Paste the output into `docs/decisions/ADR-002-feed-profile.md`.

The profile answers seven questions, and every later design decision hangs off
them:

| Q | Question | Decides |
|---|---|---|
| Q1 | Is `current_status`/`stop_sequence` populated? | Whether resolver A (agency's own arrival call) is usable, per agency |
| Q2 | Do predictions settle to `uncertainty=0`? | Whether resolver C is usable |
| Q3 | Is the feed actually advancing? | Real poll cadence vs. wasted API budget |
| Q4 | `schedule_relationship` distribution | Exclusion rules for SKIPPED / CANCELED / ADDED |
| Q5 | StopTimeUpdate fan-out ratio | Storage sizing |
| Q6 | Delay absent vs zero | Magnitude of the pitfall 2.1 corruption you avoided |
| Q7 | Do any trips revisit a stop_id? | Confirms grain must be `stop_sequence`, not `stop_id` |

## What's enforced in the code

- **Presence-aware decoding.** GTFS-RT is proto2, so `HasField()` distinguishes
  "delay is 0" from "delay is absent." Every scalar read goes through `_get()`.
  Conflating them injects fake on-time records that look completely plausible
  and corrupt every downstream metric and ML label. `test_absent_delay_is_not_zero`
  is the load-bearing test in this repo.
- **Archive before decode.** Raw bytes hit disk before parsing is attempted;
  a decode failure quarantines the payload rather than losing it.
- **Producer timestamps as event time.** `vehicle_report_ts` / `trip_update_ts`
  come from the producer, never from our poll clock. `ingest_ts` is recorded
  separately and is never used for event time.
- **Client-side rate budget.** Enforced locally rather than discovered via 429s,
  because a throttled token stops the archive.
- **Stale feed detection.** Payload hash plus header timestamp; identical
  payloads are archived but flagged.
- **`poll_interval_s` stamped on every row**, so downstream consumers can bound
  label quantization error even if the cadence changes mid-project.

## The one operational rule

**The poller runs as exactly one instance.** It is not a Kafka consumer, it has
no lag, and N replicas means N× the API calls and a throttled token. When this
moves to Kubernetes, cap the Deployment at 1 replica. Scale consumers on lag;
never the poller.

## Layout

```
ingest/poller/
  decode.py      presence-aware GTFS-RT decoding  <- the correctness core
  poller.py      fetch, archive, rate budget, stale detection
profiling/
  profile_feed.py  Week 0 evidence generator -> ADR-002
tests/
  test_decode.py   11 tests; absent-vs-zero is the critical one
  make_fixture.py  synthetic archive, no API key required
data/                (gitignored except data/replay_sample/)
  raw/{feed}/ingest_dt=.../*.pb.gz        original bytes, never deleted
  decoded/{feed}/ingest_dt=.../*.jsonl.gz flattened rows
  quarantine/                             unparseable payloads + error text
```

The partition is **ingest date** — the UTC date of our poll — not GTFS service
date. Measured on the live regional feed, a single payload carried trips from
three different service dates plus 2,858 rows with no `start_date` at all, so
service date is a property of a row and not of a payload. Partitioning by true
`service_date` belongs in bronze, after the explode to one row per
StopTimeUpdate.

This was originally (and wrongly) named `service_dt` while being filled from the
poll clock, which filed every poll between 17:00 PT and midnight under
tomorrow's date — silently dropping PM peak from any partition-pruned query.

## Next

- `docs/decisions/ADR-002-feed-profile.md` — paste the profile output
- `docs/decisions/ADR-003-arrival-resolver.md` — pick the precedence ladder
  using Q1/Q2, and benchmark against MTC's published stop-observations dataset
- Then Kafka, and the poller's writer swaps from disk to a producer
