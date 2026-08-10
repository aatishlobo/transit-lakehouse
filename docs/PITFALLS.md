# DE#1 — Pitfall Register

*Companion to the build spec. Working document — add rows as you hit things.*

**How to use this:** Tier 0 items are irreversible or expensive-to-fix and should be handled before Week 0 code. Everything else is organized by layer, roughly in the order it will bite you. Each entry has a severity, the symptom you'd actually observe, and the mitigation.

Severity key: **[S1]** silently corrupts data or the ML label · **[S2]** breaks the pipeline loudly · **[S3]** costs time, money, or credibility

---

## Tier 0 — highest regret, do these first

These are the ones where the cost of discovering them in Week 5 is enormous.

**0.1 — Not archiving raw bytes from day one. [S1]**
You cannot re-fetch GTFS-RT history. If your silver/gold logic is wrong in Week 4 and bronze doesn't contain everything needed to reprocess, that history is gone permanently. Bronze must store the *fully decoded, lossless* payload — every field, including ones you don't use yet — plus the raw protobuf bytes for at least the first few weeks while you're still learning the feed's shape. Cheapest insurance in the project.

**0.2 — Request the 511 rate limit increase now. [S3]**
Default is 60 requests/hour per token. You need TripUpdates + VehiclePositions, so that's a 2-minute effective cadence, which puts a ±60s quantization floor on your arrival labels when typical delays are 2–5 minutes. It's an email round-trip to 511 developer resources, so start it before you need it. Record the granted cadence as a documented project constraint.

**0.3 — VACUUM will silently destroy your ML contract. [S1]**
Delta's default `deletedFileRetentionDuration` is 7 days. §5 of the spec promises MLE#1 can reconstruct the gold table as of any past prediction time via time travel. Those two facts are in direct conflict. Set `delta.logRetentionDuration` and `delta.deletedFileRetentionDuration` explicitly on the gold table (long enough to cover your intended training window), document the storage cost you're accepting, and never run a bare `VACUUM`. This is the single easiest way to invalidate the whole one-substrate strategy without noticing.

**0.4 — Do the volume arithmetic before committing to storage. [S3]**
Regional feed, N agencies, your polling cadence, average entities per poll, average StopTimeUpdates per TripUpdate (the fan-out is the part people forget — one TripUpdate explodes into tens of rows). Estimate bytes/day for bronze before Week 1, not after MinIO fills up.

**0.5 — Secrets. [S3]**
The 511 API key will end up in a notebook, a docker-compose file, or a commit message. Set up `.env` + `.gitignore` + a secret manager path (or at minimum a K8s Secret) in Week 0, and add a pre-commit hook that greps for the key pattern. Rotating a leaked key mid-project also means re-requesting your rate limit increase.

**0.6 — Check the 511 terms of use for public redistribution. [S3]**
You're planning a public dashboard. Confirm attribution requirements and whether redistributing derived data is permitted before Week 7, not after you've published it.

---

## 1. Source / API layer

**1.1 — Stale feed served as fresh. [S1]**
The API will happily return the identical payload twice. If you timestamp rows with your poll time, you manufacture duplicate "observations" at distinct times. Use `FeedHeader.timestamp` and each entity's own `timestamp` field as the event time; dedupe on `(entity_id, entity_timestamp)`. Monitor: alert when `FeedHeader.timestamp` fails to advance across N consecutive polls.

**1.2 — HTTP 200 with a non-protobuf body. [S2]**
Error pages, rate-limit messages, and maintenance HTML all come back as 200 with the wrong content type. Never trust status code alone — validate that the parse succeeded and the feed contains entities, and route parse failures to a quarantine path rather than crashing the poller.

**1.3 — Missing `Accept` header. [S2]**
The 511 docs require `application/x-google-protobuf` or `application/octet-stream` explicitly if you don't specify a response format. Omit it and you may get something you don't expect.

**1.4 — Silent agency dropout. [S1]**
An agency stops appearing in the regional feed for six hours. Your regional OTP number changes because the *composition* changed, not because service changed. This looks like a real trend and it isn't. Build a per-agency-per-hour coverage table from day one and make the dashboard show it; regional aggregates should be composition-aware or excluded when coverage is incomplete.

**1.5 — RT/static version skew. [S1]**
The RT feed matches 511's regional static feed, not the agency's own. Pull static exclusively from 511. Separately: static is republished daily and RT trip_ids reference the *current* version — if your static ingest is a day behind, RT trips won't resolve.

**1.6 — Orphan trips. [S1]**
`trip_id`s appear in RT that don't exist in any static version you hold: `ADDED` trips, `DUPLICATED` trips, or your static ingest lagging. Decide the policy explicitly — drop, quarantine, or late-bind on the next static ingest — and count them. A rising orphan rate is your early warning that static ingest has broken.

**1.7 — `trip_id` churn across static versions. [S1]**
Some agencies do not keep trip_ids stable when they republish. If yours don't, your fact rows' `trip_id` stops joining to the new dimension version and history goes dark. Measure churn between consecutive static versions in Week 2 — before you build the SCD2 join on the assumption of stability. If churn is high, you need a natural key (route + direction + first departure time + service_id) as a fallback join path.

**1.8 — Backfill has coarser static granularity than you need. [S3]**
Historical regional static feeds are available monthly. Service changes happen 3–4× a year, so monthly is *usually* fine — but a mid-month service change means some historical days join against the wrong schedule version. Bound the error, note it, move on.

**1.9 — Single point of failure on 511 itself. [S3]**
Feed format changes, endpoint deprecations, and outages are all outside your control. Subscribe to the developer resources mailing list. Your bronze archive is what makes you resilient.

---

## 2. Protobuf decoding

**2.1 — Absent vs zero. [S1]**
The nastiest silent bug in the whole project. In proto3, a scalar field that's absent and a scalar field set to `0` can be indistinguishable unless you use presence checks. `delay = 0` means "exactly on time"; `delay` unset means "no information." Conflate them and you inject a spike of fake on-time records into your delay distribution — which will look plausible and shift every metric and every model. Use `HasField()` / presence-aware decoding, and carry explicit nulls into bronze rather than defaulted zeros.

**2.2 — Pin the proto and library versions. [S2]**
GTFS-RT has evolved (and continues to). An unpinned `gtfs-realtime-bindings` upgrade mid-project changes decode behavior. Pin, and record the version in bronze as a column so you can tell which decoder produced which rows.

**2.3 — Agency proto extensions dropped silently. [S3]**
Some feeds carry extensions. Unregistered extensions are discarded without error. Check whether the regional feed uses any before you assume you've captured everything.

**2.4 — Fan-out sizing. [S3]**
One `TripUpdate` entity contains many `StopTimeUpdate`s. Naive explode-then-write can be a 30–100× row multiplier over what you'd estimate from entity counts. This is what breaks your volume estimate in 0.4.

---

## 3. Kafka

**3.1 — Non-idempotent producer. [S1]**
A retry on an ambiguous ack produces a duplicate. `enable.idempotence=true` costs nothing. Do it in Week 1, not as a fix in Week 5.

**3.2 — Poller restart replays the same poll. [S1]**
The poller crashes after producing but before committing its "last polled" marker, restarts, re-polls, re-produces. Your dedup key must make this a no-op: `(feed_type, feed_header_timestamp, entity_id)` is the natural key. Test it by killing the poller mid-poll deliberately.

**3.3 — Treating Kafka retention as your archive. [S1]**
It isn't. Bronze Delta is. Set Kafka retention to whatever your replay-for-debugging window needs (days, not months) and be explicit that reprocessing runs bronze→silver→gold, not from Kafka.

**3.4 — Partition count is effectively immutable. [S2]**
Increasing partitions rehashes keys, so a given `trip_id` moves to a new partition and per-key ordering across the change is broken. Pick a count in Week 1 with a little headroom.

**3.5 — Schema Registry compatibility surprises. [S2]**
Default is BACKWARD. Adding a field without a default, removing a field, or changing a type will be rejected — or worse, accepted under a looser mode and break consumers. Decide the compatibility mode deliberately and write the evolution policy into an ADR.

**3.6 — Don't log-compact these topics. [S1]**
Compaction keeps only the latest value per key. You want the full event history. If you key by `trip_id` and enable compaction, you delete most of your data. Retention-based deletion only.

**3.7 — Rebalance thrash under KEDA. [S2]**
Scaling consumers triggers group rebalance; aggressive scaling means constant rebalancing and no progress. Use the cooperative-sticky assignor and generous KEDA cooldowns.

---

## 4. Spark Structured Streaming

**4.1 — Checkpoints are coupled to the query plan. [S2]**
Change the streaming query's structure and the checkpoint becomes incompatible; you either reset (losing offset position) or do a careful migration. Mitigation is architectural: **keep the bronze job dumb and stable.** No business logic, no schema opinions, nothing that would ever require a redeploy. All the logic that will change lives in silver/gold where you can reprocess freely.

**4.2 — Idle source stalls the watermark, which stalls all output. [S1]**
In append mode with a watermark, output is only emitted once the watermark passes. Watermark advances with the *minimum* event time across sources/partitions. One quiet agency, one dead partition, or an overnight service gap and the watermark freezes — the stream keeps running, reports healthy, and emits nothing. Extremely confusing to debug. Know this is possible, monitor watermark advancement as a first-class metric, and understand how your source layout affects it.

**4.3 — Unbounded state growth. [S2]**
Stateful operations (dedup, stream-stream joins) retain state keyed on values that grow forever. `dropDuplicates` without a watermark keeps every key seen, ever. Always pair state with a watermark, and monitor state store size.

**4.4 — First run consumes everything. [S2]**
No `maxOffsetsPerTrigger` and the initial batch tries to process the entire topic. OOM, or a multi-hour first micro-batch. Set it from the start.

**4.5 — `foreachBatch` retries aren't free. [S1]**
A batch can be retried, so your MERGE can execute twice. Make the MERGE naturally idempotent on the gold grain (which you're doing anyway) or dedupe on `batchId`. Don't rely on "it probably won't retry."

**4.6 — Session timezone. [S1]**
Set `spark.sql.session.timeZone=UTC` explicitly and store every timestamp in UTC. Convert to `America/Los_Angeles` exactly once, in the serving layer. Half the timezone bugs in §6 below disappear if this is disciplined from Week 0.

**4.7 — Checkpoint directory file explosion. [S3]**
Offset and commit metadata accumulate as thousands of small files, which is slow on object storage. Tune the metadata retention config; don't discover this when stream startup takes ten minutes.

---

## 5. Delta / lakehouse

**5.1 — `ConcurrentAppendException` on MERGE. [S2]**
Two writers touching the same partition. Include a partition predicate in the MERGE condition so Delta can prove they don't conflict, and add retry-with-backoff.

**5.2 — `mergeSchema` silently accepting typos. [S1]**
Autofix-by-schema-evolution means a renamed or misspelled column quietly becomes a new column with nulls in the old one. Disable schema evolution on production write paths; make schema changes explicit migrations.

**5.3 — Z-ORDER cargo cult. [S3]**
Z-ORDER on a column you've already partitioned by does nothing. Z-ORDER on very high cardinality helps less than you'd hope. The value here is in showing the before/after measurement, so measure the right thing: dashboard query latency, on the columns the dashboard actually filters.

**5.4 — Partition cardinality. [S3]**
`service_date` alone is right. `(service_date, agency)` with 30 agencies × 365 days is 10,950 partitions of small files. If you feel the urge to add a second partition column, measure first.

**5.5 — Local MinIO vs real S3. [S2]**
Delta's commit protocol relies on atomic operations that differ between MinIO and S3, and multi-writer setups need the right `LogStore` configuration. Something that works locally can fail in the cloud, and vice versa. Test the cloud path once, early — not for the first time in Week 7 when you're trying to ship the demo.

**5.6 — OPTIMIZE vs storage cost. [S3]**
Rewriting files creates new versions while old files are retained for time travel. Combined with 0.3 (long retention), storage grows faster than you expect. Budget for it.

---

## 6. SCD2 schedule dimension — the crown jewel, and the densest cluster of bugs

**6.1 — Interval boundary inclusivity. [S1]**
Use half-open `[valid_from, valid_to)` universally, and write it in the docstring. Closed intervals produce either duplicate matches or gaps at version boundaries, and the resulting bug affects exactly one day per version change — rare enough to survive testing, common enough to be wrong.

**6.2 — Daily republish creating spurious versions. [S1]**
The static feed is published daily, mostly unchanged. Version on *content change*, not on publish. Hash the relevant subset (trips, stop_times, routes, calendar), normalizing row order, whitespace, and quoting — an order-dependent hash gives you a new "version" every day and your dimension becomes useless.

**6.3 — Choosing the versioning grain. [S1]**
Whole-feed versioning is simple, but one changed stop time re-versions everything, and then "which version was in effect" is coarse. Per-trip SCD2 is more work but is what actually answers the question. Decide deliberately and write the ADR — this is the decision the whole "correctness spine" rests on.

**6.4 — `calendar.txt` + `calendar_dates.txt`. [S1]**
The schedule version being in effect does not mean a given trip *ran* on a given date. Service ID resolution — weekday pattern plus exception dates — is a second correctness problem sitting behind the first. A trip that didn't run should not appear as a missing arrival.

**6.5 — Times past 24:00:00. [S1]**
Already flagged in the spec, but the mechanics matter: `stop_times.arrival_time` is a **duration from the start of the service day**, not a clock time. Parse it as seconds-since-service-day-start. Any code path that does `strptime("25:14:00")` fails, and any code path that silently wraps to `01:14:00` on the wrong date is worse.

**6.6 — DST arithmetic. [S1]**
The correct order of operations: take `service_date`, localize midnight in `America/Los_Angeles`, add the duration from 6.5, then convert to UTC. Doing the arithmetic in UTC breaks twice a year — the spring-forward service day has 23 hours, and one hour of scheduled times shifts by 60 minutes. Every trip on those two days gets a fake hour of delay. Write a unit test that pins both transition dates.

**6.7 — As-of join fan-out. [S2]**
A subtly wrong as-of join silently duplicates fact rows (matching two dimension versions) or drops them (matching none). Add a hard assertion: fact row count before the join must equal fact row count after. This catches both failure modes immediately and is a two-line test.

---

## 7. The arrival resolver

*(Detailed separately, but the pitfalls belong in the register.)*

**7.1 — Resolver bias, not noise, is the enemy. [S1]**
Geofencing biases arrival early; TripUpdate settlement inherits the agency's prediction error. Both shift the delay distribution systematically. Emit `arrival_method` provenance so this is filterable rather than baked in.

**7.2 — Resolver heterogeneity becomes a fake agency effect. [S1]**
If BART resolves one way and AC Transit another, `agency` correlates with label bias, and any model or dashboard breakdown by agency partly measures your own code. Report resolver mix per agency.

**7.3 — Polling cadence sets the noise floor. [S1]**
Store `poll_interval_s` on every fact row so downstream consumers can bound the label error, and so the number changes correctly if your rate limit changes mid-project.

**7.4 — "Missing" conflated with "didn't stop." [S1]**
A vehicle that legitimately passed a stop without stopping, a `SKIPPED` stop, a `NO_DATA` stop, and a stop you simply failed to observe are four different things and must be four different states. Collapsing them into null makes your coverage metrics and your OTP denominator wrong.

**7.5 — Frequency-based trips have no scheduled stop times. [S3]**
Delay is undefined for them. Detect and exclude explicitly; don't let them become silent nulls.

**7.6 — Grain must be `(service_date, trip_id, stop_sequence)`. [S1]**
Not `stop_id` — loop and circulator routes serve the same stop twice in one trip, and the spec's stated grain collapses those two events into one. `stop_id` is an attribute.

---

## 8. dbt

**8.1 — Lookback window vs watermark mismatch. [S1]**
Already in the spec. The specific failure: lookback narrower than the watermark permits means late facts land outside the window and are never picked up — permanently missing, no error. Tie the two numbers together in config so they can't drift, and run a periodic wide reconciliation pass to catch stragglers.

**8.2 — Incremental and full-refresh paths diverge. [S1]**
`is_incremental()` branches mean two different SQL bodies, and only one gets exercised day to day. CI should build both and diff the results on a fixed window.

**8.3 — Tests that pass on empty tables. [S1]**
`unique` and `not_null` are vacuously true against zero rows. Add row-count and expected-range assertions so a silently empty upstream fails loudly.

**8.4 — Hour-of-day truncation in UTC. [S1]**
"Delay by hour" computed with UTC truncation is shifted 7–8 hours from what any rider or interviewer expects, and it's off by a *different* amount across the DST boundary. Most user-visible bug in the project. Convert to Pacific before truncating, in exactly one place.

**8.5 — Incremental strategy prerequisites. [S2]**
`merge` needs a `unique_key` and appropriate file format; `insert_overwrite` needs partition overwrite mode configured correctly. Getting this half-right produces duplicates rather than errors.

**8.6 — Freshness measured on the wrong column. [S3]**
Freshness SLA should watch `ingest_ts` (are we ingesting?), while correctness logic uses `event_ts`. Mixing them means either false alarms during quiet service hours or no alarm when ingestion dies.

---

## 9. Dagster

**9.1 — dbt running mid-static-ingest. [S1]**
If the SCD2 dimension is half-written when the as-of join runs, you get wrong answers with no error. Model the dependency as an asset edge, not a schedule offset and a hope.

**9.2 — Non-idempotent retries. [S1]**
Every asset materialization must be safe to run twice. Dagster will retry.

**9.3 — Backfill storms. [S3]**
Launching 365 daily partitions at once saturates the cluster and can trip API rate limits. Cap concurrency before your first backfill, not after.

**9.4 — Schedule timezone. [S3]**
Dagster defaults to UTC. A "3am compaction window" scheduled in UTC runs at 7 or 8pm Pacific — peak service, peak write contention.

---

## 10. Kubernetes / Strimzi / KEDA

**10.1 — The poller must never autoscale. [S2] — *conceptual error latent in the current spec***
§4 says "scale the poller/consumer on lag via KEDA." The poller is not a Kafka consumer; it has no lag. And scaling it would multiply your 511 API calls, blowing the rate limit and getting your token throttled. **The poller is exactly one replica, and that constraint is itself good interview material** — the pressure signal for a rate-limited source-side poller is fundamentally different from a consumer's. Scale the *consumers* on lag; the poller is a singleton with a leader-election or a `Deployment` capped at 1.

**10.2 — Scaling past partition count. [S3]**
Consumers beyond the partition count sit idle. Cap `maxReplicaCount` at partitions and explain why — it's a cheap detail that reads as real experience.

**10.3 — PVC loss on local cluster teardown. [S3]**
`kind delete cluster` takes your Kafka data with it. Fine, as long as bronze lives in MinIO with a persistent host mount and you knew it would happen.

**10.4 — Silent OOMKilled executors. [S2]**
Spark executors hitting container memory limits get killed and retried; the stream looks slow rather than broken. Watch pod restart counts, not just stream status.

**10.5 — Orphaned cloud resources after teardown. [S3]**
Deleting a cluster does not delete the load balancers, NAT gateways, persistent disks, or static IPs it provisioned. These are the line items that turn "$0, I tore it down" into a surprise bill. `terraform destroy` plus a manual console check the first time, and a billing alarm regardless.

---

## 11. Serving layer

**11.1 — Dashboard querying Delta directly. [S3]**
Interactive latency over object storage is bad. Serve from a small pre-aggregated table (or a cached materialization); keep the lakehouse out of the request path.

**11.2 — The always-on vs ephemeral contradiction. [S3] — *unresolved tension in the current spec***
§7 wants a permanent public demo link; §9 wants ephemeral infrastructure to control cost. Resolve it explicitly: the **serving tier is small and always-on** (static frontend + minimal API over a compact aggregate table — a few dollars a month), while **Kafka, Spark, and K8s are ephemeral**. Which means the dashboard must degrade gracefully when the pipeline isn't running, and must say so.

**11.3 — A dashboard showing stale data with no indication. [S3]**
Nothing damages credibility faster than a recruiter opening your live demo and seeing three-week-old numbers presented as real-time. Prominent "data as of X, pipeline status Y." Honest staleness reads as engineering maturity; unlabeled staleness reads as a broken toy.

**11.4 — Unauthenticated public API. [S3]**
Cache aggressively, rate limit, and cap result sizes. Someone will loop over your endpoints.

---

## 12. The DE↔ML contract — where the expensive mistakes live

**12.1 — VACUUM. [S1]**
See 0.3. Listed twice on purpose.

**12.2 — `event_ts` is ambiguous and must be pinned. [S1]**
There are at least four candidate timestamps: feed header time, vehicle report time, your poll time, and the resolved arrival time. MLE#1's point-in-time join needs a specific one, and "when the thing happened" is not self-explanatory across those four. Define each in `docs/architecture.md` with a worked example, and name the column so the meaning is unambiguous (`arrival_event_ts`, `vehicle_report_ts`, `ingest_ts`).

**12.3 — Prediction leakage through the resolver. [S1] — *the one most likely to actually bite you***
If `actual_arrival_ts` is derived from the final TripUpdate prediction (resolver C), and MLE#1 also uses TripUpdate predictions as features, the model can learn to copy the agency's prediction and score beautifully while having learned nothing. You'd get a suspiciously good MAE and a completely hollow result. Two defenses: (a) the agency's own prediction is a **baseline to beat**, not a feature, unless you're deliberately building a prediction-correction model — in which case say so loudly; (b) prefer position-derived labels for training rows so the label and features have independent provenance. This one is subtle, project-specific, and the sort of thing that gets caught in an interview if you haven't caught it yourself.

**12.4 — Windowed features that include the prediction point. [S1]**
"Upstream delay on the same trip" is causally safe. "Average delay on this route today" includes stops that happen after the prediction. Every aggregate feature needs a strictly-before window with an explicit cutoff, and the cutoff must be `arrival_event_ts`, not a date.

**12.5 — Train/serve skew from batch-only features. [S1]**
A dbt-computed rolling aggregate exists in gold with hours of latency. At serving time it doesn't exist yet. Any feature MLE#1 uses must be computable online with the same definition, or the online path must read exactly the same table with documented staleness. Decide per feature; §5 already flags this, and it's the one that turns a good offline model into a bad service.

**12.6 — Schedule changes as distribution shift. [S3]**
Service changes 3–4× a year invalidate learned route-level patterns. Your SCD2 version changes are a free, precise retraining trigger — wire them up and it's a genuinely differentiated MLOps story.

**12.7 — Freezing the gold schema too late. [S3]**
§7 puts the freeze in Week 8. Anything MLE#1 builds before that is built on sand. Respect the ordering.

---

## 13. Process

**13.1 — ADRs instead of a working slice. [S3]**
The spec's own biggest risk. Ten ADRs and no end-to-end pipeline is worth less than an ugly working slice and two ADRs. Week 1 exists for a reason.

**13.2 — Multi-agency before single-agency works. [S3]**
Each additional agency multiplies resolver edge cases. One agency, fully correct, then scale.

**13.3 — No cost alarm. [S3]**
Set a billing alert at a threshold that would annoy you, on day one.

---

## Invariant harness — catches most S1s automatically

Rather than remembering 60 pitfalls, encode the invariants. These belong in dbt tests or a Dagster asset check and cover a large share of the list above:

1. **Grain uniqueness:** `(service_date, trip_id, stop_sequence)` is unique in gold. *(6.7, 7.6, 8.5)*
2. **Join conservation:** fact row count is identical before and after the as-of dimension join. *(6.7)*
3. **Idempotence:** reprocessing the same bronze window twice produces a byte-identical gold partition. *(3.1, 3.2, 4.5)*
4. **Referential integrity:** every gold `trip_id` resolves to exactly one dimension version; orphan count is tracked and alerted on trend. *(1.5, 1.6, 1.7)*
5. **Timezone anchors:** unit tests pinning both DST transition dates and at least one past-midnight (`24:00:00`+) scheduled time. *(6.5, 6.6, 8.4)*
6. **Coverage:** per-agency-per-hour observed-stop counts, with alerts on drops. *(1.4)*
7. **Watermark liveness:** watermark advancement monitored as a metric, not inferred from stream health. *(4.2)*
8. **Delay distribution stability:** alert on distribution shift — catches 2.1's fake on-time spike, resolver changes, and composition changes all at once. *(2.1, 7.1, 7.2)*
9. **Null-zero discrimination:** assert that no delay column contains `0` where the source field was absent. *(2.1)*
10. **Non-empty tests:** every test suite includes a row-count floor. *(8.3)*

If you build 1, 2, 3, 5, and 9 in Week 2, the register mostly enforces itself.
