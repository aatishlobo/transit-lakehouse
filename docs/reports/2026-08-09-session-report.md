# Session Report — 9 August 2026

**Project:** transit-lakehouse (DE#1) · **Session:** Sunday afternoon

Saturday derived 6,046 arrivals and left them sitting in a queue. Today turned
them into an answer, and built the evidence that the answer can be trusted.

As before: no prior knowledge assumed, terms defined on first use, glossary at
the end.

---

## 1. What was missing

By Saturday night the pipeline ran end to end — archive to Kafka to derived
arrivals. But "6,046 arrivals exist" is not a result. Nobody asked that
question.

Three gaps, all required by your course:

| Gap | Why it matters |
|---|---|
| No **useful output** | The course requires a real artifact — a metric, report, or dataset |
| No **tests** for Saturday's code | "Repeatable tests" is a graded requirement |
| No **evaluation** artifact | The course requires validation evidence |

All three are now closed.

---

## 2. The useful output: how wrong are transit predictions?

### The question

When your app says the bus arrives in 7 minutes, how wrong is that?

Nobody publishes the answer, because answering it requires knowing when the bus
*actually* arrived — which, as established on day one, no agency states. But we
derive that ourselves now. So we can grade the agency's homework.

### Why the answer means anything: independent sources

This is the most important design point in today's work.

The two halves of the comparison come from **completely separate feeds**:

- the **prediction** comes from TripUpdates — the agency telling us what it
  expects;
- the **actual** comes from VehiclePositions — us watching the vehicle report
  `STOPPED_AT`.

They share no data and no logic.

Why that matters: there is a third way to derive arrivals, called *prediction
settlement*, which takes the agency's last prediction before the bus passed and
treats it as truth. Had we used that, this entire comparison would be measuring
the agency's predictions **against its own predictions**. The result would be a
beautifully small error that means absolutely nothing.

This is the leakage trap documented in the project notes as §7.3, and it is
avoided here **by construction, not by vigilance** — the labels come from
position data, the predictions from a different feed, and the two never touch.

### How the measurement works

For every arrival we derived, find all predictions issued for that stop
*before* the vehicle got there:

```
error_s     = predicted_arrival − actual_arrival
              negative → the agency predicted EARLIER than reality
lead_time_s = actual_arrival − prediction_issued
              how far in advance the prediction was made
```

Errors are grouped by lead time, because a 30-second-out estimate and a
20-minute-out estimate are different products. Averaging them together hides
exactly what a rider cares about.

**One exclusion worth noting.** A "prediction" issued *after* the vehicle
already arrived isn't a prediction — it's a correction. Scoring it would count
hindsight as foresight and flatter the agency. 40 such pairs were excluded, and
the exclusion is counted rather than silent.

### What we report, and why two statistics

Each group gets **median absolute error** and **mean signed error**. The second
is the important one — it's the **bias**.

The distinction: random error averages out. If a bus is sometimes 60 seconds
early and sometimes 60 seconds late, the route works fine overall. But a
*systematic* 60-second optimism never averages out. It makes riders miss buses,
every single time, in the same direction.

---

## 3. The result

SF Muni, 39-minute sample, 25,501 (prediction, arrival) pairs:

```
   lead time       n     bias  med|err|     p90    <60s   <180s
     0-2 min     509      -11        13      52   92.7%  100.0%
     2-5 min    4603      -46        50     112   59.5%   98.4%
    5-10 min    4897      -66        70     172   43.3%   91.2%
   10-20 min    8919      -98       102     257   31.1%   77.0%
     20+ min    6573     -142       146     375   23.0%   59.3%
```

Reading this:

**Short-horizon predictions are excellent.** Under two minutes out, the median
error is 13 seconds and 92.7% land within a minute. When the bus is nearly
there, the agency knows where it is.

**Accuracy degrades steadily with horizon.** By 20+ minutes out, only 23% are
within a minute and the median error is nearly 2.5 minutes.

**The bias is negative at every horizon, and grows.** Negative means the agency
predicts *earlier* than we observe — vehicles arrive later than promised. At 20+
minutes the average optimism is 142 seconds.

That last finding is the interesting one, and it comes with a serious caveat.

Four files were written to `outputs/`: error by lead time, by route, by
route-and-hour (395 rows), and a summary JSON carrying the method description
and all caveats.

---

## 4. Correcting my own analysis

Earlier in the session the summary output described the poll interval as
"±60 seconds of noise." **That was wrong**, and the correction is the most
interesting part of today.

It conflates three different effects. Measuring how long vehicles actually sit
at stops settled it:

```
consecutive polls showing STOPPED_AT at the same stop (SF Muni):
  seen in 1 poll:  2788  (90.3%)
  seen in 2 polls:  105  ( 3.4%)
  seen in 3+ polls: 202  ( 6.3%)
```

**90.3% of stop events appear in exactly one poll.** So typical dwell time is
much shorter than our 120-second sampling interval. The three effects:

**1. Coverage, not precision.** Because dwell is short and sampling is slow, we
*miss* most stop events entirely — roughly 21% get captured. That's a
sample-size problem, not an accuracy problem. Being clear about which is which
matters.

**2. A one-sided offset.** A vehicle is only visible as `STOPPED_AT` between
arriving and departing. So a detected timestamp always falls at or *after* the
true arrival — never before. Our derived arrivals are biased **late**, by an
amount bounded by dwell time. Not symmetric noise: a systematic push in one
direction.

**3. Selection bias — the one that actually threatens the conclusion.** The
chance of catching a stop is proportional to how long the vehicle sits there. So
we preferentially observe **long-dwell stops**: terminals, layovers, and
timepoints where vehicles wait *on purpose*. Median observed dwell among
multi-poll sightings is 353 seconds — nearly six minutes. Those stops are not
representative, and they're precisely where schedule-holding distorts what
"arrival" even means.

**Consequence:** effects 2 and 3 both push our measured "actual" later, which
makes the agency look more optimistic than it may be. **The negative bias should
be read as an upper bound on agency optimism, not a point estimate.** That
sentence is now in the code, the summary JSON, and the console output.

This is also the strongest argument for the 511 rate-limit increase: faster
polling attacks all three effects simultaneously.

The general lesson: *"±60s of noise" and "systematically biased late with a
selection effect" are completely different claims.* Only one of them is true,
and only one would have survived a question in a review.

---

## 5. A bug found by tearing everything down

To verify the reviewer's experience, Kafka was destroyed and the whole pipeline
re-run from scratch. It worked — and every count came out **exactly half** the
previous run, while every bias, median and percentile was **identical**.

The cause: Kafka still held data from an earlier run. Each arrival was pairing
with *two copies* of every prediction.

**Why this is the dangerous kind of bug.** Duplicating every measurement changes
no average, no median, no percentile — the distribution is identical. Only `n`
doubles. So the output looked completely correct while the sample size silently
lied. Nothing would ever have flagged it.

**The fix** was to store predictions in a *set* rather than a *list*. The same
(stop, issue time, predicted time) tuple is the same observation, so collapsing
duplicates is semantically correct — and it makes the aggregate idempotent, the
same guarantee the arrival resolver already had.

Verified by deliberately replaying twice: counts now stay identical. Three
regression tests cover it.

---

## 6. Tests

35 new tests (66 total, all passing).

**Resolver (16 tests).** The headline is
`test_arrival_is_first_sighting_not_last`. A vehicle reports `STOPPED_AT` on
every poll while it waits; taking the last sighting would silently measure
*departure* instead of arrival. Nothing would error — every timestamp would just
be wrong, by the dwell time, in one direction. Also covered: loop routes
resolving separately, trips on different service dates staying independent,
event time coming from the vehicle, and every skip reason being counted.

**Aggregator (19 tests).** Covering the sign convention, the
hindsight-exclusion rule, bias being a *signed* mean rather than an absolute
one, lead-time bucket boundaries, and the duplicate-collapse fix.

One test deserves mention: `test_hour_is_local_not_utc`. Converting to local
time exactly once, at the very end, is a rule that's easy to violate. Get it
wrong and every bar of an hourly chart shifts 7–8 hours while every total stays
correct — a wrong answer that looks entirely plausible.

---

## 7. The acceptance harness

`evaluation/run_acceptance_checks.py` — the validation artifact your course
requires.

**These are not unit tests.** Unit tests check that a function behaves on inputs
the author imagined. Acceptance checks verify that properties hold across the
*actual data the pipeline produced*, which is where the interesting failures
live.

### Every check has a row-count floor

A check that passes on an empty table is worse than no check: it reports green
while measuring nothing, and keeps reporting green after an upstream break
empties the input. So each check **fails if handed too little data**, and
reports that distinctly from a real failure.

### The checks

| Check | What it proves | Result |
|---|---|---|
| `null_zero_discrimination` | Missing data never became a zero | **408,149 rows, 0 violations** |
| `contract_validation` | Every record satisfies the contract | 245,701 valid, 0 invalid |
| `grain_uniqueness` | No arrival counted twice | 6,046 arrivals, 6,046 distinct keys |
| `idempotent_resolution` | Re-running gives identical output | run1 = run2, identical |
| `provenance_complete` | Every arrival records how it was derived | 0 missing fields |
| `event_time_not_ingest_time` | Timestamps come from vehicles, not our clock | 0 collisions |

**The first is the strongest artifact in the project.** It re-parses the raw
protobuf and compares field *presence* directly against the decoded output —
rather than trusting the decoder's own claim about itself. Across 408,149 rows:
199,675 genuinely absent, 10,693 explicit zeros, 197,781 real values, **zero
violations**. That is independent proof of the invariant the whole project rests
on.

### One check reports INFO, not PASS

`loop_route_revisits_observed` found zero loop revisits in the 39-minute
sample. With ~21% stop capture, catching *both* visits of a loop in 39 minutes
is rare — so the check asserts nothing here.

Reporting it green would inflate the tally with a check that measured nothing.
It's now marked informational and excluded from the pass count, with the
behaviour asserted directly in the resolver tests instead. **A check that
asserts nothing must not contribute to a green result.**

---

## 8. One command

```bash
make demo
```

Starts Kafka, creates topics, replays the committed sample, derives arrivals,
aggregates the metrics, runs the acceptance checks. **Cold start to passing
checks: 2 minutes 16 seconds.** No API key, no network, no cloud account.

That is your reviewer's entire experience.

---

## 9. Design decisions, collected

| Decision | Alternative rejected | Why |
|---|---|---|
| Compare predictions against position-derived arrivals | Use prediction-settlement arrivals | Would measure the agency against itself — a great number that means nothing |
| Group errors by lead time | One overall average | A 30-second and a 20-minute estimate are different products |
| Report signed bias *and* absolute error | Absolute error alone | Random error averages out; systematic error doesn't |
| Exclude predictions issued after arrival | Score everything | Counts hindsight as foresight |
| Dedupe predictions with a set | List append | Duplicates inflate n while leaving every statistic identical |
| Row-count floors on every check | Plain assertions | A check passing on empty data is worse than no check |
| INFO status for non-asserting checks | Report as PASS | A check that measures nothing must not count as green |
| Convert to local time once, at the end | Truncate hours in UTC | Shifts every hourly bar 7–8 hours while totals stay right |

---

## 10. Where things stand

**Complete:** the full course-required path, from data source through validated
contract, producer, Kafka, consumer, useful output, and validation evidence —
runnable with one command from committed data. 66 tests. 6 acceptance checks
passing plus 1 informational.

**Remaining:**

| Item | When |
|---|---|
| Bounded AI element (delay predictor) | Mon–Tue |
| `DATA_SOURCE.md` | Wed |
| `AI_USAGE.md` | Wed |
| README rewrite mapping the submission structure | Wed |
| `report.pdf` | Thu |
| Presentation | Fri |

Optional if time allows: GTFS-Static schedule data, which would let us compute
true on-time performance (actual vs *scheduled*) rather than prediction accuracy
(actual vs *predicted*).

---

## Glossary (additions)

| Term | Meaning |
|---|---|
| **Lead time** | How far in advance a prediction was made |
| **Bias** | Mean *signed* error — the systematic push in one direction |
| **Absolute error** | Error magnitude ignoring direction |
| **p90** | The value 90% of cases fall below; captures the bad tail |
| **Dwell time** | How long a vehicle sits at a stop |
| **Selection bias** | When what you *can* observe differs systematically from the whole |
| **Leakage** | When a measurement secretly contains the answer, flattering the result |
| **Acceptance check** | A property verified against real produced data, not sample inputs |
| **Row-count floor** | A minimum data volume below which a check reports "unmeasured" |
| **Regression test** | A test written after a bug, to catch its return |
| **Idempotent** | Doing it twice has the same effect as doing it once |
