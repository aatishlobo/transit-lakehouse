# AI Usage

This project involves AI in two distinct ways, documented separately because
they carry different obligations:

- **Part A — the bounded AI element**: a supervised model that corrects transit
  arrival predictions. This is the graded AI component.
- **Part B — AI-assisted development**: an AI coding assistant was used
  throughout, and is disclosed here in full.

---

# Part A — The bounded AI element

## A1. What the AI owns

**Task.** Given a transit agency's own arrival estimate plus context available at
the moment it was issued, predict how wrong that estimate will turn out to be.

```
target  =  actual_arrival − agency_predicted_arrival        (seconds)
```

A positive target means the vehicle arrived *later* than the agency said. The
model output is applied as a correction:

```
corrected_ETA  =  agency_ETA  +  predicted_correction
```

**Scope boundary.** The model does not predict arrival times from scratch, does
not schedule, and does not decide anything. It adjusts one number. Predicting
nothing (correction = 0) is exactly the "trust the agency" baseline, so the model
is measured against doing nothing on identical terms.

**Why this task.** Measurement of the live feed showed agency predictions are
systematically *optimistic*, and that the optimism grows with horizon — from −11 s
at two minutes out to −142 s at twenty-plus minutes. Systematic structure is
learnable; random noise is not. The model exists because the bias was measured
first.

**Model.** `HistGradientBoostingRegressor` (scikit-learn), 300 iterations, depth
6. Deliberately modest: ~1.3 M rows and seven tabular features is exactly where
gradient boosting is the right tool, and where a neural network would be an
unjustifiable choice that could not be defended.

**Files.**

| File | Role |
|---|---|
| `ml/build_features.py` | builds the training table from the archive |
| `ml/train.py` | trains, evaluates against three baselines, writes the report |
| `ml/predict.py` | inference, with fallback enforced in code |
| `ml/artifacts/prediction_correction_model.joblib` | the model artifact |
| `ml/artifacts/training_report.json` | full metrics |
| `ml/data/features_sample.csv.gz` | committed training data (168 k rows) |
| `tests/test_ml_leakage.py` | 13 tests covering the leakage defences |

## A2. Representative input and output

**Input** — one prediction, as it would arrive live:

```json
{
  "horizon_s":          600,      // agency says "10 minutes"
  "hour":               17,       // 5pm, local
  "stop_sequence_f":    20,       // 20th stop of the trip
  "agency_delay_s":     140,      // agency's own delay estimate here
  "direction_id":       0,
  "n_stops_in_update":  30,
  "route_code":         1         // SF:1
}
```

**Output:**

```json
{
  "agency_eta_epoch":     1786053600,
  "correction_s":         +90,
  "corrected_eta_epoch":  1786053690,
  "applied":              true
}
```

Read as: *the agency says 10 minutes; the model expects 11.5.*

**Training data.** 1,345,275 (prediction, actual) pairs for SF Muni, drawn from
78,593 derived arrivals over six days (2026-08-05 to 2026-08-10). Labels come
from vehicle positions; features come from trip updates — see A4.

## A3. Results, and what they are compared against

Reporting a model's error alone is meaningless. Three baselines, each fitted on
**training data only** and applied unchanged to the held-out test period:

| | MAE | RMSE | bias | p90 | within 60 s |
|---|---|---|---|---|---|
| `baseline_agency` — trust the ETA | 195.2 s | 381.9 | −154.0 | 406.0 | 31.0% |
| `baseline_global_bias` — add the mean | 215.1 s | 358.2 | +78.5 | 347.5 | 14.8% |
| `baseline_horizon_bias` — add the mean per horizon bucket | 206.7 s | 359.2 | +74.8 | 380.4 | 22.4% |
| **model** | **166.6 s** | **305.6** | **+43.7** | **341.0** | **32.1%** |

**14.7% lower error than the agency's own prediction**, and it beats all three
baselines. Trained on 1,008,746 rows, tested on 336,529 held strictly forward in
time.

Per horizon:

| horizon | n | agency MAE | model MAE | improvement |
|---|---|---|---|---|
| 0–2 min | 16,221 | 92.0 s | 75.4 s | 18.0% |
| 2–5 min | 25,101 | 103.1 s | 87.3 s | 15.3% |
| 5–10 min | 39,357 | 135.4 s | 108.4 s | 19.9% |
| 10–20 min | 69,910 | 174.6 s | 140.2 s | 19.7% |
| 20+ min | 185,940 | 237.0 s | 207.4 s | 12.5% |

**A result worth noticing:** both naive bias corrections are *worse* than simply
trusting the agency. Residuals are heavily right-skewed (mean +213 s, median
+111 s), so adding the mean overcorrects the typical case. The obvious
application of our own earlier finding would have made predictions worse. The
model earns its place by learning a horizon-, hour- and route-dependent
correction rather than a constant.

## A4. What was accepted, and what was rejected

**Rejected: deriving labels from predictions (resolver C).**
GTFS-Realtime offers a tempting shortcut — take the last prediction before a
vehicle passes and call it the actual arrival. Had we done that, this model would
be trained to predict agency predictions *from* agency predictions. It would post
an excellent MAE that means nothing. Labels instead come from vehicle position
data (`current_status == STOPPED_AT`), a **different feed**. Features cannot
contain the label, structurally rather than by vigilance.

**Accepted, with explicit declaration: using the agency prediction as a feature.**
This is normally leakage. It is legitimate here only because this is deliberately
a *prediction-correction* model and the labels have independent provenance. The
project's own design notes require that such a model "say so loudly" — hence this
section.

**Rejected: `lead_time` as a feature.**
`lead_time = actual_arrival − issued` is used in evaluation and would be a
natural copy-paste into the feature list. It contains the target. It would have
produced a spectacular score and a model that cannot run in production, because
lead time is unknowable until the vehicle has already arrived. Excluded, and
asserted by `test_lead_time_is_not_a_feature`.

**Rejected: a random train/test split.**
Random splitting on this data leaks twice over — the same trip contributes ~10
rows which would scatter across both sides, and later observations would train a
model tested on earlier ones. Both inflate the score invisibly. The split is a
strict time cut.

**Rejected: day-of-week, despite it improving the score.**
`dow` reduced MAE by 10 seconds (156.8 s vs 166.6 s). It was removed anyway.
With a six-day archive, day-of-week is very nearly a unique identifier per
calendar day, and `dow = 0` (Monday) occurs **only** in the test window — 31.2%
of test rows carried a value the model had never seen. Those rows fall below
every training bin boundary and silently inherit Wednesday's correction. The gain
was a coincidence of bin placement, not learned structure. *A number that cannot
be accounted for is not a result.* The reported 166.6 s is the honest figure.

This is now a general rule in `ml/train.py`: any low-cardinality feature whose
test values are absent from training is dropped, with the reason logged.

**Accepted: automatic removal of degenerate features.**
Three features were dropped automatically as constant or entirely absent, and the
reasons are themselves findings — `uncertainty` is 100% absent because SF Muni
never publishes it, independently confirming the earlier profiling result that
prediction-settlement is unavailable for this operator.

## A5. How the result was verified

**Temporal holdout.** Train 2026-08-05 → 08-09 15:45; test 08-09 15:47 → 08-10
16:12. No training row postdates any test row.

**Beaten against three baselines**, not reported in isolation.

**13 leakage tests** (`tests/test_ml_leakage.py`), including a whitelist assertion
that every feature is knowable at issue time — so adding a feature requires
justifying it rather than assuming it.

**Reproducibility.** The committed 168 k-row sample reproduces the result:
168.5 s MAE versus 166.6 s on the full archive. Run `make train`.

**Independent label provenance,** verified in the acceptance harness: the
absent-vs-zero invariant that the labels depend on is checked against raw
protobuf across 408,149 rows with zero violations.

## A6. Known limitations

**Trained on afternoons and evenings.** 95.8% of training rows fall in hours
14–19, because the machine running the archiver sleeps overnight. This is
effectively an afternoon/evening model and should not be trusted for morning
predictions. A collection artifact, not a property of the method.

**One operator.** SF Muni only. Nothing has been shown to transfer.

**Six days.** No seasonal, holiday, or weather variation. Day-of-week is
unsupported, which is why that feature was removed.

**The labels themselves carry measurement bias.** Derived arrivals are biased
late (a vehicle is only observable as `STOPPED_AT` after it arrives) and
over-represent long-dwell stops such as terminals and layovers, since capture
probability scales with dwell time. The model therefore learns to predict a
slightly late, non-representative target. **The 14.7% improvement is measured
against this target, not against ground truth.**

**Coverage is roughly 21% of stop events**, so the training set is a sample of
convenience rather than a census.

**No confidence intervals.** The model emits a point estimate. A production
version should emit a range.

## A7. Fallback

The fallback is **enforced in code, not described in a document**
(`ml/predict.py`). The agency's unmodified prediction is served whenever:

1. no model artifact is present;
2. the model failed to beat its best baseline at training time — checked against
   `model_beats_best_baseline` in the training report on every load;
3. the input falls outside the trained range (horizon < 1 s or > 3,600 s).

The baseline is the production default; the model is an override that must earn
its place on **every** retrain. A correction model that silently degrades an ETA
is worse than no model at all, because riders would trust a number we made worse.
Three tests cover these paths.

---

# Part B — AI-assisted development

## B1. Disclosure

**Tool.** Claude (Anthropic), used through Claude Code, over six working
sessions, 2026-08-05 to 2026-08-11.

**Scope.** The assistant contributed to most of the codebase: the streaming
layer, the arrival resolver, the aggregator, the ML pipeline, the test suite, the
acceptance harness, and the documentation — including drafting this file. It also
performed analysis, ran the pipeline, and diagnosed failures.

This is a substantial contribution and is stated plainly rather than minimised.

## B2. What the student owns

Per the course requirement that AI "may not replace your ownership of the design,
code, testing, or explanation":

- **Problem and scope** — student's, from the project proposal.
- **Architecture** — student's, defined before implementation began.
- **Every design decision** — reviewed and accepted or rejected by the student;
  the significant ones are recorded with their rationale in `CLAUDE.md`,
  `docs/PITFALLS.md`, and `docs/reports/`.
- **Explanation** — the student can explain the full path: absent-vs-zero, the
  grain choice, the partition-key decision, the resolver ladder, and the leakage
  defences in Part A.

`docs/reports/` contains a day-by-day record of what was built, what broke, and
why each decision was taken.

## B3. How AI output was verified

Not by inspection alone. Four mechanisms:

**Tests.** 83 unit tests, all written to assert project invariants rather than
implementation details.

**Acceptance checks against real data.** Six checks in `evaluation/`, each
carrying a row-count floor so it reports `UNMEASURED` rather than `PASS` when
handed too little data to be meaningful. The strongest re-parses raw protobuf and
compares field *presence* against decoded output across 408,149 rows — verifying
the founding invariant against ground truth rather than trusting the decoder's
claim about itself.

**Running it against live data.** Every substantive claim in this project was
checked against real feed output, not accepted from a model.

**Cross-checking analyses.** Several AI-produced statements were wrong and were
caught this way — see B4.

## B4. AI errors that were caught

Recorded because they are the most honest evidence of the verification process
working.

**A mislabelled partition column.** AI-written code named a directory
`service_dt=` while filling it with the UTC poll date. Every poll between 5pm and
midnight was filed under the following day — seven hours daily, including all of
PM peak. Caught within minutes of live data arriving, by comparing the directory
name against the `start_date` values actually inside the files. The fix was not
"use the right date" but a realisation the AI's framing had missed: a payload
spans multiple service dates, so the raw tier cannot be partitioned by service
date at all.

**A wrong statistical claim.** The AI described the poll interval as "±60 s of
noise" in generated output. Measuring dwell times disproved it: 90.3% of stop
events appear in exactly one poll, so the real effects are a *coverage* limit, a
*one-sided late* bias, and a *selection* bias toward long-dwell stops. Three
different things, only one of which the original phrasing described, and the
corrected version materially weakens the headline claim.

**A silent duplicate-counting bug.** Replaying data twice doubled every sample
count while leaving every average, median and percentile identical — output that
looked entirely correct while `n` lied. Caught by tearing down Kafka and
re-running from scratch, then noticing counts had halved.

**A leaked API key.** AI-written logging passed a `requests` exception straight
to the log; the exception text embeds the request URL, including `api_key=`. The
log was staged for a public push and caught by a pre-push secret scan.

**A misleading green check.** An acceptance check reported `PASS` while asserting
nothing, because the sample contained no instances of the case it tested. Changed
to report `INFO` and excluded from the pass count.

**A feature that improved the score for the wrong reason.** Described in A4.

## B5. Limitations of this approach

AI assistance accelerated implementation substantially, but every error in B4
shared a property: **the output looked correct.** None crashed, none warned, and
several produced better-looking numbers than the correct version. Reviewing
AI-generated code by reading it would have caught approximately none of them.
What caught them was running the code against real data and checking the output
against independent measurement.

The practical conclusion, and the reason this project invested so heavily in
tests and acceptance checks: for data work, AI output must be verified by
measurement rather than by inspection.

## B6. Preparation of this document

This file was drafted with AI assistance and reviewed, edited, and verified by
the student. Every figure quoted in Part A is reproducible from
`ml/artifacts/training_report.json` and `make train`.
