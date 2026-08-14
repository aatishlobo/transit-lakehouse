# AI Usage

AI appears here in two ways, documented separately because they carry different
obligations:

- **Part A, the bounded AI element:** a supervised model that corrects transit
  arrival predictions. This is the graded AI component.
- **Part B, AI-assisted development:** an AI coding assistant was used
  throughout, disclosed in full.

---

# Part A: the bounded AI element

## A1. What the AI owns

**Task.** Given an agency's own arrival estimate plus context available when it
was issued, predict how wrong that estimate will be.

```
target         =  actual_arrival - agency_predicted_arrival     (seconds)
corrected_ETA  =  agency_ETA + predicted_correction
```

A positive target means the vehicle arrived later than the agency said.

**Scope boundary.** The model does not predict arrivals from scratch, does not
schedule, and does not decide anything. It adjusts one number. Predicting nothing
(correction = 0) is exactly the "trust the agency" baseline, so the model is
measured against doing nothing on identical terms.

**Why this task.** Agency predictions were measured to be systematically
optimistic, with optimism growing by horizon, from -11 s at two minutes out to
-142 s at twenty-plus. Systematic structure is learnable; random noise is not.
The model exists because the bias was measured first.

**Model.** `HistGradientBoostingRegressor` (scikit-learn), 300 iterations, depth
6. Deliberately modest: 1.3 M rows and seven tabular features is where gradient
boosting is the right tool and a neural network would be indefensible.

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

```json
// input: one prediction, as it would arrive live
{
  "horizon_s":          600,      // agency says "10 minutes"
  "hour":               17,       // 5pm, local
  "stop_sequence_f":    20,       // 20th stop of the trip
  "agency_delay_s":     140,      // agency's own delay estimate here
  "direction_id":       0,
  "n_stops_in_update":  30,
  "route_code":         1         // SF:1
}

// output
{
  "agency_eta_epoch":     1786053600,
  "correction_s":         +90,
  "corrected_eta_epoch":  1786053690,
  "applied":              true
}
```

Read as: the agency says 10 minutes; the model expects 11.5.

**Training data.** 1,345,275 (prediction, actual) pairs for SF Muni, from 78,593
derived arrivals over six days (2026-08-05 to 2026-08-10). Labels come from
vehicle positions, features from trip updates. See A4.

## A3. Results, and what they are compared against

A model's error alone is meaningless. Three baselines, each fitted on **training
data only** and applied unchanged to the held-out test period:

| | MAE | RMSE | bias | p90 | within 60 s |
|---|---|---|---|---|---|
| `baseline_agency`, trust the ETA | 195.2 s | 381.9 | -154.0 | 406.0 | 31.0% |
| `baseline_global_bias`, add the mean | 215.1 s | 358.2 | +78.5 | 347.5 | 14.8% |
| `baseline_horizon_bias`, add the mean per bucket | 206.7 s | 359.2 | +74.8 | 380.4 | 22.4% |
| **model** | **166.6 s** | **305.6** | **+43.7** | **341.0** | **32.1%** |

**14.7% lower error than the agency's own prediction**, beating all three
baselines. Trained on 1,008,746 rows, tested on 336,529 held strictly forward in
time.

| horizon | n | agency MAE | model MAE | improvement |
|---|---|---|---|---|
| 0-2 min | 16,221 | 92.0 s | 75.4 s | 18.0% |
| 2-5 min | 25,101 | 103.1 s | 87.3 s | 15.3% |
| 5-10 min | 39,357 | 135.4 s | 108.4 s | 19.9% |
| 10-20 min | 69,910 | 174.6 s | 140.2 s | 19.7% |
| 20+ min | 185,940 | 237.0 s | 207.4 s | 12.5% |

**Both naive bias corrections are worse than trusting the agency.** Residuals are
right-skewed (mean +213 s, median +111 s), so adding the mean overcorrects the
typical case. The obvious application of our own earlier finding would have made
predictions worse. The model earns its place by learning a horizon-, hour-, and
route-dependent correction rather than a constant.

## A4. What was accepted, and what was rejected

**Rejected: deriving labels from predictions (resolver C).** The tempting
shortcut is to take the last prediction before a vehicle passes and call it the
actual arrival. That trains the model to predict agency predictions from agency
predictions, posting an excellent MAE that means nothing. Labels instead come
from vehicle positions (`current_status == STOPPED_AT`), a **different feed**, so
features cannot contain the label structurally rather than by vigilance.

**Accepted, with explicit declaration: the agency prediction as a feature.**
Normally leakage. Legitimate here only because this is deliberately a
prediction-correction model and the labels have independent provenance. The
project's design notes require such a model to say so loudly, hence this section.

**Rejected: `lead_time` as a feature.** `actual_arrival - issued` is used in
evaluation and would be a natural copy-paste into the feature list. It contains
the target and is unknowable until the vehicle has arrived, so it would produce a
spectacular score and a model that cannot run in production. Asserted by
`test_lead_time_is_not_a_feature`.

**Rejected: a random train/test split.** It leaks twice: one trip contributes
about 10 rows that scatter across both sides, and later observations would train
a model tested on earlier ones. The split is a strict time cut.

**Rejected: day-of-week, despite it improving the score.** `dow` cut MAE by 10
seconds (156.8 s versus 166.6 s) and was removed anyway. Over a six-day archive
it is nearly a unique identifier per calendar day, and `dow = 0` occurs **only**
in the test window: 31.2% of test rows carried a value the model had never seen.
Those rows fall below every training bin boundary and silently inherit
Wednesday's correction. The gain was bin placement, not learned structure. *A
number that cannot be accounted for is not a result.* The reported 166.6 s is the
honest figure. `ml/train.py` now drops any low-cardinality feature whose test
values are absent from training, logging the reason.

**Accepted: automatic removal of degenerate features.** Three were dropped as
constant or entirely absent, and the reasons are themselves findings.
`uncertainty` is 100% absent because SF Muni never publishes it, independently
confirming the profiling result that prediction settlement is unavailable here.

## A5. How the result was verified

- **Temporal holdout.** Train 2026-08-05 to 08-09 15:45; test 08-09 15:47 to
  08-10 16:12. No training row postdates any test row.
- **Compared against three baselines**, not reported in isolation. The full
  comparison is the optional extension, Section 10 of `report.pdf`.
- **13 leakage tests** (`tests/test_ml_leakage.py`), including a whitelist
  assertion that every feature is knowable at issue time, so adding one requires
  justification rather than assumption.
- **Reproducibility.** The committed 168 k-row sample reproduces the result at
  168.5 s MAE, against 166.6 s on the full archive. Run `make train`.
- **Independent label provenance,** verified in the acceptance harness: the
  absent-versus-zero invariant the labels depend on is checked against raw
  protobuf across 408,149 rows with zero violations.

## A6. Known limitations

- **Trained on afternoons and evenings.** 95.8% of training rows fall in hours
  14-19, because the machine running the archiver sleeps overnight. It should not
  be trusted for morning predictions. A collection artifact, not a property of
  the method.
- **One operator.** SF Muni only. Nothing has been shown to transfer.
- **Six days.** No seasonal, holiday, or weather variation, which is why
  day-of-week is unsupported.
- **The labels carry measurement bias.** Derived arrivals are biased late, since
  a vehicle is observable as `STOPPED_AT` only after arriving, and
  over-represent long-dwell stops, since capture probability scales with dwell.
  The model therefore learns a slightly late, non-representative target. **The
  14.7% improvement is measured against that target, not ground truth.**
- **Coverage is roughly 21% of stop events**, so the training set is a sample of
  convenience rather than a census.
- **No confidence intervals.** A production version should emit a range.

## A7. Fallback

Enforced in code, not described in a document (`ml/predict.py`). The unmodified
agency prediction is served whenever:

1. no model artifact is present;
2. the model failed to beat its best baseline at training time, checked against
   `model_beats_best_baseline` in the training report on every load;
3. the input falls outside the trained range (horizon < 1 s or > 3,600 s).

The baseline is the production default; the model is an override that must earn
its place on every retrain. A correction model that silently degrades an ETA is
worse than no model, because riders would trust a number we made worse. Three
tests cover these paths.

---

# Part B: AI-assisted development

## B1. Disclosure

**Tool.** Claude (Anthropic), through Claude Code, over six working sessions,
2026-08-05 to 2026-08-11.

**Scope.** The assistant contributed to most of the codebase: the streaming
layer, the arrival resolver, the aggregator, the ML pipeline, the test suite, the
acceptance harness, and the documentation, including drafting this file. It also
ran the pipeline, performed analysis, and diagnosed failures. This is a
substantial contribution, stated plainly rather than minimised.

## B2. What the student owns

Per the requirement that AI "may not replace your ownership of the design, code,
testing, or explanation":

- **Problem and scope:** the student's, from the proposal.
- **Architecture:** the student's, defined before implementation began.
- **Every design decision:** reviewed and accepted or rejected by the student,
  with rationale recorded in `CLAUDE.md`, `docs/PITFALLS.md`, and
  `docs/reports/`.
- **Explanation:** the student can explain the full path, including
  absent-versus-zero, the grain choice, the partition key, the resolver ladder,
  and the leakage defences in Part A.

## B3. How AI output was verified

Not by inspection alone. Four mechanisms:

- **Tests.** 83 unit tests, asserting project invariants rather than
  implementation details.
- **Acceptance checks against real data.** Six checks in `evaluation/`, each with
  a row-count floor so it reports `UNMEASURED` rather than `PASS` on thin data.
  The strongest re-parses raw protobuf and compares field presence against
  decoded output across 408,149 rows, verifying the founding invariant against
  ground truth rather than the decoder's claim about itself.
- **Running against live data.** Every substantive claim was checked against real
  feed output, not accepted from a model.
- **Cross-checking analyses.** Several AI statements were wrong and were caught
  this way. See B4.

## B4. AI errors that were caught

Recorded because they are the most honest evidence of verification working.

**A mislabelled partition column.** AI-written code named a directory
`service_dt=` while filling it with the UTC poll date, filing every poll between
5pm and midnight under the following day: seven hours daily, including all of PM
peak. Caught within minutes of live data arriving, by comparing the directory
name against the `start_date` values inside the files. The fix was not "use the
right date" but a realisation the AI's framing had missed: a payload spans
multiple service dates, so the raw tier cannot be partitioned by service date at
all.

**A wrong statistical claim.** The AI described the poll interval as "plus or
minus 60 s of noise". Measuring dwell disproved it: 90.3% of stop events appear
in exactly one poll, so the real effects are a coverage limit, a one-sided late
bias, and a selection bias toward long-dwell stops. Three different things, and
the corrected version materially weakens the headline claim.

**A silent duplicate-counting bug.** Replaying twice doubled every sample count
while leaving every average, median, and percentile identical: output that looked
correct while `n` lied. Caught by tearing down Kafka, re-running from scratch,
and noticing counts had halved.

**A leaked API key.** AI-written logging passed a `requests` exception straight
to the log, and the exception text embeds the request URL including `api_key=`.
The log was staged for a public push and caught by a pre-push secret scan.

**A misleading green check.** An acceptance check reported `PASS` while asserting
nothing, because the sample contained no instances of the case it tested. Changed
to `INFO` and excluded from the pass count.

**A feature that improved the score for the wrong reason.** Described in A4.

## B5. Limitations of this approach

AI assistance accelerated implementation substantially, but every error in B4
shared a property: **the output looked correct.** None crashed, none warned, and
several produced better-looking numbers than the correct version. Reading the
generated code would have caught approximately none of them. Running it against
real data and checking against independent measurement did.

The practical conclusion, and the reason this project invested heavily in tests
and acceptance checks: for data work, AI output must be verified by measurement
rather than by inspection.

## B6. Preparation of this document

Drafted with AI assistance, then reviewed, edited, and verified by the student.
Every figure in Part A is reproducible from `ml/artifacts/training_report.json`
and `make train`.
