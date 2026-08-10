"""Train and evaluate the prediction-correction model.

TASK
----
    residual = actual_arrival - predicted_arrival    (seconds; + = arrived late)

The model predicts that residual. Applying it to the agency's ETA gives a
corrected ETA. Doing nothing is the same as predicting residual = 0, which is
exactly the "trust the agency" baseline -- so the comparison is apples to
apples and the model has to genuinely earn its place.

THE SPLIT IS TEMPORAL, NOT RANDOM. THIS IS THE MOST IMPORTANT LINE HERE.
------------------------------------------------------------------------
A random train/test split on time-series data leaks in two ways at once:

  1. The SAME TRIP appears many times -- one row per prediction, ~10 per stop.
     A random split scatters those rows across train and test, so the model
     memorises specific trips and is then tested on the trips it memorised.
  2. Later observations land in training while earlier ones land in test, so
     the model effectively sees the future.

Both inflate the score enormously and neither is visible in the output: you get
a great MAE and a model that is worthless in production, because production
never gets to see tomorrow. The split here is a strict time cut -- everything
before the boundary trains, everything after tests -- which is the only split
that mirrors how the model would actually be used.

BASELINES -- THE MODEL MUST BEAT ALL OF THEM
--------------------------------------------
Reporting a model's MAE alone is meaningless without something to compare to.
Three, in increasing order of difficulty:

  * agency        -- residual = 0. Trust the ETA as published.
  * global_bias   -- residual = mean residual from TRAINING data only.
                     "Everything runs ~N seconds late, add N."
  * horizon_bias  -- residual = mean residual per horizon bucket, from training
                     only. This is yesterday's finding turned into a predictor,
                     and it is the honest bar: if the model cannot beat a
                     five-row lookup table, the model is not earning anything.

Every baseline is fitted on TRAINING data only and applied unchanged to test.
Fitting a baseline on the test set would leak and would understate the model's
advantage in a way that looks generous but is simply wrong.

MODEL
-----
HistGradientBoostingRegressor. Deliberately modest: tens of thousands of rows,
~9 features, mostly tabular and non-linear. Gradient boosting is the correct
tool; a neural network here would be an unjustifiable choice. It also handles
missing values natively, which matters because `agency_delay_s` is genuinely
absent much of the time and must not be imputed to 0 (pitfall 2.1).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

log = logging.getLogger("train")

# Features the model may see. Every one is knowable when the prediction was
# issued. `lead_time` is absent by design -- it contains the target.
FEATURES = [
    "horizon_s",
    "hour",
    "dow",
    "stop_sequence_f",
    "agency_delay_s",
    "agency_delay_known",
    "uncertainty",
    "uncertainty_known",
    "direction_id",
    "n_stops_in_update",
    "route_code",
]
TARGET = "residual_s"

HORIZON_BINS = [0, 120, 300, 600, 1200, 10**9]
HORIZON_LABELS = ["0-2m", "2-5m", "5-10m", "10-20m", "20m+"]


def metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    err = y_pred - y_true
    return {
        "mae_s": round(float(mean_absolute_error(y_true, y_pred)), 1),
        "rmse_s": round(float(np.sqrt(mean_squared_error(y_true, y_pred))), 1),
        "bias_s": round(float(np.mean(err)), 1),
        "p90_abs_s": round(float(np.percentile(np.abs(err), 90)), 1),
        "pct_within_60s": round(float(100 * np.mean(np.abs(err) <= 60)), 1),
        "pct_within_180s": round(float(100 * np.mean(np.abs(err) <= 180)), 1),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", default="ml/data/features.csv")
    ap.add_argument("--outdir", default="ml/artifacts")
    ap.add_argument("--test-frac", type=float, default=0.25,
                    help="fraction of the TIME RANGE held out at the end")
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(message)s")

    df = pd.read_csv(args.features)
    log.info("loaded %d rows", len(df))
    if len(df) < 1000:
        log.error("only %d rows -- too few to train or trust", len(df))
        return 1

    # Route as a numeric code. Tree models split on it fine, and it avoids a
    # one-hot explosion across ~55 routes.
    df["route_code"] = df["route_id"].astype("category").cat.codes

    # ---- temporal split -------------------------------------------------
    df = df.sort_values("issued_epoch").reset_index(drop=True)
    cut_idx = int(len(df) * (1 - args.test_frac))
    cut_epoch = int(df.loc[cut_idx, "issued_epoch"])
    # Cut on the TIMESTAMP, not the row index, so no single instant straddles
    # the boundary and no trip can appear on both sides at the same moment.
    train = df[df["issued_epoch"] < cut_epoch]
    test = df[df["issued_epoch"] >= cut_epoch]

    log.info(
        "temporal split at %s -- train=%d (%s -> %s)  test=%d (%s -> %s)",
        datetime.fromtimestamp(cut_epoch),
        len(train),
        datetime.fromtimestamp(train["issued_epoch"].min()),
        datetime.fromtimestamp(train["issued_epoch"].max()),
        len(test),
        datetime.fromtimestamp(test["issued_epoch"].min()),
        datetime.fromtimestamp(test["issued_epoch"].max()),
    )

    overlap = set(train["trip_id"]) & set(test["trip_id"])
    log.info("trip_ids in both splits: %d (expected: small, from trips "
             "spanning the cut)", len(overlap))

    # ---- drop degenerate features ---------------------------------------
    # A column that is entirely missing, or constant, carries no information.
    # Dropped automatically (rather than hardcoded) so this keeps working if
    # the data changes, and LOGGED, because which columns vanish is itself a
    # finding about the feed.
    #
    # Observed on SF Muni: `uncertainty` is 100% absent -- Muni never publishes
    # it, which independently confirms the Week-0 profile that resolver C is
    # unavailable for this operator. `agency_delay_known` is constant 1 for the
    # same reason its counterpart is constant 0: within the joined subset Muni
    # always publishes a delay.
    #
    # A third rule, and the subtle one: drop any LOW-CARDINALITY feature whose
    # test values are not present in training. The model cannot have learned
    # anything about an unseen category, so whatever it does with those rows is
    # an artefact of bin placement rather than structure.
    #
    # This removed `dow`. With a six-day archive, day-of-week is very nearly a
    # unique identifier per calendar day, and dow=0 (Monday) occurs ONLY in the
    # test window -- the model was being scored on a value it had never seen.
    # Keeping it measurably IMPROVED test MAE (156.8s vs 166.6s), which is
    # precisely why it had to go: unseen Monday rows fall below every training
    # bin boundary and inherit Wednesday's correction, so the gain is a
    # coincidence of binning, not something we could explain or expect to hold.
    # A number you cannot account for is not a result.
    usable, dropped = [], {}
    for f in FEATURES:
        col = train[f]
        if col.isna().all():
            dropped[f] = "entirely absent in training data"
        elif col.nunique(dropna=True) < 2:
            dropped[f] = f"constant (value={col.dropna().iloc[0] if len(col.dropna()) else None})"
        elif col.nunique(dropna=True) <= 12:
            unseen = set(test[f].dropna().unique()) - set(col.dropna().unique())
            if unseen:
                n_unseen = int(test[f].isin(unseen).sum())
                dropped[f] = (
                    f"test contains values never seen in training "
                    f"({sorted(unseen)}; {n_unseen} rows, "
                    f"{100 * n_unseen / len(test):.1f}% of test) -- the model "
                    f"cannot have learned them"
                )
            else:
                usable.append(f)
        else:
            usable.append(f)
    for f, why in dropped.items():
        log.warning("dropping feature %s -- %s", f, why)
    if not usable:
        log.error("no usable features remain")
        return 1
    log.info("using %d features: %s", len(usable), usable)

    X_tr, y_tr = train[usable], train[TARGET].to_numpy()
    X_te, y_te = test[usable], test[TARGET].to_numpy()

    # ---- baselines, all fitted on TRAIN only ----------------------------
    results = {}
    results["baseline_agency"] = metrics(y_te, np.zeros(len(y_te)))

    global_bias = float(np.mean(y_tr))
    results["baseline_global_bias"] = metrics(y_te, np.full(len(y_te), global_bias))

    tr_bucket = pd.cut(train["horizon_s"], HORIZON_BINS, labels=HORIZON_LABELS,
                       right=False)
    te_bucket = pd.cut(test["horizon_s"], HORIZON_BINS, labels=HORIZON_LABELS,
                       right=False)
    bucket_bias = train.groupby(tr_bucket, observed=False)[TARGET].mean()
    pred_bucket = te_bucket.map(bucket_bias).astype(float).fillna(global_bias)
    results["baseline_horizon_bias"] = metrics(y_te, pred_bucket.to_numpy())

    # ---- model ----------------------------------------------------------
    model = HistGradientBoostingRegressor(
        max_iter=300,
        learning_rate=0.08,
        max_depth=6,
        min_samples_leaf=50,
        l2_regularization=1.0,
        early_stopping=True,
        validation_fraction=0.15,
        random_state=args.seed,
    )
    model.fit(X_tr, y_tr)
    y_hat = model.predict(X_te)
    results["model"] = metrics(y_te, y_hat)
    log.info("trained %d iterations", model.n_iter_)

    # ---- per-horizon breakdown -----------------------------------------
    per_horizon = []
    for label in HORIZON_LABELS:
        m = (te_bucket == label).to_numpy()
        if m.sum() < 50:
            continue
        per_horizon.append({
            "horizon": label,
            "n": int(m.sum()),
            "agency_mae_s": metrics(y_te[m], np.zeros(int(m.sum())))["mae_s"],
            "model_mae_s": metrics(y_te[m], y_hat[m])["mae_s"],
        })
        last = per_horizon[-1]
        last["improvement_pct"] = round(
            100 * (last["agency_mae_s"] - last["model_mae_s"])
            / last["agency_mae_s"], 1
        ) if last["agency_mae_s"] else 0.0

    best_baseline = min(
        ("baseline_agency", "baseline_global_bias", "baseline_horizon_bias"),
        key=lambda k: results[k]["mae_s"],
    )
    beats = results["model"]["mae_s"] < results[best_baseline]["mae_s"]

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    import joblib
    joblib.dump({"model": model, "features": usable,
                 "route_categories": list(df["route_id"].astype("category").cat.categories)},
                outdir / "prediction_correction_model.joblib")

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task": "predict residual = actual_arrival - agency_predicted_arrival",
        "model_type": "HistGradientBoostingRegressor",
        "split": {
            "strategy": "temporal (strict time cut, no shuffling)",
            "why": ("A random split would scatter rows from the same trip "
                    "across train and test and let the model see the future. "
                    "Both inflate the score invisibly."),
            "cut_at": datetime.fromtimestamp(cut_epoch).isoformat(),
            "train_rows": int(len(train)),
            "test_rows": int(len(test)),
            "trip_ids_in_both": len(overlap),
        },
        "features_declared": FEATURES,
        "features_used": usable,
        "features_dropped": dropped,
        "features_excluded_for_leakage": {
            "lead_time_s": "= actual_arrival - issued; contains the target",
            "actual_epoch": "the target itself",
        },
        "label_provenance": "VehiclePositions STOPPED_AT (resolver A)",
        "feature_provenance": "TripUpdates (agency forecast)",
        "results": results,
        "per_horizon": per_horizon,
        "best_baseline": best_baseline,
        "model_beats_best_baseline": bool(beats),
        "fallback_policy": (
            "If the model does not beat the best baseline on a future "
            "evaluation, serve the agency prediction unchanged. The baseline "
            "is the production default; the model is an override that must "
            "earn its place on every retrain."
        ),
    }
    (outdir / "training_report.json").write_text(json.dumps(report, indent=2))

    # ---- console summary ------------------------------------------------
    print("\n" + "=" * 76)
    print("PREDICTION-CORRECTION MODEL  --  temporal holdout")
    print("=" * 76)
    print(f"train {len(train):>7,} rows   test {len(test):>7,} rows   "
          f"cut at {datetime.fromtimestamp(cut_epoch):%m-%d %H:%M}")
    print(f"\n{'':22s} {'MAE':>8s} {'RMSE':>8s} {'bias':>8s} {'p90':>8s} "
          f"{'<60s':>7s} {'<180s':>7s}")
    for name in ("baseline_agency", "baseline_global_bias",
                 "baseline_horizon_bias", "model"):
        r = results[name]
        print(f"{name:22s} {r['mae_s']:>8.1f} {r['rmse_s']:>8.1f} "
              f"{r['bias_s']:>+8.1f} {r['p90_abs_s']:>8.1f} "
              f"{r['pct_within_60s']:>6.1f}% {r['pct_within_180s']:>6.1f}%")

    a = results["baseline_agency"]["mae_s"]
    m = results["model"]["mae_s"]
    print(f"\nvs raw agency ETA: {a:.1f}s -> {m:.1f}s MAE "
          f"({100 * (a - m) / a:+.1f}%)")
    print(f"best baseline: {best_baseline} "
          f"({results[best_baseline]['mae_s']:.1f}s) -- "
          f"model {'BEATS' if beats else 'DOES NOT BEAT'} it")

    if per_horizon:
        print(f"\n{'horizon':>9s} {'n':>7s} {'agency MAE':>11s} "
              f"{'model MAE':>10s} {'improve':>8s}")
        for h in per_horizon:
            print(f"{h['horizon']:>9s} {h['n']:>7,} {h['agency_mae_s']:>11.1f} "
                  f"{h['model_mae_s']:>10.1f} {h['improvement_pct']:>7.1f}%")
    print("=" * 76)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
