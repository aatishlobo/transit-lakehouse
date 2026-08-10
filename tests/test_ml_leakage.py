"""Tests for the prediction-correction model's leakage defences.

These are the tests that matter most for this component. A leaking model does
not crash, does not warn, and does not look wrong -- it posts an excellent
score and is worthless in production. Every failure mode below produces a
BETTER-looking number, which is exactly why each needs an explicit test rather
than reviewer vigilance.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ml.predict import MAX_HORIZON_S, Corrector, build_feature_row
from ml.train import FEATURES, TARGET


# --------------------------------------------------------------------------
# Feature-set leakage
# --------------------------------------------------------------------------

def test_lead_time_is_not_a_feature():
    """lead_time = actual - issued, so it CONTAINS the target.

    It is used legitimately in evaluation (streaming/aggregator.py) and would
    be a natural copy-paste into the model. Doing so would produce a
    spectacular MAE and a model that cannot run, because lead_time is unknowable
    until the vehicle has already arrived.
    """
    assert "lead_time_s" not in FEATURES
    assert "lead_time" not in FEATURES


def test_actual_arrival_is_not_a_feature():
    for banned in ("actual_epoch", "actual_arrival_ts", "actual_arrival_ts_epoch",
                   "residual_s"):
        assert banned not in FEATURES, f"{banned} leaks the target"


def test_target_is_not_in_the_feature_list():
    assert TARGET not in FEATURES


def test_every_feature_is_knowable_at_issue_time():
    """Whitelist, deliberately. A new feature must be justified, not assumed."""
    knowable = {
        "horizon_s",            # predicted - issued, both known at issue
        "hour", "dow",          # from issued timestamp
        "stop_sequence_f",      # from the update itself
        "agency_delay_s", "agency_delay_known",
        "uncertainty", "uncertainty_known",
        "direction_id", "n_stops_in_update", "route_code",
    }
    unexpected = set(FEATURES) - knowable
    assert not unexpected, (
        f"features not verified knowable at issue time: {unexpected}. "
        f"Add to the whitelist only after confirming the value exists before "
        f"the vehicle arrives."
    )


# --------------------------------------------------------------------------
# Absent vs zero, one layer down
# --------------------------------------------------------------------------

def test_absent_agency_delay_is_not_imputed_to_zero():
    """Imputing 0 tells the model 'on time' when the truth is 'no data'."""
    row = build_feature_row(
        horizon_s=600, issued_epoch=1_786_053_000, stop_sequence=10,
        agency_delay_s=None, uncertainty=None, direction_id=0,
        n_stops_in_update=30, route_id="SF:1", route_categories=["SF:1"],
    )
    assert row["agency_delay_s"] is None, "absent delay was imputed"
    assert row["agency_delay_known"] == 0


def test_explicit_zero_delay_is_distinguishable_from_absent():
    zero = build_feature_row(
        horizon_s=600, issued_epoch=1_786_053_000, stop_sequence=10,
        agency_delay_s=0, uncertainty=0, direction_id=0,
        n_stops_in_update=30, route_id="SF:1", route_categories=["SF:1"],
    )
    assert zero["agency_delay_s"] == 0
    assert zero["agency_delay_known"] == 1
    assert zero["uncertainty_known"] == 1


# --------------------------------------------------------------------------
# The split
# --------------------------------------------------------------------------

def _synthetic(n=2000):
    rng = np.random.default_rng(0)
    base = 1_786_000_000
    return pd.DataFrame({
        "issued_epoch": np.sort(rng.integers(base, base + 500_000, n)),
        "trip_id": [f"SF:t{i % 50}" for i in range(n)],
        "residual_s": rng.normal(60, 30, n),
    })


def test_temporal_split_puts_no_training_row_after_any_test_row():
    """The defining property: training must never contain the future."""
    df = _synthetic().sort_values("issued_epoch").reset_index(drop=True)
    cut = int(df.loc[int(len(df) * 0.75), "issued_epoch"])
    train, test = df[df.issued_epoch < cut], df[df.issued_epoch >= cut]
    assert train["issued_epoch"].max() < test["issued_epoch"].min()


def test_random_split_would_leak_trips_across_the_boundary():
    """Demonstrates why the temporal split exists.

    Each trip contributes ~10 rows. A random split scatters them, so the model
    memorises trips and is tested on the trips it memorised.
    """
    df = _synthetic()
    shuffled = df.sample(frac=1, random_state=1)
    cut = int(len(shuffled) * 0.75)
    tr, te = shuffled.iloc[:cut], shuffled.iloc[cut:]
    overlap = set(tr["trip_id"]) & set(te["trip_id"])
    assert len(overlap) > 40, (
        "expected a random split to leak nearly every trip across the boundary"
    )


def test_baselines_must_be_fitted_on_training_data_only():
    """A baseline fitted on test leaks and understates the model's advantage."""
    df = _synthetic().sort_values("issued_epoch").reset_index(drop=True)
    cut = int(df.loc[int(len(df) * 0.75), "issued_epoch"])
    train, test = df[df.issued_epoch < cut], df[df.issued_epoch >= cut]
    train_bias = train["residual_s"].mean()
    test_bias = test["residual_s"].mean()
    honest = np.mean(np.abs(test["residual_s"] - train_bias))
    cheating = np.mean(np.abs(test["residual_s"] - test_bias))
    assert cheating <= honest, (
        "a test-fitted baseline should look at least as good -- that is "
        "precisely why it must not be used"
    )


# --------------------------------------------------------------------------
# Serving fallback
# --------------------------------------------------------------------------

def test_missing_artifact_falls_back_to_agency_prediction():
    """No model must mean 'serve the agency ETA', never a crash or a zero."""
    c = Corrector(artifact=Path("ml/artifacts/does_not_exist.joblib"))
    assert c.enabled is False
    out = c.correct(predicted_arrival_epoch=1_786_053_600,
                    issued_epoch=1_786_053_000)
    assert out["applied"] is False
    assert out["corrected_eta_epoch"] == out["agency_eta_epoch"]
    assert out["correction_s"] == 0


def test_model_that_lost_to_baseline_is_not_served(tmp_path):
    """A model must earn its place on every retrain, not just the first."""
    import joblib
    from sklearn.ensemble import HistGradientBoostingRegressor

    art = tmp_path / "m.joblib"
    rep = tmp_path / "r.json"
    m = HistGradientBoostingRegressor(max_iter=5)
    m.fit(np.zeros((20, len(FEATURES))), np.zeros(20))
    joblib.dump({"model": m, "features": FEATURES, "route_categories": []}, art)
    rep.write_text('{"model_beats_best_baseline": false}')

    c = Corrector(artifact=art, report=rep)
    assert c.enabled is False
    assert "did not beat" in c.reason


def test_horizon_outside_trained_range_is_not_corrected(tmp_path):
    """Refuse to extrapolate: an absurd horizon gets the agency value back."""
    import joblib
    from sklearn.ensemble import HistGradientBoostingRegressor

    art = tmp_path / "m.joblib"
    rep = tmp_path / "r.json"
    m = HistGradientBoostingRegressor(max_iter=5)
    m.fit(np.zeros((20, len(FEATURES))), np.zeros(20))
    joblib.dump({"model": m, "features": FEATURES, "route_categories": []}, art)
    rep.write_text('{"model_beats_best_baseline": true}')

    c = Corrector(artifact=art, report=rep)
    assert c.enabled is True
    issued = 1_786_053_000
    out = c.correct(predicted_arrival_epoch=issued + MAX_HORIZON_S + 1,
                    issued_epoch=issued)
    assert out["applied"] is False
    assert out["corrected_eta_epoch"] == out["agency_eta_epoch"]


def test_correction_is_additive_on_the_agency_eta():
    """corrected = agency + correction. Never a replacement from scratch."""
    import joblib
    from sklearn.ensemble import HistGradientBoostingRegressor

    art = Path("ml/artifacts/prediction_correction_model.joblib")
    rep = Path("ml/artifacts/training_report.json")
    if not art.exists():
        pytest.skip("model not trained yet")
    c = Corrector(artifact=art, report=rep)
    if not c.enabled:
        pytest.skip(f"model not served: {c.reason}")
    issued = 1_786_053_000
    out = c.correct(predicted_arrival_epoch=issued + 600, issued_epoch=issued,
                    route_id="SF:1", stop_sequence=20, n_stops_in_update=30,
                    direction_id=0)
    assert out["corrected_eta_epoch"] == out["agency_eta_epoch"] + out["correction_s"]
