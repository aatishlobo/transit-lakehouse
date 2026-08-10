"""Inference: apply the correction model to a live agency prediction.

This is the serving side of the bounded AI element. Given what is knowable at
the moment an agency issues an ETA, return a corrected ETA.

    corrected_eta = agency_eta + predicted_residual

THE FALLBACK IS PART OF THE DESIGN, NOT AN AFTERTHOUGHT
-------------------------------------------------------
`correct()` returns the agency's own prediction unchanged whenever it cannot do
better with confidence:

  * no model artifact on disk;
  * the model failed to beat its best baseline at training time;
  * the input falls outside the range the model was trained on.

The baseline is the production default and the model is an override that has to
earn its place. That ordering matters: a correction model that silently degrades
an ETA is worse than no model, because riders would trust a number that we made
worse. The course requires a stated fallback; this is it, and it is enforced in
code rather than described in a document.

TRAIN/SERVE CONSISTENCY
-----------------------
Features are built here by the SAME helper the training table uses, from the
same raw fields, so the serving path cannot drift from the training path
without a test failing. Train/serve skew is pitfall 12.5 and is normally
invisible: the model looks fine offline and quietly underperforms in
production.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

log = logging.getLogger("predict")

DEFAULT_ARTIFACT = Path("ml/artifacts/prediction_correction_model.joblib")
DEFAULT_REPORT = Path("ml/artifacts/training_report.json")

# Guard rails: refuse to extrapolate beyond what the model saw.
MIN_HORIZON_S = 1
MAX_HORIZON_S = 3600


def build_feature_row(
    *,
    horizon_s: int,
    issued_epoch: int,
    stop_sequence: int,
    agency_delay_s: int | None,
    uncertainty: int | None,
    direction_id: int | None,
    n_stops_in_update: int,
    route_id: str,
    route_categories: list[str],
) -> dict:
    """Assemble one feature row. Shared with training by construction.

    Note the absent-vs-zero handling: `agency_delay_s` stays None (which the
    gradient booster treats as genuinely missing) and a separate _known flag
    carries the presence bit. Imputing 0 here would tell the model "on time"
    when the truth is "no information" -- the same pitfall 2.1 error, one layer
    further down the stack.
    """
    local = datetime.fromtimestamp(issued_epoch, tz=timezone.utc).astimezone()
    try:
        route_code = route_categories.index(route_id)
    except ValueError:
        route_code = -1  # unseen route; the tree handles it as its own branch
    return {
        "horizon_s": horizon_s,
        "hour": local.hour,
        "dow": local.weekday(),
        "stop_sequence_f": stop_sequence,
        "agency_delay_s": agency_delay_s,
        "agency_delay_known": 0 if agency_delay_s is None else 1,
        "uncertainty": uncertainty,
        "uncertainty_known": 0 if uncertainty is None else 1,
        "direction_id": direction_id,
        "n_stops_in_update": n_stops_in_update,
        "route_code": route_code,
    }


class Corrector:
    def __init__(self, artifact: Path = DEFAULT_ARTIFACT,
                 report: Path = DEFAULT_REPORT):
        self.model = None
        self.features: list[str] = []
        self.route_categories: list[str] = []
        self.enabled = False
        self.reason = "not loaded"

        if not artifact.exists():
            self.reason = f"no artifact at {artifact}"
            return
        try:
            import joblib
            bundle = joblib.load(artifact)
            self.model = bundle["model"]
            self.features = bundle["features"]
            self.route_categories = bundle.get("route_categories", [])
        except Exception as e:
            self.reason = f"artifact failed to load: {e}"
            return

        # Refuse to serve a model that did not beat its baseline.
        if report.exists():
            try:
                r = json.loads(report.read_text())
                if not r.get("model_beats_best_baseline", False):
                    self.reason = (
                        "model did not beat its best baseline at training time "
                        "-- serving agency predictions unchanged"
                    )
                    return
            except json.JSONDecodeError:
                pass

        self.enabled = True
        self.reason = "ok"

    def correct(self, *, predicted_arrival_epoch: int, issued_epoch: int,
                stop_sequence: int = 0, agency_delay_s: int | None = None,
                uncertainty: int | None = None, direction_id: int | None = None,
                n_stops_in_update: int = 0, route_id: str = "") -> dict:
        """Return the corrected ETA, or the original with a reason."""
        horizon = predicted_arrival_epoch - issued_epoch
        original = {
            "agency_eta_epoch": predicted_arrival_epoch,
            "corrected_eta_epoch": predicted_arrival_epoch,
            "correction_s": 0,
            "applied": False,
        }

        if not self.enabled:
            return {**original, "reason": self.reason}
        if not (MIN_HORIZON_S <= horizon <= MAX_HORIZON_S):
            return {**original,
                    "reason": f"horizon {horizon}s outside trained range "
                              f"[{MIN_HORIZON_S}, {MAX_HORIZON_S}]"}

        import pandas as pd

        row = build_feature_row(
            horizon_s=horizon, issued_epoch=issued_epoch,
            stop_sequence=stop_sequence, agency_delay_s=agency_delay_s,
            uncertainty=uncertainty, direction_id=direction_id,
            n_stops_in_update=n_stops_in_update, route_id=route_id,
            route_categories=self.route_categories,
        )
        X = pd.DataFrame([row])[self.features]
        correction = int(round(float(self.model.predict(X)[0])))
        return {
            "agency_eta_epoch": predicted_arrival_epoch,
            "corrected_eta_epoch": predicted_arrival_epoch + correction,
            "correction_s": correction,
            "applied": True,
            "reason": "ok",
        }


def main() -> int:
    ap = argparse.ArgumentParser(description="Apply the ETA correction model")
    ap.add_argument("--horizon-s", type=int, default=600)
    ap.add_argument("--route-id", default="SF:1")
    ap.add_argument("--stop-sequence", type=int, default=20)
    ap.add_argument("--agency-delay-s", type=int, default=None)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    c = Corrector()
    now = int(datetime.now(timezone.utc).timestamp())
    out = c.correct(
        predicted_arrival_epoch=now + args.horizon_s,
        issued_epoch=now,
        stop_sequence=args.stop_sequence,
        agency_delay_s=args.agency_delay_s,
        route_id=args.route_id,
        n_stops_in_update=30,
        direction_id=0,
    )
    if args.json:
        print(json.dumps(out, indent=2))
    else:
        print(f"model enabled : {c.enabled}  ({c.reason})")
        print(f"route         : {args.route_id}  stop_seq={args.stop_sequence}")
        print(f"agency ETA    : in {args.horizon_s}s")
        if out["applied"]:
            print(f"correction    : {out['correction_s']:+d}s")
            print(f"corrected ETA : in {args.horizon_s + out['correction_s']}s")
        else:
            print(f"correction    : none applied -- {out['reason']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
