"""Tests for the prediction-accuracy aggregator.

Two behaviours here are easy to get subtly wrong and impossible to notice from
the output, because both produce plausible-looking numbers:

  * scoring a "prediction" that was issued AFTER the vehicle already arrived,
    which flatters the agency by counting hindsight as foresight;
  * truncating hour-of-day in UTC, which shifts every bar of an hourly chart by
    7-8 hours without changing any total.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from streaming.aggregator import Aggregator, bucket_for, hour_of, summarize


def _pred(trip_id="SF:t1", service_date="20260806", seq=5,
          issued=1_786_053_000, predicted=1_786_053_300):
    return {
        "envelope": {"feed_header_ts_epoch": issued, "poll_interval_s": 120},
        "trip": {"trip_id": trip_id, "service_date": service_date, "route_id": "SF:1"},
        "stop_sequence": seq,
        "arrival_time_epoch": predicted,
        "trip_update_ts_epoch": issued,
    }


def _arr(trip_id="SF:t1", service_date="20260806", seq=5,
         actual=1_786_053_300, agency="SF"):
    return {
        "service_date": service_date,
        "trip_id": trip_id,
        "stop_sequence": seq,
        "stop_id": "13550",
        "route_id": "SF:1",
        "agency": agency,
        "actual_arrival_ts_epoch": actual,
        "actual_arrival_ts": "2026-08-06T21:55:00+00:00",
        "poll_interval_s": 120,
        "arrival_method": "stopped_at",
    }


# --------------------------------------------------------------------------
# Error sign convention
# --------------------------------------------------------------------------

def test_predicting_late_gives_positive_error():
    a = Aggregator("SF")
    a.add_prediction(_pred(issued=1000, predicted=2100))
    a.add_arrival(_arr(actual=2000))
    pairs = a.join()
    assert len(pairs) == 1
    assert pairs[0]["error_s"] == 100


def test_predicting_early_gives_negative_error():
    """Vehicle arrives later than promised -- the optimism case."""
    a = Aggregator("SF")
    a.add_prediction(_pred(issued=1000, predicted=1900))
    a.add_arrival(_arr(actual=2000))
    assert a.join()[0]["error_s"] == -100


def test_lead_time_measured_from_issue_to_actual_arrival():
    a = Aggregator("SF")
    a.add_prediction(_pred(issued=1000, predicted=1800))
    a.add_arrival(_arr(actual=2000))
    assert a.join()[0]["lead_time_s"] == 1000


# --------------------------------------------------------------------------
# Hindsight must not be scored as foresight
# --------------------------------------------------------------------------

def test_prediction_issued_after_arrival_is_excluded():
    """A 'prediction' made after the fact is a correction, not a forecast."""
    a = Aggregator("SF")
    a.add_prediction(_pred(issued=3000, predicted=3010))  # after actual=2000
    a.add_arrival(_arr(actual=2000))
    assert a.join() == []
    assert a.stats["pairs_dropped_negative_lead"] == 1


def test_exclusion_is_counted_not_silent():
    a = Aggregator("SF")
    a.add_prediction(_pred(issued=3000, predicted=3010))
    a.add_prediction(_pred(issued=1000, predicted=1950))
    a.add_arrival(_arr(actual=2000))
    pairs = a.join()
    assert len(pairs) == 1
    assert a.stats["pairs_dropped_negative_lead"] == 1
    assert a.stats["arrivals_matched"] == 1


def test_arrival_with_no_predictions_is_counted_unmatched():
    a = Aggregator("SF")
    a.add_arrival(_arr())
    assert a.join() == []
    assert a.stats["arrivals_unmatched"] == 1


def test_one_arrival_pairs_with_every_earlier_prediction():
    """Each successive prediction for the same stop is scored separately."""
    a = Aggregator("SF")
    for issued in (1000, 1200, 1400, 1600):
        a.add_prediction(_pred(issued=issued, predicted=2000))
    a.add_arrival(_arr(actual=2000))
    pairs = a.join()
    assert len(pairs) == 4
    assert sorted(p["lead_time_s"] for p in pairs) == [400, 600, 800, 1000]


# --------------------------------------------------------------------------
# Grain and filtering
# --------------------------------------------------------------------------

def test_join_is_keyed_on_stop_sequence_not_stop_id():
    """Loop routes revisit a stop_id; predictions must not cross-match."""
    a = Aggregator("SF")
    a.add_prediction(_pred(seq=3, issued=1000, predicted=1500))
    a.add_arrival(_arr(seq=17, actual=2000))
    assert a.join() == []


def test_service_date_separates_repeated_trip_ids():
    a = Aggregator("SF")
    a.add_prediction(_pred(service_date="20260806", issued=1000, predicted=1900))
    a.add_arrival(_arr(service_date="20260807", actual=2000))
    assert a.join() == []


def test_agency_filter_excludes_other_operators():
    a = Aggregator("SF")
    a.add_prediction(_pred(trip_id="AC:t9", issued=1000, predicted=1900))
    a.add_arrival(_arr(trip_id="AC:t9", agency="AC", actual=2000))
    assert a.stats["predictions_buffered"] == 0
    assert a.stats["arrivals_seen"] == 0


# --------------------------------------------------------------------------
# Bucketing and summary statistics
# --------------------------------------------------------------------------

def test_lead_buckets_partition_the_range():
    assert bucket_for(0) == "0-2 min"
    assert bucket_for(119) == "0-2 min"
    assert bucket_for(120) == "2-5 min"
    assert bucket_for(299) == "2-5 min"
    assert bucket_for(300) == "5-10 min"
    assert bucket_for(1200) == "20+ min"
    assert bucket_for(99999) == "20+ min"


def test_summary_bias_is_signed_mean():
    """Bias must not be an absolute value -- direction is the whole point."""
    pairs = [
        {"lead_bucket": "2-5 min", "error_s": -100, "abs_error_s": 100},
        {"lead_bucket": "2-5 min", "error_s": -200, "abs_error_s": 200},
    ]
    row = summarize(pairs, ["lead_bucket"])[0]
    assert row["mean_error_s"] == -150.0
    assert row["median_abs_error_s"] == 150.0
    assert row["n"] == 2


def test_symmetric_errors_cancel_in_bias_but_not_in_abs():
    """Random error averages out; systematic error does not. That is why
    both statistics are reported."""
    pairs = [
        {"lead_bucket": "b", "error_s": 100, "abs_error_s": 100},
        {"lead_bucket": "b", "error_s": -100, "abs_error_s": 100},
    ]
    row = summarize(pairs, ["lead_bucket"])[0]
    assert row["mean_error_s"] == 0.0
    assert row["median_abs_error_s"] == 100.0


def test_within_threshold_percentages():
    pairs = [
        {"lead_bucket": "b", "error_s": 30, "abs_error_s": 30},
        {"lead_bucket": "b", "error_s": 90, "abs_error_s": 90},
        {"lead_bucket": "b", "error_s": 300, "abs_error_s": 300},
        {"lead_bucket": "b", "error_s": -45, "abs_error_s": 45},
    ]
    row = summarize(pairs, ["lead_bucket"])[0]
    assert row["pct_within_60s"] == 50.0
    assert row["pct_within_180s"] == 75.0


# --------------------------------------------------------------------------
# Timezone
# --------------------------------------------------------------------------

def test_hour_is_local_not_utc():
    """UTC truncation shifts an hourly chart by 7-8 hours (pitfall 8.4).

    21:52 UTC is 14:52 Pacific. If this returns 21, every hour-of-day
    conclusion in the project is wrong while every total stays correct.
    """
    h = hour_of("2026-08-06T21:52:47+00:00")
    assert h is not None
    assert h != 21 or hour_of("2026-08-06T07:00:00+00:00") != 7, (
        "hour appears to be UTC rather than local time"
    )


def test_hour_handles_malformed_timestamps():
    assert hour_of("") is None
    assert hour_of("not-a-timestamp") is None
    assert hour_of(None) is None


# --------------------------------------------------------------------------
# Idempotence under duplicate delivery
# --------------------------------------------------------------------------

def test_duplicate_predictions_do_not_double_the_counts():
    """Replaying the same archive twice must not inflate n.

    Regression cover for a real defect: with a list-backed buffer, a second
    replay doubled every pair count while leaving bias and percentiles
    identical -- so the output looked correct and n silently lied.
    """
    a = Aggregator("SF")
    for _ in range(2):  # two identical replays
        a.add_prediction(_pred(issued=1000, predicted=1900))
        a.add_arrival(_arr(actual=2000))
    pairs = a.join()
    assert len(pairs) == 1, "duplicate delivery inflated the pair count"
    assert a.stats["duplicate_predictions_collapsed"] == 1
    assert a.stats["duplicate_arrivals_collapsed"] == 1


def test_distinct_predictions_for_one_stop_are_all_kept():
    """Dedup must collapse only true duplicates, not successive forecasts."""
    a = Aggregator("SF")
    a.add_prediction(_pred(issued=1000, predicted=1900))
    a.add_prediction(_pred(issued=1200, predicted=1950))
    a.add_arrival(_arr(actual=2000))
    assert len(a.join()) == 2
    assert a.stats["duplicate_predictions_collapsed"] == 0


def test_same_prediction_reissued_later_is_not_a_duplicate():
    """Same predicted time, different issue time = a new observation."""
    a = Aggregator("SF")
    a.add_prediction(_pred(issued=1000, predicted=1900))
    a.add_prediction(_pred(issued=1500, predicted=1900))
    a.add_arrival(_arr(actual=2000))
    assert len(a.join()) == 2
