"""Tests for the arrival resolver.

The headline test is test_arrival_is_first_sighting_not_last. A vehicle reports
STOPPED_AT on every poll while it waits at a stop; taking the last sighting
would silently measure DEPARTURE instead of arrival. Nothing would error -- the
timestamps would just all be wrong, by the dwell time, in one direction.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from streaming.consumer import ArrivalResolver, Stats, agency_of


def _vp(
    trip_id="SF:t1",
    service_date="20260806",
    status="IN_TRANSIT_TO",
    seq=5,
    ts_epoch=1786053000,
    stop_id="13550",
    route_id="SF:1",
    vehicle_id="5885",
    poll_interval_s=120,
):
    """A decoded vehicle_positions event, contract-shaped."""
    return {
        "envelope": {
            "poll_interval_s": poll_interval_s,
            "decoder_version": "1.0.0",
            "contract_version": "1.0.0",
        },
        "trip": {
            "trip_id": trip_id,
            "route_id": route_id,
            "service_date": service_date,
            "direction_id": 0,
        },
        "vehicle_id": vehicle_id,
        "vehicle_report_ts": f"2026-08-06T21:{ts_epoch % 60:02d}:00+00:00",
        "vehicle_report_ts_epoch": ts_epoch,
        "current_stop_sequence": seq,
        "current_stop_id": stop_id,
        "current_status": status,
    }


def _resolver():
    return ArrivalResolver(Stats())


# --------------------------------------------------------------------------
# The load-bearing behaviour
# --------------------------------------------------------------------------

def test_arrival_is_first_sighting_not_last():
    """A parked vehicle reports STOPPED_AT repeatedly. Only the first counts."""
    r = _resolver()
    assert r.process(_vp(status="IN_TRANSIT_TO", ts_epoch=1000)) is None
    first = r.process(_vp(status="STOPPED_AT", ts_epoch=1120))
    assert first is not None, "first STOPPED_AT must produce an arrival"
    assert first["actual_arrival_ts_epoch"] == 1120

    # Still sitting there two polls later -- must NOT emit again, or the
    # timestamp would drift toward the departure time.
    assert r.process(_vp(status="STOPPED_AT", ts_epoch=1240)) is None
    assert r.process(_vp(status="STOPPED_AT", ts_epoch=1360)) is None
    assert r.stats.arrivals == 1
    assert r.stats.suppressed_repeat == 2


def test_each_stop_in_a_trip_resolves_once():
    r = _resolver()
    for seq, ts in ((1, 1000), (2, 1120), (3, 1240)):
        assert r.process(_vp(status="STOPPED_AT", seq=seq, ts_epoch=ts)) is not None
    assert r.stats.arrivals == 3


def test_revisiting_a_stop_id_at_a_different_sequence_resolves_twice():
    """Loop routes: same stop_id, two sequences, two genuine arrivals.

    7 of 28 operators do this. A stop_id-keyed resolver would emit only one
    and silently lose a real event (pitfall 7.6).
    """
    r = _resolver()
    a = r.process(_vp(status="STOPPED_AT", seq=3, stop_id="SAME", ts_epoch=1000))
    b = r.process(_vp(status="STOPPED_AT", seq=17, stop_id="SAME", ts_epoch=2000))
    assert a is not None and b is not None
    assert a["stop_id"] == b["stop_id"] == "SAME"
    assert a["stop_sequence"] != b["stop_sequence"]
    assert r.stats.arrivals == 2


def test_different_trips_are_tracked_independently():
    r = _resolver()
    assert r.process(_vp(trip_id="SF:t1", status="STOPPED_AT", seq=5)) is not None
    assert r.process(_vp(trip_id="SF:t2", status="STOPPED_AT", seq=5)) is not None
    assert r.stats.arrivals == 2


def test_same_trip_id_on_different_service_dates_is_separate():
    """trip_id repeats daily; yesterday's stop 5 must not suppress today's."""
    r = _resolver()
    assert r.process(_vp(service_date="20260806", status="STOPPED_AT")) is not None
    assert r.process(_vp(service_date="20260807", status="STOPPED_AT")) is not None
    assert r.stats.arrivals == 2


# --------------------------------------------------------------------------
# Event time
# --------------------------------------------------------------------------

def test_event_time_comes_from_the_vehicle():
    """Never our poll clock -- a stale feed would fabricate an arrival."""
    r = _resolver()
    a = r.process(_vp(status="STOPPED_AT", ts_epoch=1786053150))
    assert a["actual_arrival_ts_epoch"] == 1786053150
    assert a["actual_arrival_ts"].endswith("+00:00")


def test_missing_vehicle_timestamp_is_skipped_not_substituted():
    r = _resolver()
    ev = _vp(status="STOPPED_AT")
    ev["vehicle_report_ts"] = None
    assert r.process(ev) is None
    assert r.stats.skipped_no_event_ts == 1
    assert r.stats.arrivals == 0


# --------------------------------------------------------------------------
# Skips are measurements, not noise
# --------------------------------------------------------------------------

def test_absent_status_is_counted_not_dropped():
    """The skip count IS the per-agency resolver-coverage measurement."""
    r = _resolver()
    ev = _vp(status="STOPPED_AT")
    ev["current_status"] = None
    assert r.process(ev) is None
    assert r.stats.skipped_no_status == 1


def test_absent_stop_sequence_is_counted():
    r = _resolver()
    ev = _vp(status="STOPPED_AT")
    ev["current_stop_sequence"] = None
    assert r.process(ev) is None
    assert r.stats.skipped_no_sequence == 1


def test_absent_trip_id_is_counted():
    r = _resolver()
    assert r.process(_vp(trip_id=None, status="STOPPED_AT")) is None
    assert r.stats.skipped_no_trip == 1


def test_non_stopped_statuses_never_emit():
    r = _resolver()
    for s in ("IN_TRANSIT_TO", "INCOMING_AT"):
        assert r.process(_vp(status=s)) is None
    assert r.stats.arrivals == 0


# --------------------------------------------------------------------------
# Provenance
# --------------------------------------------------------------------------

def test_provenance_is_stamped_on_every_arrival():
    """Without this, agency silently correlates with label quality (7.2)."""
    r = _resolver()
    a = r.process(_vp(status="STOPPED_AT"))
    assert a["arrival_method"] == "stopped_at"
    assert a["arrival_confidence"] == "high"
    assert a["resolver_version"]
    # The error bar: an arrival can be no more precise than the sampling
    # interval that observed it.
    assert a["poll_interval_s"] == 120


def test_poll_interval_travels_with_the_row():
    """A cadence change must not invalidate previously derived rows."""
    r = _resolver()
    a = r.process(_vp(status="STOPPED_AT", seq=1, poll_interval_s=120))
    b = r.process(_vp(status="STOPPED_AT", seq=2, poll_interval_s=30))
    assert a["poll_interval_s"] == 120
    assert b["poll_interval_s"] == 30


def test_grain_fields_present():
    r = _resolver()
    a = r.process(_vp(status="STOPPED_AT"))
    for f in ("service_date", "trip_id", "stop_sequence"):
        assert a[f] is not None, f"grain field {f} missing"


# --------------------------------------------------------------------------
# Idempotence
# --------------------------------------------------------------------------

def test_replaying_identical_input_yields_identical_output():
    """Re-processing must converge, since delivery is at-least-once (3.7)."""
    events = [
        _vp(status="IN_TRANSIT_TO", seq=1, ts_epoch=1000),
        _vp(status="STOPPED_AT", seq=1, ts_epoch=1120),
        _vp(status="STOPPED_AT", seq=1, ts_epoch=1240),
        _vp(status="STOPPED_AT", seq=2, ts_epoch=1360),
    ]

    def run():
        r = _resolver()
        return [a for a in (r.process(e) for e in events) if a]

    a, b = run(), run()
    key = lambda rows: [(x["trip_id"], x["stop_sequence"],
                         x["actual_arrival_ts_epoch"]) for x in rows]
    assert key(a) == key(b)
    assert len(a) == 2


def test_agency_extraction():
    assert agency_of("SF:12053041", None) == "SF"
    assert agency_of(None, "AC:51B") == "AC"
    assert agency_of("nocolon", None) == "UNKNOWN"
    assert agency_of(None, None) == "UNKNOWN"
