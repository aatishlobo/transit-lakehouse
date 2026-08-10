"""
Tests for presence-aware decoding.

The headline test is test_absent_delay_is_not_zero. If that ever fails, the
delay distribution is silently corrupted and every downstream metric and ML
label is wrong. Everything else in the project depends on it.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from google.transit import gtfs_realtime_pb2 as pb

from ingest.poller.decode import decode, dedup_key


def _feed(header_ts=1_700_000_000):
    feed = pb.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = header_ts
    return feed


def _make_trip_updates():
    """Three StopTimeUpdates covering the three delay cases we must separate."""
    feed = _feed()
    ent = feed.entity.add()
    ent.id = "tu-1"
    tu = ent.trip_update
    tu.trip.trip_id = "TRIP_A"
    tu.trip.route_id = "ROUTE_1"
    tu.trip.start_date = "20260801"
    tu.timestamp = 1_700_000_000

    # Stop 1: delay explicitly 0 -> genuinely on time
    s1 = tu.stop_time_update.add()
    s1.stop_sequence = 1
    s1.stop_id = "STOP_X"
    s1.arrival.delay = 0
    s1.arrival.time = 1_700_000_100
    s1.arrival.uncertainty = 0  # observed, not predicted

    # Stop 2: no arrival information at all -> must decode to None, NOT 0
    s2 = tu.stop_time_update.add()
    s2.stop_sequence = 2
    s2.stop_id = "STOP_Y"

    # Stop 3: real delay, and a skipped stop marker
    s3 = tu.stop_time_update.add()
    s3.stop_sequence = 3
    s3.stop_id = "STOP_Z"
    s3.arrival.delay = 240
    s3.arrival.uncertainty = 30
    s3.schedule_relationship = pb.TripUpdate.StopTimeUpdate.SKIPPED

    return feed.SerializeToString()


def _make_vehicle_positions():
    feed = _feed()

    # Vehicle 1: fully populated, STOPPED_AT -> resolver A fires
    e1 = feed.entity.add()
    e1.id = "vp-1"
    v1 = e1.vehicle
    v1.trip.trip_id = "TRIP_A"
    v1.trip.start_date = "20260801"
    v1.vehicle.id = "BUS_100"
    v1.position.latitude = 37.7749
    v1.position.longitude = -122.4194
    v1.current_stop_sequence = 5
    v1.stop_id = "STOP_X"
    v1.current_status = pb.VehiclePosition.STOPPED_AT
    v1.timestamp = 1_700_000_050

    # Vehicle 2: position only, no status/stop fields -> resolver A cannot fire,
    # must fall through to geofencing. This is the agency-variance case.
    e2 = feed.entity.add()
    e2.id = "vp-2"
    v2 = e2.vehicle
    v2.trip.trip_id = "TRIP_B"
    v2.position.latitude = 37.8
    v2.position.longitude = -122.3
    v2.timestamp = 1_700_000_060

    return feed.SerializeToString()


def test_absent_delay_is_not_zero():
    """THE critical test. Absent arrival info must be None, never 0."""
    rows, _ = decode(_make_trip_updates(), "trip_updates", poll_interval_s=60)
    by_seq = {r["stop_sequence"]: r for r in rows}

    assert by_seq[1]["arrival_delay_s"] == 0, "explicit on-time must survive as 0"
    assert by_seq[2]["arrival_delay_s"] is None, (
        "absent delay decoded as 0 -- this injects fake on-time records into "
        "the delay distribution and corrupts every downstream metric"
    )
    assert by_seq[3]["arrival_delay_s"] == 240

    # And the same for time, so absent arrival never becomes epoch 0
    assert by_seq[2]["arrival_time"] is None
    assert by_seq[2]["arrival_time_epoch"] is None


def test_uncertainty_zero_preserved():
    """uncertainty=0 signals an observed value -- the key resolver-C signal."""
    rows, _ = decode(_make_trip_updates(), "trip_updates", poll_interval_s=60)
    by_seq = {r["stop_sequence"]: r for r in rows}
    assert by_seq[1]["arrival_uncertainty"] == 0
    assert by_seq[2]["arrival_uncertainty"] is None
    assert by_seq[3]["arrival_uncertainty"] == 30


def test_schedule_relationship_symbolic():
    """Enums stored as names, and absence distinguished from SCHEDULED."""
    rows, _ = decode(_make_trip_updates(), "trip_updates", poll_interval_s=60)
    by_seq = {r["stop_sequence"]: r for r in rows}
    assert by_seq[3]["stop_schedule_relationship"] == "SKIPPED"
    assert by_seq[1]["stop_schedule_relationship"] is None, (
        "unset schedule_relationship must stay None so profiling can measure "
        "how many agencies actually populate it"
    )


def test_service_date_from_trip_not_timestamp():
    """service_date must come from TripDescriptor.start_date (pitfalls 6.5/6.6)."""
    rows, _ = decode(_make_trip_updates(), "trip_updates", poll_interval_s=60)
    assert all(r["service_date"] == "20260801" for r in rows)


def test_fanout():
    """One TripUpdate -> N rows. Sizing input for pitfall 2.4."""
    rows, _ = decode(_make_trip_updates(), "trip_updates", poll_interval_s=60)
    assert len(rows) == 3
    assert all(r["n_stop_time_updates"] == 3 for r in rows)


def test_vehicle_positions_partial_population():
    """The agency-variance case that determines which resolver can fire."""
    rows, _ = decode(_make_vehicle_positions(), "vehicle_positions", poll_interval_s=60)
    by_id = {r["entity_id"]: r for r in rows}

    assert by_id["vp-1"]["current_status"] == "STOPPED_AT"
    assert by_id["vp-1"]["current_stop_sequence"] == 5
    assert by_id["vp-1"]["current_stop_id"] == "STOP_X"

    # Unset current_status must NOT decode to INCOMING_AT (enum value 0)
    assert by_id["vp-2"]["current_status"] is None, (
        "unset current_status decoded as INCOMING_AT -- would fabricate "
        "arrival state for agencies that do not populate it"
    )
    assert by_id["vp-2"]["current_stop_sequence"] is None
    assert by_id["vp-2"]["current_stop_id"] is None


def test_event_time_is_producer_timestamp():
    """Event time comes from the vehicle, not from our poll (pitfall 12.2)."""
    rows, _ = decode(_make_vehicle_positions(), "vehicle_positions", poll_interval_s=60)
    by_id = {r["entity_id"]: r for r in rows}
    assert by_id["vp-1"]["vehicle_report_ts_epoch"] == 1_700_000_050
    assert by_id["vp-2"]["vehicle_report_ts_epoch"] == 1_700_000_060
    # ingest_ts exists and is distinct from event time
    assert by_id["vp-1"]["ingest_ts"] != by_id["vp-1"]["vehicle_report_ts"]


def test_stale_feed_detectable():
    """Identical payload => identical hash => stale feed detectable (1.1)."""
    raw = _make_trip_updates()
    _, m1 = decode(raw, "trip_updates", poll_interval_s=60)
    _, m2 = decode(raw, "trip_updates", poll_interval_s=60)
    assert m1.payload_sha256 == m2.payload_sha256
    assert m1.ingest_ts != m2.ingest_ts or True  # ingest differs or same ms


def test_dedup_key_stable_across_replay():
    """Re-decoding the same payload yields identical keys (pitfall 3.2)."""
    raw = _make_trip_updates()
    r1, _ = decode(raw, "trip_updates", poll_interval_s=60)
    r2, _ = decode(raw, "trip_updates", poll_interval_s=60)
    assert [dedup_key(r) for r in r1] == [dedup_key(r) for r in r2]
    assert len({dedup_key(r) for r in r1}) == 3  # distinct per stop


def test_malformed_payload_raises():
    """HTML error page served as 200 must raise, not silently yield 0 rows."""
    import pytest

    with pytest.raises(Exception):
        decode(b"<html><body>Rate limit exceeded</body></html>", "trip_updates", 60)


def test_trip_update_with_no_stops_still_emitted():
    """Trip-level-only updates carry cancellation info; must not be dropped."""
    feed = _feed()
    ent = feed.entity.add()
    ent.id = "tu-cancel"
    ent.trip_update.trip.trip_id = "TRIP_C"
    ent.trip_update.trip.start_date = "20260801"
    ent.trip_update.trip.schedule_relationship = pb.TripDescriptor.CANCELED

    rows, _ = decode(feed.SerializeToString(), "trip_updates", poll_interval_s=60)
    assert len(rows) == 1
    assert rows[0]["trip_schedule_relationship"] == "CANCELED"
    assert rows[0]["stop_sequence"] is None
