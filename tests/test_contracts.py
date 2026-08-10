"""Tests for the Kafka event contracts.

The headline test is test_absent_delay_survives_validation. The validation
boundary is the most likely place for the absent-vs-zero invariant to be
undone, because "fill in a default" is exactly what validation libraries are
built to do. Measured on the live feed, 44.2% of StopTimeUpdate rows carry no
delay -- so a default of 0 here would fabricate ~54,000 on-time records per
poll cycle.
"""

import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from streaming.contracts import (
    CONTRACT_VERSION,
    Envelope,
    TripIdentity,
    TripUpdateEvent,
    VehiclePositionEvent,
    from_decoded_row,
    key_for,
    partition_key,
)


def _envelope(**over):
    base = dict(
        feed_type="trip_updates",
        feed_header_ts="2026-08-06T21:52:00+00:00",
        feed_header_ts_epoch=1786052000,
        gtfs_realtime_version="2.0",
        incrementality=None,
        ingest_ts="2026-08-06T21:52:01+00:00",
        payload_sha256="a" * 64,
        poll_interval_s=120,
        decoder_version="1.0.0",
        protobuf_runtime="6.33.6",
    )
    base.update(over)
    return base


def _row(**over):
    """A flat decoder-shaped row, as decode.py emits."""
    base = dict(
        **_envelope(),
        trip_id="SF:1234",
        route_id="SF:38",
        direction_id=0,
        trip_start_time="14:52:00",
        service_date="20260806",
        trip_schedule_relationship="SCHEDULED",
        entity_id="SF:tu1",
        is_deleted=None,
        vehicle_id="SF:bus1",
        vehicle_label=None,
        trip_update_ts="2026-08-06T21:52:00+00:00",
        trip_update_ts_epoch=1786052000,
        trip_delay_s=None,
        n_stop_time_updates=3,
        stop_sequence=5,
        stop_id="SF:stop9",
        stop_schedule_relationship=None,
        arrival_time=None,
        arrival_time_epoch=None,
        arrival_delay_s=None,
        arrival_uncertainty=None,
        departure_time=None,
        departure_time_epoch=None,
        departure_delay_s=None,
        departure_uncertainty=None,
    )
    base.update(over)
    return base


# --------------------------------------------------------------------------
# The load-bearing invariant
# --------------------------------------------------------------------------

def test_absent_delay_survives_validation():
    """None must stay None. A default of 0 here fabricates on-time records."""
    ev = from_decoded_row(_row(arrival_delay_s=None))
    assert ev.arrival_delay_s is None, (
        "absent delay became a value at the validation boundary -- this is "
        "pitfall 2.1 reintroduced downstream of the decoder that prevents it"
    )


def test_explicit_zero_delay_survives_validation():
    """0 must stay 0 and must not be confused with absence."""
    ev = from_decoded_row(_row(arrival_delay_s=0))
    assert ev.arrival_delay_s == 0
    assert ev.arrival_delay_s is not None


def test_absent_and_zero_are_distinguishable_after_serialization():
    """The distinction must survive the trip through Kafka, not just memory."""
    import json

    absent = json.loads(from_decoded_row(_row(arrival_delay_s=None)).model_dump_json())
    zero = json.loads(from_decoded_row(_row(arrival_delay_s=0)).model_dump_json())
    assert absent["arrival_delay_s"] is None
    assert zero["arrival_delay_s"] == 0


def test_uncertainty_zero_survives():
    """uncertainty == 0 means 'observed, not predicted' -- resolver C's signal."""
    assert from_decoded_row(_row(arrival_uncertainty=0)).arrival_uncertainty == 0
    assert from_decoded_row(_row(arrival_uncertainty=None)).arrival_uncertainty is None


# --------------------------------------------------------------------------
# Drift and malformed input
# --------------------------------------------------------------------------

def test_unknown_field_is_rejected():
    """Schema drift must fail loudly, not be silently dropped."""
    with pytest.raises((ValidationError, TypeError)):
        from_decoded_row(_row(some_new_field="surprise"))


def test_string_number_is_rejected_not_coerced():
    """Strict mode: a wrong TYPE signals upstream change and must not be repaired."""
    with pytest.raises(ValidationError):
        from_decoded_row(_row(arrival_delay_s="0"))


def test_epoch_zero_rejected():
    """0 means 1970-01-01 -- always a bug leaking a default, never a real time."""
    with pytest.raises(ValidationError):
        from_decoded_row(_row(arrival_time_epoch=0))


def test_malformed_service_date_rejected():
    with pytest.raises(ValidationError):
        TripIdentity(service_date="2026-08-06")  # dashes; must be YYYYMMDD


def test_absent_service_date_allowed():
    """None is legal: 2,858 rows in one observed poll had no start_date."""
    assert TripIdentity(service_date=None).service_date is None


def test_out_of_range_latitude_rejected():
    """Catches an unset coordinate decoded as 0.0 -- the Gulf of Guinea bug."""
    with pytest.raises(ValidationError):
        VehiclePositionEvent(
            envelope=Envelope(**_envelope(feed_type="vehicle_positions")),
            trip=TripIdentity(),
            latitude=91.0,
            n_stop_time_updates=0,
        )


def test_events_are_immutable():
    ev = from_decoded_row(_row())
    with pytest.raises(ValidationError):
        ev.arrival_delay_s = 5


# --------------------------------------------------------------------------
# Partitioning
# --------------------------------------------------------------------------

def test_partition_key_includes_service_date():
    """trip_id repeats daily; the key must separate service dates."""
    assert partition_key("20260806", "SF:1") != partition_key("20260807", "SF:1")


def test_partition_key_is_stable():
    """Same trip, same key -- required for ordering across replays."""
    assert partition_key("20260806", "SF:1") == partition_key("20260806", "SF:1")


def test_unkeyed_records_get_a_stable_bucket():
    """A None key would be distributed round-robin, scattering the records."""
    k = partition_key(None, None)
    assert k == b"nodate:notrip"
    assert k == partition_key(None, None)


def test_key_for_event_matches_partition_key():
    ev = from_decoded_row(_row(service_date="20260806", trip_id="SF:1234"))
    assert key_for(ev) == partition_key("20260806", "SF:1234")


def test_contract_version_stamped():
    """Consumers must be able to tell which shape they are holding."""
    assert from_decoded_row(_row()).envelope.contract_version == CONTRACT_VERSION
