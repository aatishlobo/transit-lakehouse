"""Invariant 3.6: scheduled-time arithmetic across both DST transitions.

CLAUDE.md requires that unit tests pin BOTH transition dates. These are the
tests that would have caught the bug this module was written with: subtracting
12 hours from an aware datetime performs wall-clock arithmetic inside the zone,
silently collapsing the spec's noon-minus-12h anchor back to naive local
midnight and shifting every scheduled time on a transition day by an hour.
"""

from __future__ import annotations

from datetime import timezone
from zoneinfo import ZoneInfo

import pytest

from spark.scheduled_time import (
    AGENCY_TZ,
    gtfs_time_to_seconds,
    scheduled_utc,
    service_day_start_epochs,
    service_day_start_utc,
)

# US DST transitions in 2026.
SPRING_FORWARD = "20260308"  # 02:00 -> 03:00, the local day is 23 hours
FALL_BACK = "20261101"  # 02:00 -> 01:00, the local day is 25 hours
NORMAL = "20260806"


# --- parsing -------------------------------------------------------------


def test_past_midnight_hours_are_valid():
    """25:14:00 means 1:14am the NEXT day. Rejecting it loses late service."""
    assert gtfs_time_to_seconds("25:14:00") == 25 * 3600 + 14 * 60


def test_hour_28_parses():
    assert gtfs_time_to_seconds("28:00:00") == 28 * 3600


def test_absent_time_is_none_not_zero():
    """Invariant 3.1. Zero would mean 'scheduled at service-day start'."""
    assert gtfs_time_to_seconds(None) is None
    assert gtfs_time_to_seconds("") is None
    assert gtfs_time_to_seconds("not a time") is None
    assert gtfs_time_to_seconds("08:00:00") == 28800  # 0 would be wrong here


def test_malformed_minutes_rejected():
    assert gtfs_time_to_seconds("08:60:00") is None
    assert gtfs_time_to_seconds("08:00:60") is None


# --- the DST cases -------------------------------------------------------


def test_spring_forward_8am_is_pdt():
    """On 2026-03-08 the clocks jump 02:00 -> 03:00 before 8am, so 08:00
    local is PDT (UTC-7) and must land at 15:00 UTC.

    The naive-midnight bug produces 16:00 UTC here."""
    got = scheduled_utc(SPRING_FORWARD, "08:00:00")
    assert got.astimezone(timezone.utc).isoformat() == "2026-03-08T15:00:00+00:00"


def test_fall_back_8am_is_pst():
    """On 2026-11-01 the clocks fall back before 8am, so 08:00 local is PST
    (UTC-8) and must land at 16:00 UTC.

    The naive-midnight bug produces 15:00 UTC here -- note it errs in the
    OPPOSITE direction from spring forward, which is why one transition alone
    is not a sufficient test."""
    got = scheduled_utc(FALL_BACK, "08:00:00")
    assert got.astimezone(timezone.utc).isoformat() == "2026-11-01T16:00:00+00:00"


def test_both_transitions_agree_on_local_wall_time():
    """The invariant a rider cares about: '8am' is 8am on every date."""
    for sd in (SPRING_FORWARD, FALL_BACK, NORMAL):
        local = scheduled_utc(sd, "08:00:00").astimezone(AGENCY_TZ)
        assert (local.hour, local.minute) == (8, 0), sd


def test_service_day_lengths_are_23_24_25_hours():
    """Direct evidence the anchor tracks DST rather than assuming 24h days."""
    def hours(a: str, b: str) -> float:
        return (service_day_start_utc(b) - service_day_start_utc(a)).total_seconds() / 3600

    # The short/long span runs from the day BEFORE the transition to the
    # transition day, because it is that interval which contains the clock
    # change. Measured starts: 20260308 begins 23:00 local on 03-07, and
    # 20261101 begins 01:00 local on 11-01 -- the spec's "effectively
    # midnight except on days when DST changes", made concrete.
    assert hours("20260307", "20260308") == 23  # spring forward
    assert hours("20261031", "20261101") == 25  # fall back
    assert hours("20260806", "20260807") == 24  # normal


def test_past_midnight_crosses_the_calendar_date():
    """A 25:14 arrival on 2026-08-06 is on 2026-08-07 in UTC and locally."""
    got = scheduled_utc(NORMAL, "25:14:00")
    assert got.astimezone(timezone.utc).isoformat() == "2026-08-07T08:14:00+00:00"
    assert got.astimezone(AGENCY_TZ).strftime("%Y-%m-%d %H:%M") == "2026-08-07 01:14"


def test_past_midnight_on_spring_forward_stays_on_the_wall_clock():
    """25:00 is one hour past the service day's own midnight, on every date.

    On the 23-hour day the service day starts at 23:00 local on 03-07, so
    24:00 lands exactly on local midnight 03-09 and 25:00 on 01:00 local. The
    lost hour is absorbed by the anchor, not by the trip -- which is the whole
    reason the anchor is noon rather than midnight."""
    got = scheduled_utc(SPRING_FORWARD, "25:00:00")
    assert got.astimezone(AGENCY_TZ).strftime("%Y-%m-%d %H:%M") == "2026-03-09 01:00"
    midnight = scheduled_utc(SPRING_FORWARD, "24:00:00")
    assert midnight.astimezone(AGENCY_TZ).strftime("%Y-%m-%d %H:%M") == "2026-03-09 00:00"


# --- broadcast lookup ----------------------------------------------------


def test_epoch_lookup_matches_the_scalar_path():
    """The Spark path (integer epoch + seconds) must equal the Python path.

    If these ever diverge, every scheduled time in the lake is wrong while
    every unit test above still passes."""
    lookup = service_day_start_epochs([SPRING_FORWARD, FALL_BACK, NORMAL])
    for sd in (SPRING_FORWARD, FALL_BACK, NORMAL):
        for t in ("00:00:00", "08:00:00", "25:14:00"):
            expected = int(scheduled_utc(sd, t).timestamp())
            assert lookup[sd] + gtfs_time_to_seconds(t) == expected, (sd, t)


def test_epoch_lookup_skips_junk_service_dates():
    lookup = service_day_start_epochs(["20260806", None, "", "not-a-date", "2026080"])
    assert set(lookup) == {"20260806"}


def test_bad_service_date_raises():
    for bad in ("2026-08-06", "", None, "20260"):
        with pytest.raises(ValueError):
            service_day_start_utc(bad)


def test_timezone_is_the_agency_not_the_machine():
    """Running this pipeline in UTC must not change the answer."""
    got = scheduled_utc(NORMAL, "08:00:00", tz=ZoneInfo("America/Los_Angeles"))
    assert got.astimezone(timezone.utc).isoformat() == "2026-08-06T15:00:00+00:00"
