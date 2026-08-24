"""GTFS scheduled-time arithmetic. Invariant 3.6.

Two things about GTFS times break naive code, and both fail silently.

**1. Hours run past 24.** `stop_times.arrival_time` of `25:14:00` is valid and
means 1:14am the following calendar day. A trip departing at 23:50 and arriving
at 24:20 is one trip, not a trip that goes backwards in time. Parsing with a
time library rejects it or wraps it; either way the late-night service that is
most interesting for reliability analysis is the part you lose.

**2. The service day does not start at midnight.** The GTFS spec defines times
as measured from *noon minus 12 hours* of the service date -- "effectively
midnight except for days on which daylight savings time changes occur." Noon is
used as the anchor precisely because noon is never ambiguous, while midnight
can be skipped or repeated by a DST transition.

Worked example, spring forward on 2026-03-08 (US):

    local noon           = 12:00 PDT      = 19:00 UTC
    noon - 12h           =                  07:00 UTC   <- the reference
    local midnight       = 00:00 PST      = 08:00 UTC   <- NOT the reference

    a trip scheduled "08:00:00":
      correct:   07:00 UTC + 8h = 15:00 UTC = 08:00 PDT   correct
      midnight:  08:00 UTC + 8h = 16:00 UTC = 09:00 PDT   one hour late

Every scheduled time on that day would be an hour off, in the same direction,
and the resulting OTP number would look entirely plausible.

Implementation note: the service-day start is computed once per service_date in
Python (where `zoneinfo` gets DST exactly right) and broadcast to Spark as an
integer epoch. Scheduled time is then integer addition, which is both fast and
immune to Spark's timezone-function subtleties.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

AGENCY_TZ = ZoneInfo("America/Los_Angeles")

_TIME_RE = re.compile(r"^(\d{1,3}):([0-5]\d):([0-5]\d)$")


def gtfs_time_to_seconds(value: str | None) -> int | None:
    """Parse `HH:MM:SS` into seconds from service-day start.

    Accepts hours >= 24 deliberately. Returns None on absent/malformed input
    rather than 0 -- invariant 3.1, absent is not zero, and 0 here would mean
    "scheduled at exactly service-day start", a real and different claim.
    """
    if value is None:
        return None
    m = _TIME_RE.match(value.strip())
    if not m:
        return None
    h, mi, s = (int(g) for g in m.groups())
    return h * 3600 + mi * 60 + s


def service_day_start_utc(service_date: str, tz: ZoneInfo = AGENCY_TZ) -> datetime:
    """The instant a GTFS service day begins, as a UTC datetime.

    service_date is `YYYYMMDD` (GTFS `start_date`), never derived from a
    timestamp -- invariant 3.3.

    Uses the spec's noon-minus-12h rule so both DST transitions are handled.
    Anchoring on noon is what makes this safe: at 02:00 local on a transition
    day the wall clock is either skipped or repeated, so localizing midnight is
    ill-defined, while noon is unambiguous on every day of the year.
    """
    if not re.fullmatch(r"\d{8}", service_date or ""):
        raise ValueError(f"service_date must be YYYYMMDD, got {service_date!r}")
    y, m, d = int(service_date[:4]), int(service_date[4:6]), int(service_date[6:8])
    noon_local = datetime(y, m, d, 12, 0, 0, tzinfo=tz)
    # Convert to UTC BEFORE subtracting. Arithmetic on an aware datetime is
    # wall-clock arithmetic within its own zone, so `noon_local - 12h` yields
    # local midnight -- collapsing straight back to the naive behaviour this
    # function exists to avoid. Subtracting in UTC is absolute-duration
    # arithmetic, which is what the spec's noon-minus-12h rule means.
    return noon_local.astimezone(timezone.utc) - timedelta(hours=12)


def scheduled_utc(service_date: str, gtfs_time: str, tz: ZoneInfo = AGENCY_TZ):
    """Full path: (service_date, 'HH:MM:SS') -> UTC datetime, or None."""
    secs = gtfs_time_to_seconds(gtfs_time)
    if secs is None:
        return None
    return service_day_start_utc(service_date, tz) + timedelta(seconds=secs)


def service_day_start_epochs(service_dates) -> dict[str, int]:
    """Build the {service_date: epoch_seconds} lookup broadcast into Spark.

    Small by construction -- one entry per service date, not per row.
    """
    valid = {
        sd
        for sd in service_dates
        if isinstance(sd, str) and re.fullmatch(r"\d{8}", sd)
    }
    # Filter BEFORE sorting: a None in the set makes sorted() raise TypeError,
    # which in Spark surfaces as an opaque driver-side failure rather than as
    # "one row had no service_date".
    return {sd: int(service_day_start_utc(sd).timestamp()) for sd in sorted(valid)}
