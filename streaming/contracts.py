"""Event contracts for the Kafka path.

WHY A CONTRACT EXISTS AT ALL
----------------------------
The producer and the consumer are written by the same person today, so it is
tempting to skip validation and just publish dictionaries. The contract is not
there to catch a stranger's mistakes -- it is there because:

  * It is the *boundary of ownership*. Once an event is on a topic, any number
    of consumers may read it (DE#5 reads these same topics). The contract is
    the only statement of what those consumers are entitled to assume.
  * It catches DRIFT. If decode.py grows a field and this file doesn't, the
    mismatch fails loudly here instead of silently reaching a consumer that
    ignores it. That is what `extra="forbid"` below is for.
  * It makes bad records *routable*. A record that fails validation goes to a
    dead-letter topic and the stream keeps running -- rather than crashing the
    consumer, or worse, being silently skipped.

THE ONE INVARIANT THIS FILE MUST NOT BREAK
------------------------------------------
Every nullable numeric field here (`arrival_delay_s`, `arrival_uncertainty`,
`trip_delay_s`, ...) distinguishes None ("no information") from 0 ("exactly on
time"). Measured on the live regional feed, 44.2% of StopTimeUpdate rows carry
NO delay at all; treating those as 0 fabricates a huge spike of on-time records
that corrupts every downstream metric (pitfall 2.1).

This is precisely why Pydantic **v2** is a hard requirement and not a
preference. Pydantic v1 would coerce a None into a field's default, which would
undo the entire absent-vs-zero property at the validation boundary -- the one
place in the system specifically meant to protect it. Defaults here are `None`
and the types are `Optional`, so absence survives.

Strict mode is on for the same reason: in lax mode Pydantic will happily turn
the string "0" into the integer 0. Values arriving as the wrong *type* signal
that something upstream changed, and we want that to fail rather than be
quietly repaired.
"""

from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

# Bump on any breaking change to these models. Stamped on every event so a
# consumer can tell which shape it is holding -- the same reasoning as
# DECODER_VERSION in decode.py (pitfall 2.2).
CONTRACT_VERSION = "1.0.0"

TOPIC_TRIP_UPDATES = "gtfsrt.trip_updates.v1"
TOPIC_VEHICLE_POSITIONS = "gtfsrt.vehicle_positions.v1"
TOPIC_DEAD_LETTER = "gtfsrt.dead_letter.v1"
TOPIC_ARRIVAL_EVENTS = "transit.arrival_events.v1"


class _Base(BaseModel):
    model_config = ConfigDict(
        # Reject unknown fields. If decode.py starts emitting something this
        # file doesn't declare, that is schema drift and must be loud.
        extra="forbid",
        # No silent type coercion -- see module docstring.
        strict=True,
        # Events are immutable once published; mutating a validated event
        # in-flight would let it diverge from what was actually written.
        frozen=True,
    )


class Envelope(_Base):
    """Metadata attached to every event, from the feed header and our poll."""

    feed_type: Literal["trip_updates", "vehicle_positions"]
    feed_header_ts: Optional[str] = None
    feed_header_ts_epoch: Optional[int] = None
    gtfs_realtime_version: Optional[str] = None
    incrementality: Optional[str] = None

    # When WE received it. Never event time (pitfall 12.2). Kept so the
    # observation delay (event_ts -> ingest_ts) stays measurable.
    ingest_ts: str

    # Content hash. Identical hash across consecutive polls == stale upstream.
    payload_sha256: str

    # The sampling granularity in force when this was captured. This is the
    # NOISE FLOOR of every arrival timestamp derived from it -- an arrival can
    # never be more precise than the interval that observed it. Carried per
    # event so a mid-project cadence change doesn't silently invalidate older
    # rows (pitfall 7.3).
    poll_interval_s: int

    decoder_version: str
    protobuf_runtime: str
    contract_version: str = CONTRACT_VERSION


class TripIdentity(_Base):
    """The GTFS trip a record belongs to."""

    trip_id: Optional[str] = None
    route_id: Optional[str] = None
    direction_id: Optional[int] = None
    trip_start_time: Optional[str] = None

    # Authoritative service date from TripDescriptor.start_date, format
    # YYYYMMDD. Never derived from a timestamp: GTFS service days run past
    # midnight, so a 00:40 trip commonly belongs to the PREVIOUS service date
    # (pitfalls 6.5/6.6).
    service_date: Optional[str] = None

    trip_schedule_relationship: Optional[str] = None

    @field_validator("service_date")
    @classmethod
    def _service_date_shape(cls, v: Optional[str]) -> Optional[str]:
        # None is legal and common -- 2,858 rows in a single observed poll had
        # no start_date. Only a PRESENT but malformed value is an error.
        if v is None:
            return v
        if len(v) != 8 or not v.isdigit():
            raise ValueError(f"service_date must be YYYYMMDD, got {v!r}")
        return v


class TripUpdateEvent(_Base):
    """One row per StopTimeUpdate -- the grain the resolver consumes.

    Grain is (service_date, trip_id, stop_sequence). NOT stop_id: measured on
    the live feed, 7 of 28 operators run trips that visit the same stop_id
    twice (Emery Go-Round: 23 of 29 trips). A stop_id grain collapses two real
    events into one (pitfall 7.6).
    """

    envelope: Envelope
    trip: TripIdentity

    entity_id: Optional[str] = None
    is_deleted: Optional[bool] = None

    vehicle_id: Optional[str] = None
    vehicle_label: Optional[str] = None

    # The producer's own timestamp for this update. Closer to true event time
    # than the feed header.
    trip_update_ts: Optional[str] = None
    trip_update_ts_epoch: Optional[int] = None

    # Trip-level delay. None != 0.
    trip_delay_s: Optional[int] = None
    n_stop_time_updates: int

    stop_sequence: Optional[int] = None
    stop_id: Optional[str] = None
    stop_schedule_relationship: Optional[str] = None

    arrival_time: Optional[str] = None
    arrival_time_epoch: Optional[int] = None
    arrival_delay_s: Optional[int] = None
    # uncertainty == 0 is a documented signal meaning "observed, not
    # predicted". It is the entire basis of resolver C, so 0 must survive as 0
    # and absence must survive as None.
    arrival_uncertainty: Optional[int] = None

    departure_time: Optional[str] = None
    departure_time_epoch: Optional[int] = None
    departure_delay_s: Optional[int] = None
    departure_uncertainty: Optional[int] = None

    @field_validator("arrival_time_epoch", "departure_time_epoch",
                     "trip_update_ts_epoch")
    @classmethod
    def _no_epoch_zero(cls, v: Optional[int]) -> Optional[int]:
        # A literal 0 here means 1970-01-01 and is always a producer bug or a
        # default leaking through. Reject rather than let it become a real
        # timestamp 56 years in the past.
        if v is not None and v <= 0:
            raise ValueError(f"non-positive epoch timestamp: {v}")
        return v

    @field_validator("arrival_uncertainty", "departure_uncertainty")
    @classmethod
    def _uncertainty_non_negative(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v < 0:
            raise ValueError(f"negative uncertainty: {v}")
        return v


class VehiclePositionEvent(_Base):
    """One row per VehiclePosition entity.

    `current_status == STOPPED_AT` plus a stop is resolver A -- the agency's
    own arrival determination, and the highest-quality signal available.
    Measured: available for 15 of 28 operators.
    """

    envelope: Envelope
    trip: TripIdentity

    entity_id: Optional[str] = None
    is_deleted: Optional[bool] = None

    vehicle_id: Optional[str] = None
    vehicle_label: Optional[str] = None

    # EVENT TIME. Measured on board, by the vehicle. Using our poll clock
    # instead manufactures observations from stale feeds and is the wrong
    # column for any point-in-time join (pitfalls 1.1, 12.2).
    vehicle_report_ts: Optional[str] = None
    vehicle_report_ts_epoch: Optional[int] = None

    latitude: Optional[float] = None
    longitude: Optional[float] = None
    bearing: Optional[float] = None
    speed: Optional[float] = None

    current_stop_sequence: Optional[int] = None
    current_stop_id: Optional[str] = None
    current_status: Optional[str] = None

    congestion_level: Optional[str] = None
    occupancy_status: Optional[str] = None
    occupancy_percentage: Optional[int] = None

    @field_validator("vehicle_report_ts_epoch")
    @classmethod
    def _no_epoch_zero(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v <= 0:
            raise ValueError(f"non-positive epoch timestamp: {v}")
        return v

    @field_validator("latitude")
    @classmethod
    def _lat_range(cls, v: Optional[float]) -> Optional[float]:
        # Out-of-range coordinates are usually an unset field decoded as 0.0,
        # which would place the vehicle in the Gulf of Guinea. Range-checking
        # catches that class of error even when presence checks can't.
        if v is not None and not (-90.0 <= v <= 90.0):
            raise ValueError(f"latitude out of range: {v}")
        return v

    @field_validator("longitude")
    @classmethod
    def _lon_range(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (-180.0 <= v <= 180.0):
            raise ValueError(f"longitude out of range: {v}")
        return v


class DeadLetter(BaseModel):
    """A record that failed validation, preserved with enough context to fix.

    Deliberately permissive (no strict/forbid): the whole point is to accept
    something already known to be malformed. Dropping a bad record loses the
    evidence needed to diagnose it; crashing on one stops the stream.
    """

    model_config = ConfigDict(extra="allow")

    topic: str
    reason: str
    error: str
    contract_version: str = CONTRACT_VERSION
    failed_at: str
    raw: dict[str, Any] = Field(default_factory=dict)


# --------------------------------------------------------------------------
# Adapter: decoder output -> contract
# --------------------------------------------------------------------------

_ENVELOPE_FIELDS = frozenset(Envelope.model_fields) - {"contract_version"}
_TRIP_FIELDS = frozenset(TripIdentity.model_fields)

_EVENT_MODEL = {
    "trip_updates": TripUpdateEvent,
    "vehicle_positions": VehiclePositionEvent,
}


def from_decoded_row(row: dict[str, Any]) -> TripUpdateEvent | VehiclePositionEvent:
    """Reshape one flat row from decode.py into a validated event.

    decode.py emits a FLAT dict (envelope, trip and record fields all at the
    top level) because that shape writes straight to JSONL. The contract nests
    them, because on a topic the envelope is metadata about the delivery while
    the trip is part of the payload -- and a consumer that only needs the
    envelope shouldn't have to know the record's field names.

    Raises ValidationError on anything that doesn't conform; the caller is
    expected to dead-letter it rather than crash.
    """
    feed_type = row.get("feed_type")
    model = _EVENT_MODEL.get(feed_type)
    if model is None:
        raise ValueError(f"no contract for feed_type={feed_type!r}")

    envelope = {k: v for k, v in row.items() if k in _ENVELOPE_FIELDS}
    trip = {k: v for k, v in row.items() if k in _TRIP_FIELDS}
    rest = {
        k: v
        for k, v in row.items()
        if k not in _ENVELOPE_FIELDS and k not in _TRIP_FIELDS
    }

    return model(envelope=Envelope(**envelope), trip=TripIdentity(**trip), **rest)


def key_for(event: TripUpdateEvent | VehiclePositionEvent) -> bytes:
    return partition_key(event.trip.service_date, event.trip.trip_id)


# --------------------------------------------------------------------------
# Partition key
# --------------------------------------------------------------------------

def partition_key(service_date: Optional[str], trip_id: Optional[str]) -> bytes:
    """Kafka message key: `{service_date}:{trip_id}`.

    Kafka guarantees ordering only WITHIN a partition, and routes by key hash.
    Keying on the trip therefore guarantees that every observation of one trip
    lands in one partition, in order.

    That guarantee is load-bearing rather than cosmetic. The arrival resolver
    detects arrivals by watching a vehicle move through states across
    consecutive polls (IN_TRANSIT_TO -> STOPPED_AT -> IN_TRANSIT_TO). If those
    observations were spread over three partitions and consumed concurrently,
    the transitions could be seen out of order and arrivals would be missed or
    invented.

    service_date is included because trip_id is only unique WITHIN a service
    date -- the same trip_id recurs every day. Keying on trip_id alone would
    force two different days of the same trip into one partition, needlessly
    coupling them and skewing the load.

    Records with no trip identity get a stable "unkeyed" bucket rather than a
    None key (which Kafka would distribute round-robin, silently scattering
    them). They cannot be resolved anyway, but they must remain inspectable.
    """
    sd = service_date or "nodate"
    tid = trip_id or "notrip"
    return f"{sd}:{tid}".encode()
