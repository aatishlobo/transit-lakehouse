"""
Presence-aware GTFS-Realtime decoding.

The single most important thing in this module: GTFS-RT is proto2, where every
scalar field is `optional` and absence is distinguishable from zero via
HasField(). The naive read `stu.arrival.delay` returns 0 for BOTH "on time" and
"no information". Conflating them injects a spike of fake on-time records into
the delay distribution that looks completely plausible and corrupts every
downstream metric and ML label.

Rule enforced here: NO scalar is ever read directly. Everything goes through
`_get()`, which returns None when the field is absent.

See pitfall register 2.1 (absent vs zero), 2.2 (version pinning),
1.1 (stale feed detection), 7.4 (missing vs skipped).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Iterator

from google.transit import gtfs_realtime_pb2 as pb
import google.protobuf


# Recorded on every row so we can tell which decoder produced which data
# when (not if) decode semantics change. Pitfall 2.2.
DECODER_VERSION = "1.0.0"
PROTOBUF_RUNTIME = google.protobuf.__version__

# Enum int -> name maps. We store the *name*, not the int, because the int
# meaning is only stable relative to a proto version.
SCHEDULE_RELATIONSHIP_TRIP = {
    v.number: v.name
    for v in pb.TripDescriptor.ScheduleRelationship.DESCRIPTOR.values
}
SCHEDULE_RELATIONSHIP_STOP = {
    v.number: v.name
    for v in pb.TripUpdate.StopTimeUpdate.ScheduleRelationship.DESCRIPTOR.values
}
VEHICLE_STOP_STATUS = {
    v.number: v.name for v in pb.VehiclePosition.VehicleStopStatus.DESCRIPTOR.values
}
CONGESTION_LEVEL = {
    v.number: v.name for v in pb.VehiclePosition.CongestionLevel.DESCRIPTOR.values
}
OCCUPANCY_STATUS = {
    v.number: v.name for v in pb.VehiclePosition.OccupancyStatus.DESCRIPTOR.values
}


def _get(msg, field_name: str) -> Any | None:
    """Read a scalar field, returning None when absent.

    This is the whole ballgame. Never bypass it.
    """
    if msg is None:
        return None
    try:
        if not msg.HasField(field_name):
            return None
    except ValueError:
        # HasField raises for repeated fields; those are handled explicitly
        # elsewhere and should never reach here.
        raise
    return getattr(msg, field_name)


def _sub(msg, field_name: str):
    """Read a submessage, returning None when absent.

    Distinct from _get because accessing an unset submessage in protobuf
    silently returns a default-valued instance rather than raising -- which
    is exactly how "no arrival information" becomes "arrival at epoch 0".
    """
    if msg is None:
        return None
    if not msg.HasField(field_name):
        return None
    return getattr(msg, field_name)


def _enum(msg, field_name: str, mapping: dict[int, str]) -> str | None:
    """Read an enum as its symbolic name, or None if absent.

    Note the trap: schedule_relationship defaults to SCHEDULED (0). An absent
    schedule_relationship and an explicit SCHEDULED are semantically the same
    per spec, but we preserve the distinction so profiling can measure how
    many agencies actually populate it.
    """
    raw = _get(msg, field_name)
    if raw is None:
        return None
    return mapping.get(raw, f"UNKNOWN_{raw}")


def _epoch_to_iso(ts: int | None) -> str | None:
    """POSIX seconds -> ISO8601 UTC. All timestamps stored UTC. Pitfall 4.6."""
    if ts is None:
        return None
    # Guard against the epoch-0 sentinel that indicates a producer bug.
    if ts <= 0:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


@dataclass
class FeedMeta:
    """Envelope metadata attached to every decoded row."""

    feed_type: str  # trip_updates | vehicle_positions | alerts
    feed_header_ts: str | None  # producer's own feed timestamp (UTC ISO)
    feed_header_ts_epoch: int | None
    gtfs_realtime_version: str | None
    incrementality: str | None
    ingest_ts: str  # when WE received it -- never used for event time
    payload_sha256: str  # content hash; identical hash => stale feed (1.1)
    poll_interval_s: int  # label noise floor (pitfall 7.3)
    decoder_version: str = DECODER_VERSION
    protobuf_runtime: str = PROTOBUF_RUNTIME


def parse_feed_header(raw: bytes, feed_type: str, poll_interval_s: int) -> tuple[pb.FeedMessage, FeedMeta]:
    """Parse bytes into a FeedMessage + envelope metadata.

    Raises on malformed input -- the caller must quarantine rather than crash
    (pitfall 1.2: HTTP 200 with an HTML error body parses to garbage or throws).
    """
    feed = pb.FeedMessage()
    feed.ParseFromString(raw)  # raises DecodeError on non-protobuf input

    header = feed.header
    header_ts = _get(header, "timestamp")

    meta = FeedMeta(
        feed_type=feed_type,
        feed_header_ts=_epoch_to_iso(header_ts),
        feed_header_ts_epoch=header_ts,
        gtfs_realtime_version=_get(header, "gtfs_realtime_version"),
        incrementality=(
            pb.FeedHeader.Incrementality.Name(header.incrementality)
            if header.HasField("incrementality")
            else None
        ),
        ingest_ts=datetime.now(timezone.utc).isoformat(),
        payload_sha256=hashlib.sha256(raw).hexdigest(),
        poll_interval_s=poll_interval_s,
    )
    return feed, meta


def _trip_descriptor(trip) -> dict[str, Any]:
    """Flatten a TripDescriptor.

    start_date is the GTFS *service date* and is authoritative -- do NOT derive
    service_date from a timestamp, because service days run past midnight
    (pitfalls 6.5, 6.6).
    """
    if trip is None:
        return {
            "trip_id": None,
            "route_id": None,
            "direction_id": None,
            "trip_start_time": None,
            "service_date": None,
            "trip_schedule_relationship": None,
        }
    return {
        "trip_id": _get(trip, "trip_id"),
        "route_id": _get(trip, "route_id"),
        "direction_id": _get(trip, "direction_id"),
        "trip_start_time": _get(trip, "start_time"),
        "service_date": _get(trip, "start_date"),  # YYYYMMDD, service date
        "trip_schedule_relationship": _enum(
            trip, "schedule_relationship", SCHEDULE_RELATIONSHIP_TRIP
        ),
    }


def decode_trip_updates(feed: pb.FeedMessage, meta: FeedMeta) -> Iterator[dict[str, Any]]:
    """Explode TripUpdates into one row per StopTimeUpdate.

    Fan-out is large (one TripUpdate -> tens of rows); see pitfall 2.4 for
    sizing implications.
    """
    base = asdict(meta)
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        trip_fields = _trip_descriptor(_sub(tu, "trip"))
        vehicle = _sub(tu, "vehicle")
        tu_ts = _get(tu, "timestamp")

        common = {
            **base,
            "entity_id": _get(entity, "id"),
            "is_deleted": _get(entity, "is_deleted"),
            **trip_fields,
            "vehicle_id": _get(vehicle, "id") if vehicle else None,
            "vehicle_label": _get(vehicle, "label") if vehicle else None,
            # The TripUpdate's own timestamp -- when the producer computed this
            # update. Closer to a true event time than feed_header_ts.
            "trip_update_ts": _epoch_to_iso(tu_ts),
            "trip_update_ts_epoch": tu_ts,
            # Trip-level delay. Absent != 0. Pitfall 2.1.
            "trip_delay_s": _get(tu, "delay"),
            "n_stop_time_updates": len(tu.stop_time_update),
        }

        if not tu.stop_time_update:
            # A TripUpdate with no StopTimeUpdates is meaningful (trip-level
            # delay only, or a cancellation). Emit it rather than dropping.
            yield {
                **common,
                "stop_sequence": None,
                "stop_id": None,
                "stop_schedule_relationship": None,
                "arrival_time": None,
                "arrival_time_epoch": None,
                "arrival_delay_s": None,
                "arrival_uncertainty": None,
                "departure_time": None,
                "departure_time_epoch": None,
                "departure_delay_s": None,
                "departure_uncertainty": None,
            }
            continue

        for stu in tu.stop_time_update:
            arrival = _sub(stu, "arrival")
            departure = _sub(stu, "departure")

            arr_time = _get(arrival, "time") if arrival else None
            dep_time = _get(departure, "time") if departure else None

            yield {
                **common,
                # stop_sequence is authoritative over stop_id: loop routes
                # serve the same stop_id twice per trip. Pitfall 7.6.
                "stop_sequence": _get(stu, "stop_sequence"),
                "stop_id": _get(stu, "stop_id"),
                "stop_schedule_relationship": _enum(
                    stu, "schedule_relationship", SCHEDULE_RELATIONSHIP_STOP
                ),
                "arrival_time": _epoch_to_iso(arr_time),
                "arrival_time_epoch": arr_time,
                # delay=None means no information; delay=0 means exactly on
                # time. These are different rows in the delay histogram.
                "arrival_delay_s": _get(arrival, "delay") if arrival else None,
                # uncertainty=0 is a documented signal meaning "this is an
                # observed value, not a prediction" -- the single most useful
                # field for resolver C. Preserve it exactly.
                "arrival_uncertainty": _get(arrival, "uncertainty") if arrival else None,
                "departure_time": _epoch_to_iso(dep_time),
                "departure_time_epoch": dep_time,
                "departure_delay_s": _get(departure, "delay") if departure else None,
                "departure_uncertainty": (
                    _get(departure, "uncertainty") if departure else None
                ),
            }


def decode_vehicle_positions(feed: pb.FeedMessage, meta: FeedMeta) -> Iterator[dict[str, Any]]:
    """One row per VehiclePosition entity."""
    base = asdict(meta)
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        vp = entity.vehicle
        trip_fields = _trip_descriptor(_sub(vp, "trip"))
        position = _sub(vp, "position")
        vehicle = _sub(vp, "vehicle")

        # The vehicle's OWN timestamp -- when this position was measured on
        # board. This is event time. Using poll time instead manufactures
        # fake observations from stale feeds (pitfall 1.1) and is the wrong
        # column for the ML point-in-time join (pitfall 12.2).
        vp_ts = _get(vp, "timestamp")

        yield {
            **base,
            "entity_id": _get(entity, "id"),
            "is_deleted": _get(entity, "is_deleted"),
            **trip_fields,
            "vehicle_id": _get(vehicle, "id") if vehicle else None,
            "vehicle_label": _get(vehicle, "label") if vehicle else None,
            "vehicle_report_ts": _epoch_to_iso(vp_ts),
            "vehicle_report_ts_epoch": vp_ts,
            "latitude": _get(position, "latitude") if position else None,
            "longitude": _get(position, "longitude") if position else None,
            "bearing": _get(position, "bearing") if position else None,
            "speed": _get(position, "speed") if position else None,
            # current_status == STOPPED_AT with a stop_id is resolver A --
            # the agency's own arrival determination. Highest-quality signal
            # available. Whether it is populated is agency-specific and is
            # exactly what Week 0 profiling must answer.
            "current_stop_sequence": _get(vp, "current_stop_sequence"),
            "current_stop_id": _get(vp, "stop_id"),
            "current_status": _enum(vp, "current_status", VEHICLE_STOP_STATUS),
            "congestion_level": _enum(vp, "congestion_level", CONGESTION_LEVEL),
            "occupancy_status": _enum(vp, "occupancy_status", OCCUPANCY_STATUS),
            "occupancy_percentage": _get(vp, "occupancy_percentage"),
        }


DECODERS = {
    "trip_updates": decode_trip_updates,
    "vehicle_positions": decode_vehicle_positions,
}


def decode(raw: bytes, feed_type: str, poll_interval_s: int) -> tuple[list[dict], FeedMeta]:
    """Full decode path: bytes -> (rows, metadata)."""
    feed, meta = parse_feed_header(raw, feed_type, poll_interval_s)
    decoder = DECODERS.get(feed_type)
    if decoder is None:
        raise ValueError(f"no decoder for feed_type={feed_type!r}")
    return list(decoder(feed, meta)), meta


def dedup_key(row: dict[str, Any]) -> str:
    """Natural key for deduplication across poller restarts and replays.

    A restarted poller re-polls and re-produces the identical payload
    (pitfall 3.2). Keying on producer-side identity rather than our poll time
    makes that a no-op.
    """
    parts = [
        row.get("feed_type"),
        str(row.get("feed_header_ts_epoch")),
        row.get("entity_id"),
        str(row.get("stop_sequence")),  # None for vehicle_positions
    ]
    return hashlib.sha256("|".join(str(p) for p in parts).encode()).hexdigest()[:32]
