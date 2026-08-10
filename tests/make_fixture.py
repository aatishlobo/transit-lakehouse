"""Generate a synthetic archive to exercise the profiler offline.

Simulates two agencies with deliberately different field population:
  AG_A -- populates current_status/stop_sequence (resolver A available)
  AG_B -- position only, and heavy absent-delay (resolver A unavailable)

This is the heterogeneity that pitfall 7.2 warns about, and the thing the
profiler must surface.
"""

import gzip
import json
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from google.transit import gtfs_realtime_pb2 as pb

from ingest.poller.decode import decode

random.seed(7)
BASE_TS = 1_754_000_000


def build_tu(poll: int) -> bytes:
    feed = pb.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = BASE_TS + poll * 120

    for agency, n_trips in (("AG_A", 6), ("AG_B", 4)):
        for t in range(n_trips):
            ent = feed.entity.add()
            ent.id = f"{agency}:tu{t}"
            tu = ent.trip_update
            tu.trip.trip_id = f"{agency}:trip{t}"
            tu.trip.route_id = f"{agency}:route{t % 2}"
            tu.trip.start_date = "20260801"
            tu.timestamp = BASE_TS + poll * 120
            if t == 3 and poll % 5 == 0:
                tu.trip.schedule_relationship = pb.TripDescriptor.CANCELED

            n_stops = 12 if agency == "AG_A" else 8
            for s in range(n_stops):
                stu = tu.stop_time_update.add()
                stu.stop_sequence = s + 1
                # AG_A trip0 is a loop: stop 1 revisited at sequence 10
                if agency == "AG_A" and t == 0 and s == 9:
                    stu.stop_id = f"{agency}:stop1"
                else:
                    stu.stop_id = f"{agency}:stop{s+1}"

                if agency == "AG_A":
                    # Well-populated: past stops settled (uncertainty=0)
                    stu.arrival.time = BASE_TS + poll * 120 + s * 90
                    stu.arrival.delay = random.choice([0, 0, 30, 60, 150, -20])
                    stu.arrival.uncertainty = 0 if s < poll % 6 else 60
                else:
                    # AG_B: ~55% of stops carry no arrival info at all.
                    # A naive decoder would call every one of these on-time.
                    if random.random() < 0.45:
                        stu.arrival.time = BASE_TS + poll * 120 + s * 100
                        stu.arrival.delay = random.choice([0, 90, 200])
                    if s == 6 and poll % 4 == 0:
                        stu.schedule_relationship = (
                            pb.TripUpdate.StopTimeUpdate.SKIPPED
                        )
    return feed.SerializeToString()


def build_vp(poll: int) -> bytes:
    feed = pb.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = BASE_TS + poll * 120

    for agency, n in (("AG_A", 6), ("AG_B", 4)):
        for v in range(n):
            ent = feed.entity.add()
            ent.id = f"{agency}:vp{v}"
            vp = ent.vehicle
            vp.trip.trip_id = f"{agency}:trip{v}"
            vp.trip.start_date = "20260801"
            vp.vehicle.id = f"{agency}:bus{v}"
            vp.position.latitude = 37.7 + v * 0.01
            vp.position.longitude = -122.4 + v * 0.01
            vp.timestamp = BASE_TS + poll * 120 - random.randint(0, 30)

            if agency == "AG_A":
                # Populates the fields resolver A needs
                vp.current_stop_sequence = (poll + v) % 12 + 1
                vp.stop_id = f"{agency}:stop{(poll + v) % 12 + 1}"
                vp.current_status = random.choice(
                    [
                        pb.VehiclePosition.STOPPED_AT,
                        pb.VehiclePosition.IN_TRANSIT_TO,
                        pb.VehiclePosition.IN_TRANSIT_TO,
                    ]
                )
            # AG_B: position only. current_status left unset -> resolver A dead.
    return feed.SerializeToString()


def main(root: Path, polls: int = 40) -> None:
    for poll in range(polls):
        for feed_type, builder in (
            ("trip_updates", build_tu),
            ("vehicle_positions", build_vp),
        ):
            raw = builder(poll)
            rows, meta = decode(raw, feed_type, poll_interval_s=120)
            name = f"{BASE_TS + poll*120}-{meta.payload_sha256[:8]}"
            dt = "2026-08-01"

            rp = root / "raw" / feed_type / f"ingest_dt={dt}"
            rp.mkdir(parents=True, exist_ok=True)
            (rp / f"{name}.pb.gz").write_bytes(gzip.compress(raw))

            dp = root / "decoded" / feed_type / f"ingest_dt={dt}"
            dp.mkdir(parents=True, exist_ok=True)
            body = "\n".join(json.dumps(r, separators=(",", ":")) for r in rows)
            (dp / f"{name}.jsonl.gz").write_bytes(gzip.compress(body.encode()))

    print(f"wrote {polls} polls x 2 feeds to {root}")


if __name__ == "__main__":
    main(Path(sys.argv[1] if len(sys.argv) > 1 else "data"))
