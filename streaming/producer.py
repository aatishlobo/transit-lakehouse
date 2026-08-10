"""Replay producer: archived GTFS-RT bytes -> validated events -> Kafka.

This is the deterministic replay path the course review depends on. It reads
the archive from disk, decodes it, validates every row against the contract,
and publishes to Kafka. No API key, no network, byte-identical every run.

WHY REPLAY AND LIVE ARE THE SAME CODE PATH
------------------------------------------
A tempting shortcut is to have the live poller publish straight to Kafka and
treat replay as a separate testing tool. That produces two code paths that
drift, and the one the reviewer runs is the one that gets less attention.
Instead the poller only ever writes to disk, and EVERYTHING downstream reads
from disk. Live and replay differ solely in whether new files are still
appearing. The reviewer therefore exercises the real pipeline, not a mock.

THE ORDERING PROBLEM THIS FILE SOLVES
-------------------------------------
The archive stores each feed in its own directory. The naive replay -- publish
all trip_updates, then all vehicle_positions -- would deliver an entire hour of
predictions before the first GPS ping. The arrival resolver correlates the two
feeds in time, so it would see every prediction with no positions to match
against and derive nothing.

So polls from both feeds are merged into ONE stream ordered by poll timestamp,
reproducing the interleaving that actually occurred. Replay must recreate the
sequence of observation, not just its contents.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from pydantic import ValidationError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingest.poller.decode import decode
from streaming.contracts import (
    TOPIC_DEAD_LETTER,
    TOPIC_TRIP_UPDATES,
    TOPIC_VEHICLE_POSITIONS,
    DeadLetter,
    from_decoded_row,
    key_for,
)

log = logging.getLogger("producer")

TOPIC_FOR_FEED = {
    "trip_updates": TOPIC_TRIP_UPDATES,
    "vehicle_positions": TOPIC_VEHICLE_POSITIONS,
}


@dataclass
class Stats:
    polls: int = 0
    rows: int = 0
    published: int = 0
    dead_lettered: int = 0
    decode_failures: int = 0
    delivery_failures: int = 0
    by_feed: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "polls": self.polls,
            "rows": self.rows,
            "published": self.published,
            "dead_lettered": self.dead_lettered,
            "decode_failures": self.decode_failures,
            "delivery_failures": self.delivery_failures,
            "by_feed": self.by_feed,
        }


def poll_ts(p: Path) -> int:
    """Archive filenames are {poll_epoch}-{sha8}.pb.gz."""
    return int(p.name.split("-", 1)[0])


def discover(root: Path, feeds: list[str]) -> list[tuple[int, str, Path]]:
    """All archived polls across feeds, merged and sorted by poll time.

    The sort is the whole point -- see the module docstring.
    """
    out: list[tuple[int, str, Path]] = []
    for feed in feeds:
        base = root / "raw" / feed
        if not base.exists():
            log.warning("no archive for feed=%s under %s", feed, base)
            continue
        for p in base.rglob("*.pb.gz"):
            out.append((poll_ts(p), feed, p))
    out.sort(key=lambda t: (t[0], t[1]))
    return out


class ReplayProducer:
    def __init__(self, bootstrap: str, stats: Stats):
        self.stats = stats
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                # Idempotence: the broker de-duplicates retries, so a transient
                # network error can't silently write the same record twice.
                # Without it, "at least once" delivery plus retries means
                # duplicates that look exactly like real repeated observations
                # -- and this feed legitimately repeats trips every poll, so
                # they would be undetectable downstream (pitfall 3.1).
                "enable.idempotence": True,
                # Wait for all in-sync replicas. Implied by idempotence, but
                # stated so the intent survives a config change.
                "acks": "all",
                # Batch briefly. Raises throughput a lot at the cost of a few
                # ms of latency, which replay does not care about.
                "linger.ms": 20,
                "compression.type": "lz4",
                # These payloads are large: one trip_updates poll explodes to
                # ~80k rows. The default 1MB buffer stalls constantly.
                "queue.buffering.max.messages": 500_000,
            }
        )

    def _on_delivery(self, err, msg):
        if err is not None:
            self.stats.delivery_failures += 1
            log.error("delivery failed: %s", err)

    def _dead_letter(self, topic: str, row: dict, exc: Exception, reason: str) -> None:
        """Route a bad record aside instead of crashing or dropping it.

        Dropping loses the evidence needed to diagnose the problem; crashing
        lets one malformed record stop the entire stream. The dead letter keeps
        the pipeline running AND keeps the record inspectable.
        """
        dl = DeadLetter(
            topic=topic,
            reason=reason,
            error=str(exc)[:2000],
            failed_at=datetime.now(timezone.utc).isoformat(),
            # str-coerced so an unserializable value in the bad row cannot
            # itself break the dead-letter write.
            raw={k: (v if isinstance(v, (str, int, float, bool, type(None))) else str(v))
                 for k, v in row.items()},
        )
        self.producer.produce(
            TOPIC_DEAD_LETTER,
            value=dl.model_dump_json().encode(),
            on_delivery=self._on_delivery,
        )
        self.stats.dead_lettered += 1

    def publish_poll(self, path: Path, feed: str, poll_interval_s: int) -> None:
        topic = TOPIC_FOR_FEED[feed]
        raw = gzip.decompress(path.read_bytes())

        try:
            rows, _meta = decode(raw, feed, poll_interval_s)
        except Exception as e:
            # An unparseable payload is a whole-file failure, not a row
            # failure. Record it and move on -- the archive still holds the
            # bytes, so it can be re-examined later.
            self.stats.decode_failures += 1
            log.error("decode failed %s: %s", path.name, e)
            return

        self.stats.polls += 1
        self.stats.rows += len(rows)
        self.stats.by_feed[feed] = self.stats.by_feed.get(feed, 0) + len(rows)

        for row in rows:
            try:
                event = from_decoded_row(row)
            except (ValidationError, ValueError, TypeError) as e:
                self._dead_letter(topic, row, e, "contract_validation_failed")
                continue

            try:
                self.producer.produce(
                    topic,
                    key=key_for(event),
                    value=event.model_dump_json().encode(),
                    on_delivery=self._on_delivery,
                )
                self.stats.published += 1
            except BufferError:
                # Local queue full: let librdkafka drain, then retry once.
                self.producer.poll(1.0)
                self.producer.produce(
                    topic,
                    key=key_for(event),
                    value=event.model_dump_json().encode(),
                    on_delivery=self._on_delivery,
                )
                self.stats.published += 1

        # Serve delivery callbacks without blocking.
        self.producer.poll(0)

    def flush(self, timeout: float = 60.0) -> int:
        return self.producer.flush(timeout)


def main() -> int:
    ap = argparse.ArgumentParser(description="Replay archived GTFS-RT into Kafka")
    ap.add_argument(
        "--source",
        default="data/replay_sample",
        help="archive root to replay (default: the committed sample)",
    )
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument(
        "--feeds", default="trip_updates,vehicle_positions", help="comma separated"
    )
    ap.add_argument(
        "--speed",
        type=float,
        default=0.0,
        help=(
            "replay speed multiplier. 1.0 = real time (2 min between polls), "
            "60 = 1 min of history per second, 0 = as fast as possible"
        ),
    )
    ap.add_argument("--max-polls", type=int, default=0, help="0 = all")
    ap.add_argument("--poll-interval-s", type=int, default=120)
    ap.add_argument("--json", action="store_true", help="emit stats as JSON")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s"
    )

    root = Path(args.source)
    feeds = [f.strip() for f in args.feeds.split(",") if f.strip()]
    polls = discover(root, feeds)
    if not polls:
        log.error("no polls found under %s/raw/ -- nothing to replay", root)
        return 1
    if args.max_polls:
        polls = polls[: args.max_polls]

    span = (polls[-1][0] - polls[0][0]) / 60 if len(polls) > 1 else 0
    log.info(
        "replaying %d polls from %s spanning %.0f min (speed=%s)",
        len(polls),
        root,
        span,
        args.speed or "max",
    )

    stats = Stats()
    rp = ReplayProducer(args.bootstrap, stats)

    started = time.time()
    prev_ts: int | None = None
    for ts, feed, path in polls:
        # Reproduce the original spacing between polls, scaled. At speed=0 we
        # fire everything immediately, which is what tests and the reviewer's
        # quick run want; at speed=1 it behaves like the live feed.
        if args.speed > 0 and prev_ts is not None:
            delay = (ts - prev_ts) / args.speed
            if delay > 0:
                time.sleep(min(delay, 30.0))
        prev_ts = ts

        rp.publish_poll(path, feed, args.poll_interval_s)
        log.info(
            "poll %s feed=%-18s published=%d dead=%d",
            datetime.fromtimestamp(ts).strftime("%H:%M:%S"),
            feed,
            stats.published,
            stats.dead_lettered,
        )

    remaining = rp.flush()
    if remaining:
        log.error("%d messages still queued after flush timeout", remaining)

    elapsed = time.time() - started
    out = stats.as_dict()
    out["elapsed_s"] = round(elapsed, 1)
    out["rows_per_s"] = round(stats.rows / elapsed, 1) if elapsed else 0

    if args.json:
        print(json.dumps(out, indent=2))
    else:
        log.info("done: %s", json.dumps(out))

    # Non-zero exit if anything failed to reach the broker. Dead-lettered rows
    # are NOT a failure -- they are the system working as designed.
    return 1 if (stats.delivery_failures or remaining) else 0


if __name__ == "__main__":
    raise SystemExit(main())
