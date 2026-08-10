"""
511 Regional GTFS-Realtime poller.

Design constraints baked in, each traceable to the pitfall register:

  0.1  Raw bytes are archived BEFORE any decode is attempted. GTFS-RT history
       cannot be re-fetched; if decode logic is wrong we reprocess from the
       archive rather than losing the day.
  0.2  Rate limit is a hard budget, tracked and enforced client-side. Default
       511 allowance is 60 requests/hour per token, shared across feed types.
  1.1  Stale-feed detection via payload hash + feed header timestamp.
  1.2  Parse success is validated; HTTP 200 is not trusted. Failures are
       quarantined, not crashed on.
  1.3  Accept header set explicitly for protobuf.
  10.1 SINGLE INSTANCE ONLY. This process must never be horizontally scaled:
       it is not a Kafka consumer, it has no lag, and running N replicas
       multiplies API calls by N and burns the rate limit. Scale consumers,
       never the poller.

Run:  python -m ingest.poller.poller --once
      python -m ingest.poller.poller            # continuous
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import re
import signal
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import requests

from .decode import decode

log = logging.getLogger("poller")

API_BASE = "https://api.511.org/Transit"
FEEDS = {
    "trip_updates": "TripUpdates",
    "vehicle_positions": "VehiclePositions",
}
AGENCY = os.environ.get("GTFS_AGENCY", "RG")  # RG = consolidated regional feed


class RateBudget:
    """Client-side sliding-window rate limiter.

    511's default is 60 requests per 3600s per token. We enforce it ourselves
    rather than discovering it via 429s, because a throttled token stops the
    archive -- and lost minutes of GTFS-RT are lost permanently.
    """

    def __init__(self, max_requests: int, window_s: int = 3600):
        self.max_requests = max_requests
        self.window_s = window_s
        self._calls: deque[float] = deque()

    def _prune(self, now: float) -> None:
        while self._calls and now - self._calls[0] > self.window_s:
            self._calls.popleft()

    def wait_time(self) -> float:
        now = time.time()
        self._prune(now)
        if len(self._calls) < self.max_requests:
            return 0.0
        return self.window_s - (now - self._calls[0]) + 0.5

    def record(self) -> None:
        self._calls.append(time.time())

    @property
    def remaining(self) -> int:
        self._prune(time.time())
        return self.max_requests - len(self._calls)


class Archive:
    """Append-only raw + decoded storage.

    Layout:

      data/raw/{feed_type}/ingest_dt={YYYY-MM-DD}/{poll_ts}-{sha8}.pb.gz
      data/decoded/{feed_type}/ingest_dt={YYYY-MM-DD}/{poll_ts}-{sha8}.jsonl.gz
      data/quarantine/{feed_type}/...          <- unparseable payloads

    The partition is INGEST date (UTC date of our poll), not GTFS service date.
    This distinction is load-bearing and was originally mislabeled `service_dt`.

    A single payload has no service date. One observed poll contained trips with
    start_date 20260805 (58,971 rows) and 20260806 (263 rows, already past
    midnight), plus 2,858 rows carrying no start_date at all; vehicle_positions
    in the same poll ranged back to 20260730. Service date is a property of a
    ROW, not of a payload, so the raw tier cannot be partitioned by it even in
    principle.

    Ingest date, by contrast, IS a property of the payload -- it is exactly the
    thing we know at write time -- which is why it is the honest partition here.
    Partitioning on true service_date belongs in bronze, downstream of the
    explode, where the grain is one row per StopTimeUpdate. Naming this
    `service_dt` while filling it with the poll clock is pitfall 6.5/6.6 in
    disguise: between 17:00 PT and midnight, every poll lands under tomorrow's
    date and a partition-pruned query silently drops PM peak.
    """

    def __init__(self, root: Path):
        self.root = root

    def _path(self, kind: str, feed_type: str, dt: str, name: str) -> Path:
        p = self.root / kind / feed_type / f"ingest_dt={dt}" / name
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    def write_raw(self, raw: bytes, feed_type: str, dt: str, name: str) -> Path:
        p = self._path("raw", feed_type, dt, f"{name}.pb.gz")
        p.write_bytes(gzip.compress(raw))
        return p

    def write_decoded(self, rows: list[dict], feed_type: str, dt: str, name: str) -> Path:
        p = self._path("decoded", feed_type, dt, f"{name}.jsonl.gz")
        body = "\n".join(json.dumps(r, separators=(",", ":")) for r in rows)
        p.write_bytes(gzip.compress(body.encode()))
        return p

    def quarantine(self, raw: bytes, feed_type: str, dt: str, name: str, err: str) -> Path:
        p = self._path("quarantine", feed_type, dt, f"{name}.bin")
        p.write_bytes(raw)
        p.with_suffix(".error.txt").write_text(err)
        return p


def redact(text: str, secret: str | None) -> str:
    """Remove an API key from text bound for a log.

    Not paranoia -- this leaked for real. `requests` builds the query string
    into the URL, and its exception messages quote that URL in full, so a
    failed fetch logged:

        Max retries exceeded with url: /Transit/VehiclePositions?api_key=<KEY>&agency=RG

    The key then sat in poller.log, which was very nearly committed to a public
    repository. Secrets escape through error paths precisely because error
    paths are the ones nobody rehearses (pitfall 0.5).

    Also redacts any `api_key=...` pattern, so a key belonging to a different
    config (or a rotated one) cannot slip through either.
    """
    if not text:
        return text
    if secret and len(secret) >= 8:
        text = text.replace(secret, "<REDACTED>")
    return re.sub(r"(api_key=)[^&\s\"')]+", r"\1<REDACTED>", text)


class Poller:
    def __init__(
        self,
        api_key: str,
        archive: Archive,
        poll_interval_s: int,
        budget: RateBudget,
        agency: str = AGENCY,
    ):
        self.api_key = api_key
        self.archive = archive
        self.poll_interval_s = poll_interval_s
        self.budget = budget
        self.agency = agency
        self.session = requests.Session()
        # Track last payload hash per feed to detect a stale upstream (1.1)
        self._last_hash: dict[str, str] = {}
        self._last_header_ts: dict[str, int | None] = {}
        self.stats = {
            "polls": 0,
            "stale": 0,
            "quarantined": 0,
            "rows": 0,
            "http_errors": 0,
            # Stale-pool recoveries. Trend this: a rising count means the host
            # is sleeping or the network is flapping, both of which put holes
            # in an archive that cannot be backfilled.
            "conn_retries": 0,
        }

    def fetch(self, feed_type: str) -> bytes:
        """Fetch one feed, retrying a dead pooled connection.

        Why this retry is hand-rolled instead of an urllib3 Retry adapter:
        urllib3 retries BELOW us, inside a single session.get() call. Our rate
        budget records one call per fetch(), so an adapter doing 3 silent
        retries would make 4 requests against 511 while the budget counted 1 --
        under-counting until the token is throttled. That defeats the entire
        point of enforcing the limit client-side (pitfall 0.2). Retrying here,
        where every attempt goes through budget.record(), keeps the count
        honest.

        Only CONNECTION failures are retried. An HTTP error is a real answer
        from 511 and gets one attempt; retrying a 429 or a 500 just burns
        budget against a server that already said no.

        Observed 2026-08-05/06: after the host sleeps, the first request on the
        pooled socket dies with ConnectionResetError. Because trip_updates is
        polled first each cycle, it absorbed the stale connection every time --
        9 of 9 failures were trip_updates, and vehicle_positions succeeded on
        the fresh socket that followed. Every wake silently cost one poll of
        the more valuable feed.
        """
        url = f"{API_BASE}/{FEEDS[feed_type]}"
        last_exc: Exception | None = None

        for attempt in (1, 2):
            wait = self.budget.wait_time()
            if wait > 0:
                log.warning("rate budget exhausted, sleeping %.0fs", wait)
                time.sleep(wait)

            try:
                resp = self.session.get(
                    url,
                    params={"api_key": self.api_key, "agency": self.agency},
                    # Pitfall 1.3 -- required, or the response format is not
                    # guaranteed
                    headers={"Accept": "application/x-google-protobuf"},
                    timeout=30,
                )
            except (requests.ConnectionError, requests.Timeout) as e:
                self.budget.record()  # the attempt left our machine; count it
                last_exc = e
                self.stats["conn_retries"] += 1
                if attempt == 1:
                    # Drop the pool entirely. Reusing this Session would hand
                    # back the same dead socket.
                    log.warning(
                        "connection failed feed=%s (%s) -- new session, retrying",
                        feed_type,
                        type(e).__name__,
                    )  # type name only; the exception text carries the URL+key
                    self.session.close()
                    self.session = requests.Session()
                    continue
                raise

            self.budget.record()
            resp.raise_for_status()
            return resp.content

        raise last_exc  # unreachable; loop either returns or raises

    def poll_once(self, feed_type: str) -> dict:
        """One poll cycle. Archive-first, decode-second, never lose bytes."""
        now = datetime.now(timezone.utc)
        # UTC date of THIS POLL. Not the service date -- see Archive's docstring.
        ingest_dt = now.strftime("%Y-%m-%d")

        try:
            raw = self.fetch(feed_type)
        except requests.RequestException as e:
            self.stats["http_errors"] += 1
            # str(e) embeds the request URL, which carries api_key=. Redact
            # before it reaches the log OR the returned dict -- the caller
            # prints that dict to stdout under --once.
            safe = redact(str(e), self.api_key)
            log.error("fetch failed feed=%s: %s", feed_type, safe)
            return {"ok": False, "reason": "http", "error": safe}

        import hashlib

        sha = hashlib.sha256(raw).hexdigest()
        name = f"{int(now.timestamp())}-{sha[:8]}"

        # ---- ARCHIVE FIRST. Everything below can fail without losing data.
        raw_path = self.archive.write_raw(raw, feed_type, ingest_dt, name)
        self.stats["polls"] += 1

        # ---- Now decode. Failure here is recoverable from the archive.
        try:
            rows, meta = decode(raw, feed_type, self.poll_interval_s)
        except Exception as e:  # DecodeError, or an HTML body served as 200
            self.stats["quarantined"] += 1
            self.archive.quarantine(raw, feed_type, ingest_dt, name, repr(e))
            log.error(
                "decode failed feed=%s bytes=%d head=%r -- quarantined",
                feed_type,
                len(raw),
                raw[:80],
            )
            return {"ok": False, "reason": "decode", "raw_path": str(raw_path)}

        # ---- Stale feed detection (1.1)
        is_stale = self._last_hash.get(feed_type) == sha
        header_stalled = (
            meta.feed_header_ts_epoch is not None
            and self._last_header_ts.get(feed_type) == meta.feed_header_ts_epoch
        )
        if is_stale or header_stalled:
            self.stats["stale"] += 1
            log.warning(
                "stale feed=%s (identical_payload=%s header_ts_unchanged=%s) "
                "-- archived but flagged",
                feed_type,
                is_stale,
                header_stalled,
            )
        self._last_hash[feed_type] = sha
        self._last_header_ts[feed_type] = meta.feed_header_ts_epoch

        self.archive.write_decoded(rows, feed_type, ingest_dt, name)
        self.stats["rows"] += len(rows)

        log.info(
            "feed=%s rows=%d entities_hash=%s header_ts=%s stale=%s budget_left=%d",
            feed_type,
            len(rows),
            sha[:8],
            meta.feed_header_ts,
            is_stale or header_stalled,
            self.budget.remaining,
        )
        return {
            "ok": True,
            "rows": len(rows),
            "stale": is_stale or header_stalled,
            "raw_path": str(raw_path),
        }

    def run_forever(self, feed_types: list[str]) -> None:
        stop = {"flag": False}

        def _handle(signum, frame):
            log.info("signal %s received, finishing current cycle", signum)
            stop["flag"] = True

        signal.signal(signal.SIGINT, _handle)
        signal.signal(signal.SIGTERM, _handle)

        log.info(
            "poller starting: agency=%s feeds=%s interval=%ds budget=%d/hr",
            self.agency,
            feed_types,
            self.poll_interval_s,
            self.budget.max_requests,
        )
        while not stop["flag"]:
            cycle_start = time.time()
            for ft in feed_types:
                if stop["flag"]:
                    break
                self.poll_once(ft)
            elapsed = time.time() - cycle_start
            sleep_for = max(0.0, self.poll_interval_s - elapsed)
            slept = 0.0
            while slept < sleep_for and not stop["flag"]:
                time.sleep(min(1.0, sleep_for - slept))
                slept += 1.0
        log.info("poller stopped. stats=%s", self.stats)


def main() -> int:
    ap = argparse.ArgumentParser(description="511 GTFS-RT archiving poller")
    ap.add_argument("--once", action="store_true", help="single poll cycle then exit")
    ap.add_argument(
        "--feeds",
        default="trip_updates,vehicle_positions",
        help="comma-separated feed types",
    )
    ap.add_argument("--data-root", default="data", help="archive root directory")
    ap.add_argument(
        "--interval",
        type=int,
        default=int(os.environ.get("POLL_INTERVAL_S", "120")),
        help="seconds between poll cycles (this is your label noise floor)",
    )
    ap.add_argument(
        "--rate-limit",
        type=int,
        default=int(os.environ.get("RATE_LIMIT_PER_HOUR", "60")),
        help="requests per hour allowed by your 511 token",
    )
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
    )

    api_key = os.environ.get("API_511_KEY")
    if not api_key:
        log.error("API_511_KEY not set. Copy .env.example to .env and fill it in.")
        return 2

    feed_types = [f.strip() for f in args.feeds.split(",") if f.strip()]

    # Sanity check the cadence against the budget, because getting this wrong
    # silently degrades every arrival label in the project.
    cycles_per_hour = 3600 / args.interval
    needed = cycles_per_hour * len(feed_types)
    if needed > args.rate_limit:
        max_cycles = args.rate_limit / len(feed_types)
        suggested = int(3600 / max_cycles) + 1
        log.error(
            "cadence exceeds budget: %d feeds every %ds needs %.0f req/hr, "
            "have %d. Use --interval %d or request a limit increase.",
            len(feed_types),
            args.interval,
            needed,
            args.rate_limit,
            suggested,
        )
        return 2

    log.info(
        "label noise floor: +/-%ds (poll interval). Budget use: %.0f/%d req per hour.",
        args.interval,
        needed,
        args.rate_limit,
    )

    poller = Poller(
        api_key=api_key,
        archive=Archive(Path(args.data_root)),
        poll_interval_s=args.interval,
        budget=RateBudget(args.rate_limit),
    )

    if args.once:
        results = [poller.poll_once(ft) for ft in feed_types]
        print(json.dumps({"results": results, "stats": poller.stats}, indent=2))
        return 0 if all(r.get("ok") for r in results) else 1

    poller.run_forever(feed_types)
    return 0


if __name__ == "__main__":
    sys.exit(main())
