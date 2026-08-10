"""Tests for fetch-level connection recovery.

Regression cover for the 2026-08-05/06 archive gap: after the host slept, the
first request on the pooled socket died with ConnectionResetError and the poll
was lost. Because trip_updates is polled first each cycle, it absorbed that
failure every time -- 9 of 9 observed failures were trip_updates.
"""

import sys
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingest.poller.poller import Archive, Poller, RateBudget


class _Resp:
    def __init__(self, content=b"\x00", status=200):
        self.content = content
        self.status = status

    def raise_for_status(self):
        if self.status >= 400:
            raise requests.HTTPError(f"status {self.status}")


class _Session:
    """Session whose first N gets raise, as a dead pooled socket would."""

    def __init__(self, fail_times=0, exc=None):
        self.fail_times = fail_times
        self.exc = exc or requests.ConnectionError("Connection reset by peer")
        self.calls = 0
        self.closed = 0

    def get(self, *a, **kw):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise self.exc
        return _Resp()

    def close(self):
        self.closed += 1


def _poller(tmp_path, session, limit=60):
    p = Poller(
        api_key="k",
        archive=Archive(tmp_path),
        poll_interval_s=120,
        budget=RateBudget(limit),
    )
    p.session = session
    return p


def test_stale_connection_is_retried(tmp_path, monkeypatch):
    """One dead pooled connection must not cost a poll."""
    s = _Session(fail_times=1)
    p = _poller(tmp_path, s)
    monkeypatch.setattr(requests, "Session", lambda: _Session(fail_times=0))

    assert p.fetch("trip_updates") == b"\x00"
    assert s.closed == 1, "stale session must be closed, not reused"
    assert p.stats["conn_retries"] == 1


def test_retry_attempts_are_charged_to_the_rate_budget(tmp_path, monkeypatch):
    """Both attempts count. An adapter-level retry would record only one.

    Under-counting is how a client-side budget silently overruns the real
    511 limit and gets the token throttled -- which stops the archive.
    """
    s = _Session(fail_times=1)
    p = _poller(tmp_path, s)
    monkeypatch.setattr(requests, "Session", lambda: _Session(fail_times=0))

    before = p.budget.remaining
    p.fetch("trip_updates")
    assert before - p.budget.remaining == 2, "failed attempt must consume budget"


def test_second_connection_failure_gives_up(tmp_path, monkeypatch):
    """Retry once, not forever -- an unreachable network must not spin."""
    s = _Session(fail_times=2)
    p = _poller(tmp_path, s)
    monkeypatch.setattr(requests, "Session", lambda: _Session(fail_times=2))

    with pytest.raises(requests.ConnectionError):
        p.fetch("trip_updates")
    assert p.stats["conn_retries"] == 2


def test_http_errors_are_not_retried(tmp_path):
    """A 429/500 is a real answer. Retrying burns budget against a 'no'."""

    class _ErrSession(_Session):
        def get(self, *a, **kw):
            self.calls += 1
            return _Resp(status=429)

    s = _ErrSession()
    p = _poller(tmp_path, s)
    with pytest.raises(requests.HTTPError):
        p.fetch("trip_updates")
    assert s.calls == 1, "HTTP errors must get exactly one attempt"


# --------------------------------------------------------------------------
# Secret redaction
# --------------------------------------------------------------------------

def test_api_key_is_redacted_from_error_text():
    """Regression: the key leaked into poller.log via a requests exception.

    `requests` builds query parameters into the URL, and its exception messages
    quote that URL verbatim -- so a DNS failure logged
    `...?api_key=<KEY>&agency=RG`. The log was staged for a public push before
    this was caught.
    """
    from ingest.poller.poller import redact

    key = "abcd1234-ef56-7890-abcd-1234567890ab"
    msg = (
        "HTTPSConnectionPool(host='api.511.org', port=443): Max retries "
        f"exceeded with url: /Transit/VehiclePositions?api_key={key}&agency=RG"
    )
    out = redact(msg, key)
    assert key not in out
    assert "<REDACTED>" in out
    assert "api.511.org" in out, "redaction must not destroy diagnostic value"


def test_api_key_pattern_redacted_even_when_secret_unknown():
    """A rotated or foreign key must not slip through."""
    from ingest.poller.poller import redact

    msg = "url: /Transit/TripUpdates?api_key=some-other-key-value&agency=RG"
    out = redact(msg, None)
    assert "some-other-key-value" not in out
    assert "<REDACTED>" in out


def test_redaction_ignores_implausibly_short_secrets():
    """A 3-character 'secret' would otherwise scribble over normal text."""
    from ingest.poller.poller import redact

    assert redact("agency=RG and stop=RG5", "RG") == "agency=RG and stop=RG5"


def test_http_error_dict_is_redacted(tmp_path):
    """--once prints the returned dict to stdout, so it needs redaction too."""
    from ingest.poller.poller import Archive, Poller, RateBudget

    key = "zzzz1234-ef56-7890-abcd-1234567890ab"

    class _Boom:
        def get(self, *a, **kw):
            raise requests.ConnectionError(
                f"Max retries exceeded with url: /Transit/TripUpdates?api_key={key}"
            )

        def close(self):
            pass

    p = Poller(api_key=key, archive=Archive(tmp_path), poll_interval_s=120,
               budget=RateBudget(60))
    p.session = _Boom()
    res = p.poll_once("trip_updates")
    assert res["ok"] is False
    assert key not in res["error"], "API key returned to caller in error dict"
    assert "<REDACTED>" in res["error"]
