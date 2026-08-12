# Data Source

> **Data provided by [511.org](http://www.511.org)** — Metropolitan Transportation Commission.

---

## 1. Source

| | |
|---|---|
| **Name** | 511 SF Bay Open Data — GTFS-Realtime transit feeds |
| **Owner** | Metropolitan Transportation Commission (MTC), operator of the 511 service |
| **Portal** | https://511.org/open-data/transit |
| **Token request** | https://511.org/open-data/token |
| **Contact** | developerresources@511.org |
| **Format** | GTFS-Realtime (Protocol Buffers, proto2) |
| **Coverage** | All Bay Area transit operators in one consolidated regional feed |

MTC operates 511 in partnership with Caltrans District 4, the CHP Golden Gate
Division, and the Bay Area transit agencies (the "Data Suppliers"). The feeds
are republished from each operator's own AVL and prediction systems, which is
why field population varies so sharply between operators — see §6.

### Endpoints used

```
https://api.511.org/Transit/TripUpdates?api_key=<KEY>&agency=RG
https://api.511.org/Transit/VehiclePositions?api_key=<KEY>&agency=RG
```

`agency=RG` is the **consolidated regional feed**: one request returns every
participating operator. This is a deliberate choice — with a 60 request/hour
budget, per-operator polling would make a useful cadence impossible.

A third feed, `ServiceAlerts`, exists and is **not** used by this project.

---

## 2. Access and credentials

- **Free.** Register an email address at the token URL above; the key arrives
  immediately.
- Supplied as an `api_key` query parameter on every request.
- **No credentials are committed to this repository.** The key lives in `.env`,
  which is gitignored. `.env.example` documents the required variables with
  empty values.
- Logs are also excluded (`*.log`). This is not routine caution: `requests`
  embeds query parameters in exception messages, so a failed fetch wrote the
  full URL — including the key — into `poller.log`. The key is now redacted at
  source (`redact()` in `ingest/poller/poller.py`) and covered by tests in
  `tests/test_poller_fetch.py`.

**A reviewer needs no key.** The committed replay sample (§5) drives the entire
pipeline offline.

---

## 3. Rate limits

| | |
|---|---|
| **Default quota** | 60 requests per 3,600 seconds, per token |
| **Our cadence** | one cycle / 120 s × 2 feeds = exactly 60 req/hour |
| **Headroom** | none |
| **Increase** | written request to developerresources@511.org (do not include your key) |

The budget is enforced **client-side** by a sliding-window limiter
(`RateBudget` in `ingest/poller/poller.py`) rather than by reacting to HTTP 429s.
The reasoning: a throttled token stops the archive, and GTFS-Realtime history
cannot be re-fetched, so minutes lost to throttling are lost permanently.

This has a direct effect on data quality. The poll interval is the **noise floor
of every derived arrival time**: an arrival can be no more precise than the
interval that observed it. It is therefore recorded on every archived row as
`poll_interval_s`, so a future cadence change does not silently invalidate older
data.

---

## 4. Rights and terms of use

Governed by the **511 Data Disseminator Agreement** (`511_Data_Agreement_Final_2026.pdf`,
linked from the open-data portal), accepted at token registration. The clauses
that constrain this project:

**§1 — Grant of license.** A "nonexclusive, royalty-free, worldwide,
non-transferable license to use, sublicense, copy, distribute, and store
(electronically or otherwise) the Provided Data; to create derivative works…; to
display (publicly or otherwise)…". Archiving, processing, and publishing derived
results are all clearly permitted.

**§2(c) — Sublicensing requires accepted terms.** The disseminator must "secure
from prospective sublicensees their written acceptance of these terms and
conditions **prior to providing the Provided Data**."

> **This is why this repository is private.** `data/replay_sample/` contains
> verbatim 511 protobuf payloads — the Provided Data itself, not a derivative
> work. Publishing them openly would hand raw feed data to anyone, with no
> acceptance secured. Course reviewers are granted access individually, which
> keeps distribution controlled.

**§2(b)** — the data may not be sold as received on a standalone basis. Not
applicable; this is unpaid academic work.

**§2(e), §5(d)** — Data Suppliers' trademarks and logos may not be used without
their permission. No operator logos or marks appear anywhere in this project.

**§2(g)** — a disseminator must provide MTC with documentation of any product,
site, or service using the data within 30 days of launch. *Outstanding action:
notify developerresources@511.org if this work is published or demonstrated
beyond course assessment.*

**§3 — Disclaimers.** Data is furnished "AS IS," "AS AVAILABLE" and "WITH ALL
FAULTS." MTC accepts no responsibility for accuracy, completeness, reliability,
or timeliness. Any reliability figure produced here is a measurement of the
feed, not a warranted statement about transit performance.

**§5(b) — Attribution is mandatory.** The source must be acknowledged with
"powered by 511.org" or "data provided by 511.org," plus a link to
http://www.511.org, in visual proximity to the user's access to the data.
Attribution appears at the top of this file, in `README.md`, and in
`outputs/prediction_error_summary.json`.

**§5(c)** — "511," "511.org," or combinations thereof may not be used in the
naming or branding of a data-dissemination product. This project is named
`transit-lakehouse`.

**§8** — governed by California law.

---

## 5. Replay data included in this repository

| | |
|---|---|
| **Path** | `data/replay_sample/` |
| **Contents** | 20 TripUpdates polls + 19 VehiclePositions polls |
| **Window** | 2026-08-06, 14:52–15:32 local (39 contiguous minutes) |
| **Size** | 13 MB compressed |
| **Format** | raw `.pb.gz` — original protobuf bytes, exactly as received |
| **Manifest** | `data/replay_sample/MANIFEST.json` |
| **Generated by** | `scripts/make_replay_sample.py` |

Three properties of this sample are deliberate:

**Raw, not decoded.** Shipping pre-decoded JSON would be smaller and simpler —
and would bypass `ingest/poller/decode.py`, the most correctness-critical file in
the repository, so a reviewer would never exercise the absent-vs-zero presence
logic the project is built around. The sample enters the pipeline through the
same door live data does.

**Contiguous, not sampled.** Arrival detection works by observing a vehicle move
through states across *consecutive* polls. A random scatter of polls would
destroy those sequences and derive nothing. Contiguity is a correctness
requirement, not a convenience.

**Gap-checked.** The generator refuses windows containing gaps longer than five
minutes, because a hole produces missed arrivals that look identical to "this
operator does not report arrivals."

**No personal information.** GTFS-Realtime contains vehicle and trip
identifiers, positions, and schedule adherence. It contains no passenger or
personally identifying data. Vehicle IDs are fleet numbers, already public on the
side of every bus.

### The live archive (not committed)

`data/` holds ~1.3 GB accumulated since 2026-08-05 and is gitignored — too large
for version control, and covered by the §2(c) reasoning above. It is regenerable
only going forward: GTFS-Realtime has no history endpoint, which is the single
fact that shaped this project's architecture.

---

## 6. Schema

### TripUpdates — agency predictions

One `FeedMessage` per poll → many entities → one row per `StopTimeUpdate`
(fan-out is roughly 1 : 30). Key fields after decoding:

| Field | Type | Notes |
|---|---|---|
| `trip_id`, `route_id` | string | prefixed `AGENCY:` in the regional feed |
| `service_date` | `YYYYMMDD` | from `TripDescriptor.start_date`; **never** derived from a timestamp |
| `stop_sequence` | int | **part of the grain**; authoritative over `stop_id` |
| `stop_id` | string | an attribute, not an identity |
| `arrival_time_epoch` | int | predicted arrival, POSIX seconds |
| `arrival_delay_s` | int **or null** | null = no information; 0 = exactly on time |
| `arrival_uncertainty` | int or null | 0 means "observed, not predicted" |
| `trip_update_ts_epoch` | int | when the producer computed this update |
| `schedule_relationship` | enum name | `SCHEDULED` / `SKIPPED` / `CANCELED` / `ADDED` |

### VehiclePositions — GPS observations

| Field | Type | Notes |
|---|---|---|
| `vehicle_id` | string | fleet number |
| `vehicle_report_ts_epoch` | int | **event time** — measured on board |
| `latitude`, `longitude` | float | WGS84 |
| `current_stop_sequence` | int or null | which stop `current_status` refers to |
| `current_status` | enum name | `INCOMING_AT` / `STOPPED_AT` / `IN_TRANSIT_TO` |
| `occupancy_status` | enum name or null | rarely populated |

### Envelope stamped on every row

`feed_header_ts`, `ingest_ts`, `payload_sha256`, `poll_interval_s`,
`decoder_version`, `protobuf_runtime`, `contract_version`.

`ingest_ts` is recorded separately from all producer timestamps and is **never**
used as event time.

Full contract: `streaming/contracts.py`. Decoder: `ingest/poller/decode.py`.

---

## 7. Limitations — measured, not assumed

Every figure below comes from profiling the live feed
(`profiling/profile_feed.py`). These are properties of the source, and they
shaped the design.

**GTFS-Realtime contains no actual arrival times.** It publishes predictions and
positions. The fact "vehicle reached stop X at time T" must be derived. Since
delay = actual − scheduled and scheduled is exact, every error in that derivation
lands directly in the result.

**The regional feed carries 28 operators**, not the four commonly named.

**44.2% of StopTimeUpdate rows carry no delay value at all** (54,638 of 123,735
in one sample). Nineteen of 28 operators never populate it — AC Transit produced
23,469 consecutive rows with delay absent. Read naively, protobuf returns `0` for
both "absent" and "on time," which would fabricate a large spike of on-time
records that looks entirely plausible.

**Only 15 of 28 operators publish `current_status`,** the field our arrival
resolver depends on. Muni populates it 99.2% of the time; Caltrain, 0%. Resolver
availability is therefore agency-correlated, which is why every derived arrival
records the method that produced it.

**Settled predictions are effectively unavailable** — only 4 of 28 operators emit
`uncertainty = 0`, at trivial volume. Muni never publishes uncertainty at all.

**Seven operators run trips that revisit a `stop_id`** (Emery Go-Round: 23 of 29
trips). This is why the grain is `(service_date, trip_id, stop_sequence)`.

**3.0% of derived arrivals carry no `service_date`,** concentrated in three small
operators. Muni is clean.

**A single payload spans multiple service dates.** One observed poll contained
trips dated 20260805 (58,971 rows), 20260806 (263 rows, already past midnight),
and 2,858 rows with no date; vehicle positions in the same payload reached back
eight days. Service date is a property of a *row*, not of a payload — which is
why the raw archive partitions on `ingest_dt`.

**Sampling limits coverage to roughly 21% of stop events.** Vehicles dwell at
stops for seconds while we sample every 120 seconds; 90.3% of captured stop
events appear in exactly one poll. Two consequences: derived arrivals are biased
*late* (a vehicle is observable as `STOPPED_AT` only between arriving and
departing), and long-dwell stops — terminals, layovers, timepoints — are
over-represented, since capture probability scales with dwell. Median observed
dwell among multi-poll sightings is 353 seconds. **Any bias figure derived from
this data is an upper bound, not a point estimate.**

**Feed availability is not guaranteed** (§3 of the agreement). Observed during
collection: transient connection resets, and one operator dropping out of the
feed entirely between polls.

---

## 8. Reproducing the data

```bash
# Offline — no key required. This is the review path.
make demo

# Live collection — requires a key in .env
cp .env.example .env      # paste your 511 key
make poll-bg              # supervised archiver, single instance
make poll-status

# Regenerate the committed sample from a live archive
make replay-sample
```

The poller is deliberately restricted to **one instance**, enforced by a lock
file. It is not a Kafka consumer and has no work to divide; two copies simply
double the API calls against a 60/hour budget and get the token throttled.
