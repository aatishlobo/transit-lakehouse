# Session Report — 5 August 2026

**Project:** transit-lakehouse (DE#1) · **Session:** ~18:00–19:20 PDT, plus an unattended overnight run

This report explains everything that happened, assuming no prior knowledge of
the tools involved. Technical terms are explained the first time they appear,
and there is a glossary at the end.

---

## 1. What this project is trying to do

Imagine you want to answer a simple question: **"Is the 38 bus usually late?"**

You would think transit agencies just publish this. They don't. What they
publish is a live feed of *predictions* ("the bus will arrive at 3:07") and
*GPS pings* ("bus #4821 is at this latitude/longitude right now"). Nowhere does
any agency publish the sentence *"bus #4821 actually arrived at stop 22 at
3:09."*

That fact — the actual arrival — has to be **derived** by us, from indirect
evidence. And here is why that's the hard part of the whole project:

> **Delay = actual arrival − scheduled arrival.**
> The scheduled time is exact and published. So *every* mistake in our derived
> "actual" lands directly in the final delay number. There is nothing
> downstream that can correct it.

This project builds the machinery to derive those arrivals carefully, store
them so they can be trusted later, and measure on-time performance across Bay
Area transit.

**Where the data comes from.** 511.org (run by the Metropolitan Transportation
Commission) publishes a single combined feed covering every Bay Area transit
operator. You request it over the internet with a key, and it answers with a
blob of data.

---

## 2. Where things stood at the start

The project already had working code from a previous session — about 1,400
lines and 11 tests. But nothing ran. Three separate problems:

**Problem A: the folder structure had been flattened.** Python code is
organized into *packages* — folders that group related files, a bit like
chapters in a book. The code expected this arrangement:

```
ingest/poller/decode.py
ingest/poller/poller.py
profiling/profile_feed.py
tests/test_decode.py
```

but every file was sitting loose in one folder instead. The files contained
instructions like "import the decoder from `ingest.poller`," and since that
folder didn't exist, Python responded with `ModuleNotFoundError: No module named
'ingest'`. Nothing could start.

**Problem B: supporting files were missing.** The instructions referenced a
`.env.example` (a template for storing your secret API key), a `.gitignore`, and
a `docs/` folder. None existed.

**Problem C: the Python environment was borrowed from another project.** A
*virtual environment* (`.venv`) is a private toolbox for one project, holding
the exact versions of the libraries it needs — so that upgrading something for
project A can't break project B. This one had been built inside a folder called
`muni_tracker` and then copied over. Some of its shortcuts still pointed at the
old location and failed with `bad interpreter`.

### What was done

You provided a `files.zip` from a previous session containing the correct
structure plus the missing files. Before overwriting anything, every file was
compared byte-for-byte against what was already on disk. The only differences
were trailing blank lines — so nothing of yours was at risk. The archive was
extracted into place and the duplicate loose files removed.

**Verification:** all 11 tests passed, and a full practice run using fake data
produced a complete report. The code had been fine all along; only its packaging
was broken.

---

## 3. A conflict between the project and the course requirements

**What `.gitignore` is.** Git is version-control software — it tracks every
change to your code so you can undo mistakes and see history. A `.gitignore`
file lists things Git should *ignore*: passwords, temporary files, and huge data
files that would bloat the repository.

The existing `.gitignore` excluded the entire `data/` folder along with every
`.pb.gz` and `.jsonl.gz` file — sensible, because that data grows without limit.

**The conflict:** your course requires the submitted ZIP to *contain* sample
data, so the grader can run your pipeline without an API key. The current rules
guaranteed that data would be excluded.

**The fix, and why it's fiddly.** The obvious approach — "ignore `data/`, except
`data/replay_sample/`" — silently fails. Git does not look *inside* a folder it
has been told to ignore, so it never sees the exception. Each level of the path
has to be re-included in order:

```gitignore
data/                              # ignore the archive
!data/                             # ...but descend into it
!data/replay_sample/               # ...and into the sample
!data/replay_sample/**             # ...and keep everything under it
```

This was tested in a scratch repository with dummy files to confirm real archive
files are excluded and sample files are kept. It works.

---

## 4. Going live

You obtained a 511 API key. **An API key** is a password-like string that
identifies you to a service, so it can tell users apart and enforce limits. It
was placed in a `.env` file — a local file holding secrets, which `.gitignore`
excludes so it never gets committed or submitted. Your course explicitly forbids
credentials in the submission.

The first real request to 511 succeeded immediately:

```
feed=trip_updates      rows=62092  header_ts=2026-08-06T02:06:57+00:00
feed=vehicle_positions rows=1927
```

**What just happened, in detail:**

1. The program sent a web request to 511's servers with your key attached.
2. 511 replied with roughly 474 KB of **Protocol Buffers** data. Protobuf is a
   compact binary format — think of it as a zip file for structured data. It's
   far smaller than text, but unreadable without the matching *schema* (a
   definition of what each field means). The `gtfs-realtime-bindings` library
   supplies that schema.
3. Those raw bytes were written to disk **before** any attempt to interpret
   them.
4. Only then were they decoded into 62,092 individual records.

### Design choice: save the raw bytes first

This ordering is deliberate and worth understanding.

The intuitive pipeline is *fetch → interpret → save the result*. This one saves
the untouched original first, then interprets it as a separate step that is
allowed to fail.

**Why:** the two failure modes are not equally recoverable. If the interpreting
code has a bug discovered next week, the original bytes are still on disk and
everything can be re-processed. But **a moment of transit data never fetched is
gone forever** — 511 publishes only what is happening *now*; there is no way to
ask for last Tuesday at 5pm.

*The principle: make the irreversible step the cheap one.* The cost is roughly
double the storage and a "quarantine" folder for payloads that fail to decode.

---

## 5. A bug found within minutes of going live

The first poll was saved to this location:

```
data/raw/trip_updates/service_dt=2026-08-06/...
```

Something was wrong. It was 19:06 on **August 5th** in California, but the
folder said August 6th.

**Why folders have dates in their names.** Storing millions of files in one
folder is unusable. Splitting them into dated folders — called **partitioning**
— means a query for "August 3rd" can skip every other folder entirely. It's the
difference between reading one drawer and reading the whole filing cabinet.

**The cause.** The code built the folder name from the clock, converted to UTC
(a global reference timezone, 7 hours ahead of California in summer):

```python
now = datetime.now(timezone.utc)
dt = now.strftime("%Y-%m-%d")     # 19:06 PDT Aug 5 = 02:06 UTC Aug 6
```

The folder was labeled `service_dt` — *service date*, meaning the transit
service day the trips belong to — but was actually filled with the *date we
happened to download it*, in a timezone 7 hours off. **Every poll between 5pm
and midnight California time, seven hours daily including the entire evening
rush, was filed under tomorrow.** Anyone later querying "August 5th" would
silently get no evening data. No error, no warning — just a quietly wrong
answer.

### The deeper discovery

The obvious fix is "use the real service date instead." Checking the actual data
showed that's impossible. One single download contained:

| Service date in the data | Rows |
|---|---|
| 20260805 | 58,971 |
| 20260806 (trips already past midnight) | 263 |
| *no date at all* | 2,858 |

Vehicle positions in the same file went back as far as July 30th — stale ghost
vehicles still being reported.

> **A downloaded file does not have one service date. Service date belongs to
> each individual row inside it.**

So the raw archive cannot be organized by service date *even in principle*.

**The resolution:**

- **Raw files** → organized by `ingest_dt` — the date *we downloaded it*. That
  genuinely is a property of the file, and it's exactly what we know at the
  moment of writing.
- **Later processing stages** → organized by true service date, once the data
  has been split into individual rows where the question actually has an answer.

The rename was applied across the code, the four existing folders, and the
documentation. Doing it immediately mattered: at 3 files it was trivial; after a
week of archiving it becomes a migration you'd be tempted to skip — which is how
a mislabel becomes permanent.

---

## 6. What the live data revealed

The profiler was run against real data. It answers seven questions that later
design decisions depend on. Three results stood out.

### Finding 1: 44.2% of records have no delay information — and that's a trap

**The single most important concept in this project.**

The data format (`protobuf` version 2) distinguishes between a field that says
*"delay is 0 seconds"* and a field that is **absent entirely**, meaning *"we
have no information."*

An analogy: on a paper form, a box containing "0" and a box left blank mean
completely different things. The first is an answer; the second is a
non-answer.

The trap is that the programming library **returns `0` for both.** Reading the
value the natural way gives you `0` whether the agency said "on time" or said
nothing at all. The only way to tell them apart is to explicitly ask "was this
field actually filled in?" — a function called `HasField()`.

Measured on your real data:

| | |
|---|---|
| Total records | 123,735 |
| Records with **no** delay information | **54,638 (44.2%)** |
| Operators that never fill it in | **19 of 28** |

AC Transit sent 23,469 records with delay absent in every single one. Santa
Clara VTA, 16,217 — all absent.

**What this means:** software written the natural, obvious way would have
reported all 54,638 of those as *exactly on time*. Your regional on-time
statistic would have been dominated by a fabricated spike of perfect
performance. It would have looked entirely plausible, and every number built on
top of it — every chart, every model — would have been wrong, with nothing
downstream capable of detecting it.

The existing code handles this correctly. This measurement is what proves the
precaution was worth taking, with a real number attached.

### Finding 2: the feed has 28 operators, not 4

Project documentation described "BART, Muni, AC Transit, Caltrain." The live
feed carries **28** operators. This matters for scope: the guidance in the
project's own notes is to get *one* operator working end-to-end before adding
others.

### Finding 3: the operators are wildly inconsistent

Recall there are three possible ways to derive an actual arrival:

| Method | How it works | Weakness |
|---|---|---|
| **A — Vehicle status** | The bus reports "I am STOPPED_AT stop 14." The agency's own system decided. | Only some agencies send it |
| **B — Geofence** | Find where the GPS trail came closest to the stop. | Measures *closest approach*, not stopping — biased early |
| **C — Prediction settling** | Take the last prediction before the bus passed, treat it as fact. | Inherits the agency's own prediction errors |

Measured:

- **Method A works for 15 of 28 operators.** Muni fills the needed fields 99.2%
  of the time; Caltrain, 0%.
- **Method C is nearly dead** — only 4 of 28 operators, at trivial volume.

**Why this is dangerous, and the design response.** If we simply used whichever
method was available per agency and recorded only the final answer, then
*which agency* a bus belongs to would secretly determine *how accurate its
arrival time is*. Any later analysis would find "agency effects" that are really
just artifacts of our own code.

The design response is to record, on every single row, **which method produced
it**, plus a confidence level and — where two methods both fired — how far apart
they were. This makes the uncertainty visible and filterable instead of hidden.
As a bonus, when two methods disagree consistently, that disagreement measures
the bias of the weaker one for free.

### Finding 4: loop routes are real

Some routes visit the same stop twice in one trip. If records were identified by
stop *name*, those two visits would collapse into one and an arrival would
vanish.

Measured: **7 operators** have this. Emery Go-Round: 23 of 29 trips. MVgo: 6 of
6. This confirms records must be identified by **stop sequence number** (stop
#1, #2, #3…) rather than stop name. Previously an assumption; now evidence.

### Volume

62,092 records per download, every 2 minutes: roughly **46 million records and
1.4 GB per day**.

---

## 7. Making it survive unattended

### A supervisor

A script was written that restarts the poller if it crashes, with an increasing
delay between attempts. **Why the delay increases:** a program that crashes
instantly and restarts instantly becomes a runaway loop hammering 511's servers
— precisely the behavior the rate limit exists to prevent.

### Enforcing "only one copy" mechanically

Project rules state the poller must never run as more than one copy. The reason
is counterintuitive: normally, running more copies of a program handles more
load. Here it does the opposite — two copies make *twice* the requests against a
budget of 60 per hour, exhausting it and getting your key cut off. There is no
work to divide; both copies would fetch the same thing.

This rule existed only as a comment. It is now enforced with a **lock file**: on
startup the script records its ID, and a second attempt is refused. *A rule you
can break by double-clicking is not a control.*

### Two dangerous shortcuts removed

The `Makefile` (a file of named shortcuts, so `make test` runs a longer command)
contained two commands that deleted the entire archive:

- `make clean` — the most reflexively typed command in any project.
- `make fixture` — generating *practice* data wiped the *real* archive.

The second is the alarming one: running a harmless offline demo would have
destroyed irreplaceable data. Practice data now writes to a separate folder, and
deleting the archive requires typing `make clean-archive CONFIRM=yes`.

---

## 8. What happened overnight

The poller was left running at 19:16. Results:

| | |
|---|---|
| Time elapsed | 17h 33m |
| Downloads expected | ~527 per feed |
| Downloads achieved | 32 and 41 |
| **Coverage** | **8.9% / 11.4%** |

**Cause 1 — the laptop slept.** It was never plugged in. macOS put it to sleep
repeatedly and the program froze with it. From 19:08 to 20:07, while awake, it
ran flawlessly: 26 downloads at exact 2-minute spacing, zero duplicates. After
that, almost nothing. Morning rush hour was missed entirely.

**Cause 2 — a genuine bug.** Every single failure (9 of 9) hit `trip_updates`
and never `vehicle_positions`. That pattern is the diagnosis: the program keeps
a network connection open and reuses it. After the laptop sleeps, that
connection is silently dead. `trip_updates` is fetched first each cycle, so it
always hit the dead connection and failed; the software then opened a fresh
connection, so `vehicle_positions` succeeded. **Every wake cost exactly one
download of the more valuable feed.**

**The fix, and why the obvious version is wrong.** The standard remedy is to
enable automatic retries in the networking library. That would have hidden the
error — and broken something more important. Those retries happen *underneath*
our code, invisibly: three silent retries means four requests sent to 511 while
our counter records one. The client-side budget protecting your key from being
throttled would have drifted steadily under-count until 511 cut you off.

So the retry was written at the level where **every attempt is counted**. Only
*connection* failures retry; an HTTP error like "429 Too Many Requests" is a
real answer from the server, and retrying it burns budget against a refusal.

Four regression tests were added (15 total, all passing), including one that
specifically asserts a failed attempt still consumes budget.

---

## 9. Where things stand

**Working:** the repository runs end-to-end; live data flows from 511 to disk;
the profiler produces real evidence; the poller is supervised and single-instance
locked; 15 tests pass.

**Learned:** 44.2% absent delay, 28 operators, method A viable for 15 of them,
method C nearly unusable, loop routes confirmed, ~1.4 GB/day.

**Outstanding:**

1. **Plug the laptop in** and run a 3-hour window across the evening rush. You
   don't need continuous coverage — one good peak window completes the analysis.
2. **Back up `data/raw/` off the machine.** It is the only irreplaceable thing
   here; everything else can be regenerated from it. Back up raw only — the
   decoded copy is three times larger and fully reproducible.
3. **Build the Kafka pipeline** — the course deliverable proper.

---

## Glossary

| Term | Meaning |
|---|---|
| **API** | A way for programs to request data from a service over the internet |
| **API key** | A password-like string identifying you, so limits can be enforced |
| **GTFS-Realtime** | The standard format transit agencies use to publish live data |
| **Protocol Buffers (protobuf)** | A compact binary data format; needs a schema to read |
| **Schema** | The definition of what fields exist and what they mean |
| **`HasField()`** | Asks whether a field was actually filled in — distinguishes "0" from "blank" |
| **Partition** | Splitting data into dated folders so queries can skip irrelevant ones |
| **Service date** | The transit service day a trip belongs to; runs past midnight |
| **Ingest date** | The date *we* downloaded something |
| **Virtual environment** | A private, isolated set of software libraries for one project |
| **Git / `.gitignore`** | Version-control software / the list of files it should ignore |
| **Makefile** | A file of named shortcuts (`make test` runs a longer command) |
| **Rate limit** | A cap on requests per hour; exceeding it gets your key blocked |
| **Kafka** | Software that carries a stream of events between programs (not yet built) |
| **Regression test** | A test written after fixing a bug, to catch it if it returns |
