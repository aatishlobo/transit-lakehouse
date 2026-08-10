# Architecture Decision Records

One page per significant decision. Write before building where feasible.

## Planned

| ADR | Decision | Status |
|---|---|---|
| 001 | Kafka as ingest log vs. poll-straight-to-lake | pending |
| 002 | Feed profile — resolver availability per agency | **blocked on 3h of live polling** |
| 003 | Arrival resolver precedence ladder + provenance schema | blocked on 002 |
| 004 | Event-time watermark tuned to measured lateness distribution | pending |
| 005 | SCD2 schedule dimension — versioning grain (whole-feed vs per-trip) | pending |
| 006 | Partition key `trip_id` vs `route_id` — measured skew | pending |
| 007 | dbt incremental strategy + lookback window | pending |
| 008 | Compaction, Z-ORDER, partition column choice | pending |
| 009 | KEDA autoscaling on consumer lag (and why not the poller) | pending |
| 010 | Gold schema as ML feature source — the DE↔ML contract | pending |

## Template

```markdown
# ADR-NNN: <title>

**Status:** proposed | accepted | superseded
**Date:** YYYY-MM-DD

## Context
What forced the decision. Include measurements where they exist.

## Options considered
Each with its actual tradeoff, not a strawman.

## Decision
What was chosen.

## Consequences
What this makes easy, what it makes hard, and where it would be the wrong
choice in a different context.
```
