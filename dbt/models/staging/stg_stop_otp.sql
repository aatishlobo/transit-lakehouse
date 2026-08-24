{{ config(materialized='view') }}

-- Staging: the ONE place UTC becomes local time.
--
-- Invariant 3.5. Everything upstream is stored UTC; the conversion to
-- America/Los_Angeles happens exactly once, here, at the serving boundary.
-- Truncating hour-of-day in UTC produces an OTP-by-hour chart shifted 7-8
-- hours while every total stays correct -- the most user-visible possible bug
-- in this project, and one no row count would catch.

select
    service_date,
    trip_id,
    stop_sequence,
    stop_id,
    route_id,
    split(trip_id, ':')[0]                                as agency,

    actual_arrival_ts,
    scheduled_arrival_ts,
    delay_s,
    is_on_time,
    schedule_matched,

    -- Local-time derivations, downstream of the single conversion.
    from_utc_timestamp(actual_arrival_ts, 'America/Los_Angeles')      as actual_arrival_local,
    hour(from_utc_timestamp(actual_arrival_ts, 'America/Los_Angeles')) as hour_local,
    date_format(
        from_utc_timestamp(actual_arrival_ts, 'America/Los_Angeles'), 'EEEE'
    )                                                                  as day_name_local,

    -- Provenance travels all the way to the mart. Resolver availability varies
    -- by operator, so without these an "agency effect" can just be an artifact
    -- of which method resolved that agency's arrivals.
    arrival_method,
    arrival_confidence,
    poll_interval_s,
    n_stopped_observations,
    resolver_version,
    valid_from_assumed

from {{ lake('gold/fct_stop_otp') }}
where delay_s is not null
