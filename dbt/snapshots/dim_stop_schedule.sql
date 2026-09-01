{#
  The SCD2 schedule dimension, owned by dbt.

  Agencies republish GTFS constantly. Overwriting the schedule silently
  recomputes every historical on-time number, so past metrics drift without
  anything erroring. Type 2 versioning is the fix: rows are never updated in
  place, they are closed and superseded.

  `strategy='check'` with an explicit column list rather than `check_cols='all'`
  is deliberate. A new GTFS zip differing only in a shape file or a booking
  rule must NOT open a new version of all 3.8M stop times -- that would grow
  the dimension by millions of rows per fetch and slow the as-of join for no
  information gain. Only schedule-relevant attributes count as a change.

  dbt maintains dbt_valid_from / dbt_valid_to, and the windows tile: the
  closing timestamp of one version equals the opening timestamp of the next,
  so an as-of join can use a half-open interval and never match twice.
#}

{% snapshot dim_stop_schedule %}

{{
    config(
        target_schema='transit',
        unique_key='schedule_key',
        strategy='check',
        check_cols=[
            'stop_id',
            'arrival_time',
            'departure_time',
            'route_id',
            'service_id',
            'direction_id',
            'timepoint',
            'pickup_type',
            'drop_off_type',
        ],
        file_format='delta',
        invalidate_hard_deletes=True,
    )
}}

select
    schedule_key,
    trip_id,
    stop_sequence,
    stop_id,
    arrival_time,
    departure_time,
    route_id,
    service_id,
    direction_id,
    timepoint,
    pickup_type,
    drop_off_type,
    feed_sha256,
    observed_at
from {{ lake('staging/gtfs_stop_schedule') }}

{% endsnapshot %}
