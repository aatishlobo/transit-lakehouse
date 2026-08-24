{{ config(materialized='table') }}

-- Headline reliability per operator.
--
-- Early and late are reported SEPARATELY rather than folded into one
-- "off-schedule" number. They are different failures: a late bus costs you
-- time, an early bus you miss entirely. Collapsing them hides the worse one.

select
    agency,
    count(*)                                              as n_arrivals,
    count(distinct route_id)                              as n_routes,
    count(distinct service_date)                          as n_service_days,

    percentile_approx(delay_s, 0.5)                       as median_delay_s,
    round(avg(delay_s), 1)                                as mean_delay_s,
    percentile_approx(delay_s, 0.9)                       as p90_delay_s,

    round(100 * avg(cast(is_on_time as int)), 1)          as on_time_pct,
    round(100 * avg(cast(delay_s < -60 as int)), 1)       as early_pct,
    round(100 * avg(cast(delay_s > 300 as int)), 1)       as late_pct,

    -- Carried so a reader can see the error bar rather than infer it.
    max(poll_interval_s)                                  as poll_interval_s,
    round(100 * avg(cast(valid_from_assumed as int)), 1)  as assumed_schedule_pct

from {{ ref('stg_stop_otp') }}
group by agency
having count(*) >= 100
