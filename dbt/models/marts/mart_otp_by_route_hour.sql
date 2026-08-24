{{ config(materialized='table') }}

-- OTP by route and LOCAL hour. The chart this feeds is the reason invariant
-- 3.5 exists: computed in UTC, every bar lands 7-8 hours from where it belongs
-- while the daily total stays exactly right.

select
    agency,
    route_id,
    hour_local,
    count(*)                                        as n_arrivals,
    percentile_approx(delay_s, 0.5)                 as median_delay_s,
    round(100 * avg(cast(is_on_time as int)), 1)    as on_time_pct,
    round(100 * avg(cast(delay_s > 300 as int)), 1) as late_pct
from {{ ref('stg_stop_otp') }}
group by agency, route_id, hour_local
having count(*) >= 20
