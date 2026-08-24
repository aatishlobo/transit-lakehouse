{{ config(materialized='table') }}

-- The demo-able artifact: least reliable routes, ranked.
--
-- A minimum observation count is enforced rather than left to the reader. With
-- ~21% capture, a route with nine observations can top a "worst" list on noise
-- alone, and a leaderboard built on noise is worse than no leaderboard.

select
    agency,
    route_id,
    n_arrivals,
    median_delay_s,
    on_time_pct,
    late_pct,
    row_number() over (order by on_time_pct asc) as worst_rank
from (
    select
        agency,
        route_id,
        count(*)                                        as n_arrivals,
        percentile_approx(delay_s, 0.5)                 as median_delay_s,
        round(100 * avg(cast(is_on_time as int)), 1)    as on_time_pct,
        round(100 * avg(cast(delay_s > 300 as int)), 1) as late_pct
    from {{ ref('stg_stop_otp') }}
    group by agency, route_id
    having count(*) >= 50
)
