-- Freshness as a singular test, tied to the poll cadence rather than guessed:
-- the poller runs every 120s, so a lake whose newest arrival is over 72h old
-- means ingestion or the batch stopped. Returning rows fails the test.
select
    max(actual_arrival_ts) as newest_arrival,
    current_timestamp()    as checked_at
from {{ lake('gold/fct_stop_otp') }}
having max(actual_arrival_ts) < current_timestamp() - interval 72 hours
