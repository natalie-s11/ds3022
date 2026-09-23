-- Singular test: fails if any row returned here — i.e. any trip whose
-- derived average speed is physically implausible for a NYC taxi. The
-- duration filter in fct_trips catches absurd durations, but a short
-- duration paired with a long distance can still slip through as a
-- triple-digit speed, which points to a GPS/meter glitch rather than
-- an actual trip.
--
-- Note: stg_yellow_tripdata already filters speed to (0, 60], so this
-- is a regression guard — it fails only if that staging filter is
-- loosened or removed. Try it: comment out the filter and re-run.

select *
from {{ ref('fct_trips') }}
where
    avg_speed_mph is null
    or avg_speed_mph <= 0
    or avg_speed_mph > 60
