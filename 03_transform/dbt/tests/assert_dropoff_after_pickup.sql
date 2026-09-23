-- Singular test: fails if any trip ends before (or exactly when) it starts.
-- Regression guard for the `dropoff_at > pickup_at` filter in staging.

select *
from {{ ref('stg_yellow_tripdata') }}
where dropoff_at <= pickup_at
