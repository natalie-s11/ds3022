-- Mart: daily rollup — the grain a dashboard or report would consume.

select
    pickup_date,
    count(*)                              as trip_count,
    round(avg(trip_distance_miles), 2)    as avg_distance_miles,
    round(avg(trip_duration_minutes), 2)  as avg_duration_minutes,
    round(avg(avg_speed_mph), 2)          as avg_speed_mph,
    -- avg_speed_mph above is a mean of per-trip speeds (every trip weighs
    -- the same); this one is total miles / total hours, the true fleet speed
    round(sum(trip_distance_miles) / (sum(trip_duration_minutes) / 60.0), 2)
                                          as fleet_avg_speed_mph,
    round(sum(total_amount), 2)           as total_revenue,
    round(avg(tip_pct) * 100, 2)          as avg_tip_pct,
    -- total tips / total fares (dollar-weighted), vs. the mean of per-trip tip %
    round(sum(tip_amount) / sum(fare_amount) * 100, 2)
                                          as fleet_tip_pct
from {{ ref('fct_trips') }}
group by pickup_date
order by pickup_date
