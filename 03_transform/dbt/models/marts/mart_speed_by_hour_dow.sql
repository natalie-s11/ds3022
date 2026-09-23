-- Mart: avg speed by day of week x hour of day — answers QUESTIONS.md #5
-- (does avg_speed_mph track known NYC congestion patterns, slower during
-- rush hours, faster overnight, and does that vary by day of week?).
-- One row per (day_of_week, hour), grain is plot-ready as a
-- day-of-week x hour heatmap or overlaid per-day line chart.

-- note: DuckDB's dayofweek() is 0 = Sunday ... 6 = Saturday
select
    dayofweek(pickup_at)                  as pickup_dow,
    dayname(pickup_at)                    as pickup_day_name,
    hour(pickup_at)                       as pickup_hour,
    count(*)                              as trip_count,
    round(avg(avg_speed_mph), 2)          as avg_speed_mph,
    -- avg_speed_mph above is a mean of per-trip speeds (every trip weighs
    -- the same); this one is total miles / total hours, the true fleet speed
    round(sum(trip_distance_miles) / (sum(trip_duration_minutes) / 60.0), 2)
                                          as fleet_avg_speed_mph,
    round(avg(trip_duration_minutes), 2)  as avg_duration_minutes,
    round(avg(trip_distance_miles), 2)    as avg_distance_miles
from {{ ref('fct_trips') }}
group by pickup_dow, pickup_day_name, pickup_hour
order by pickup_dow, pickup_hour
