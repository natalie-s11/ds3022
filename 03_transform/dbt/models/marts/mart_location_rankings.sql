-- Mart: the 10 busiest and 10 quietest taxi zones, ranked separately for
-- pickups and dropoffs. One row per (location_type, rank_group, location_rank)
-- — 40 rows total.
--
-- Starts from the zone lookup (not the trips) so a zone with zero trips
-- still shows up with trip_count = 0 instead of silently vanishing from
-- the bottom 10. Ties are broken by location_id so the ranking is stable.

{{ config(materialized='view') }}

with zones as (
    select location_id, borough, zone
    from {{ ref('stg_taxi_zones') }}
    -- 264 = Unknown, 265 = Outside of NYC: not mappable places
    where location_id not in (264, 265)
),

trips as (
    select pickup_location_id, dropoff_location_id
    from {{ ref('fct_trips') }}
),

counts as (
    select 'pickup' as location_type, pickup_location_id as location_id, count(*) as trip_count
    from trips
    group by all

    union all

    select 'dropoff', dropoff_location_id, count(*)
    from trips
    group by all
),

ranked as (
    select
        types.location_type,
        zones.location_id,
        zones.borough,
        zones.zone,
        coalesce(counts.trip_count, 0) as trip_count,
        row_number() over (
            partition by types.location_type
            order by coalesce(counts.trip_count, 0) desc, zones.location_id
        ) as rank_desc,
        row_number() over (
            partition by types.location_type
            order by coalesce(counts.trip_count, 0) asc, zones.location_id
        ) as rank_asc
    from zones
    cross join (values ('pickup'), ('dropoff')) as types(location_type)
    left join counts
        on counts.location_type = types.location_type
        and counts.location_id = zones.location_id
)

select location_type, 'top' as rank_group, rank_desc as location_rank,
       location_id, borough, zone, trip_count
from ranked
where rank_desc <= 10

union all

select location_type, 'bottom', rank_asc,
       location_id, borough, zone, trip_count
from ranked
where rank_asc <= 10

order by location_type desc, rank_group desc, location_rank
