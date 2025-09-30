{{ config(materialized='table') }}

with em as (
  -- Normalize emissions lookup to a lowercase join key from vehicle_type
  select lower(vehicle_type) as join_key,
         co2_grams_per_mile
  from {{ source('main','vehicle_emissions') }}
),
em_avg as (
  -- Fallback average 
  select avg(co2_grams_per_mile) as avg_gpm
  from {{ source('main','vehicle_emissions') }}
),
trips_source as (
  select * from {{ source('main','trips_2024_clean') }}
),
joined as (
  select
    t.*,
    e.co2_grams_per_mile as gpm_exact,
    (select avg_gpm from em_avg) as gpm_avg
  from trips_source t
  left join em e
    -- Map 'yellow' -> 'yellow_taxi', 'green' -> 'green_taxi'
    on lower(t.taxi_color || '_taxi') = e.join_key
)

select
  taxi_color,
  pickup_datetime,
  dropoff_datetime,
  passenger_count,
  trip_distance_miles,
  pickup_location_id,
  dropoff_location_id,
  fare_amount,
  tip_amount,
  total_amount,
  payment_type,

  -- CO2 per trip (kg), 
  (trip_distance_miles * coalesce(gpm_exact, gpm_avg)) / 1000.0 as trip_co2_kgs,

  -- average mph (guard against zero duration)
  case when duration_seconds > 0
       then trip_distance_miles / (duration_seconds / 3600.0)
       else null end as avg_mph,

  -- time parts from pickup datetime
  cast(date_part('hour',  pickup_datetime) as int) as hour_of_day,
  cast(date_part('dow',   pickup_datetime) as int) as day_of_week,   -- 0=Sun..6=Sat
  cast(date_part('week',  pickup_datetime) as int) as week_of_year,
  cast(date_part('month', pickup_datetime) as int) as month_of_year
from joined
