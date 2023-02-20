{{ config(materialized = 'table') }}

with trips_data as (
    select * from {{ ref('stg_fhv_tripdata') }}
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    trips_data.tripid,
    trips_data.dispatching_base_num,
    trips_data.pickup_datetime,
    trips_data.dropOff_datetime,
    trips_data.PULocationID,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips_data.DOLocationID,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone, 
    trips_data.SR_Flag,
    trips_data.Affiliated_base_number
from trips_data 
inner join dim_zones as pickup_zone on trips_data.PULocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone on trips_data.DOLocationID = dropoff_zone.locationid