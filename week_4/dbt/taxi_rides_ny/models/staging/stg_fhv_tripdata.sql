{{ config(materialized='view') }}

select
    int64_field_0 as tripid,
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    cast(PULocationID as int) as PULocationID,
    cast(DOLocationID as int) as DOLocationID,
    SR_Flag,
    Affiliated_base_number
from {{ source('staging', 'fhv_tripdata_partitioned') }}
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
