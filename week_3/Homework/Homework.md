# Code for question 1

```SQL
CREATE OR REPLACE EXTERNAL TABLE `ny_taxi.fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://prefect-de-zoomcamp-plexiform-guide-375316/data/fhv_tripdata_2019-*.csv.gz']
);

SELECT count(*) FROM `plexiform-guide-375316.ny_taxi.fhv_tripdata`
```

# code for question 2

```SQL
select count(distinct affiliated_base_number) from `ny_taxi.fhv_tripdata`;

select count(distinct affiliated_base_number) from `ny_taxi.fhv_tripdata_table`;
```

# code for question 3

```SQL
select count(*) 
from `ny_taxi.fhv_tripdata_table`
where PUlocationID is null
and DOlocationID is null
```

# code for question 5

```SQL
select count(distinct affiliated_base_number)
from `ny_taxi.fhv_tripdata_table`
where pickup_datetime between '2019-03-01' and '2019-03-31'

select count(distinct affiliated_base_number)
from `ny_taxi.fhv_tripdata_partitioned`
where pickup_datetime between '2019-03-01' and '2019-03-31'
```

# code for question 6

