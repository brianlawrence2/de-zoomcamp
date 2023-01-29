# Code for SQL questions

```SQL
select 
	cast(lpep_pickup_datetime as date) as ReportDate,
	max(trip_distance) as LongestTrip
from green_taxi_data
where cast(lpep_pickup_datetime as date) in ('2019-01-18','2019-01-28',
											 '2019-01-15','2019-01-10')
group by cast(lpep_pickup_datetime as date)											
order by 2 desc;

select *
from green_taxi_data
where lpep_pickup_datetime > '2019-01-01'
order by lpep_pickup_datetime;

select 
	passenger_count,
	count(1) as trips
from green_taxi_data
where passenger_count in (2,3)
and cast(lpep_pickup_datetime as date) = '2019-01-01' 
group by passenger_count;

select * from green_taxi_data limit 10;
select * from taxi_zone_lookup limit 10;

select
	dro."Zone",
	max(tip_amount) as MaxTip
from green_taxi_data as td
inner join taxi_zone_lookup as pu on td."PULocationID" = pu."LocationID"
inner join taxi_zone_lookup as dro on td."DOLocationID" = dro."LocationID"
where pu."Zone" = 'Astoria'
group by dro."Zone"
order by MaxTip desc;
```

# python code for loading datasets

```python
df_iter = pd.read_csv('green_tripdata_2019-01.csv.gz', iterator=True, chunksize=100000)

df = next(df_iter)
df.head(0).to_sql(name='green_taxi_data', con=engine, if_exists='replace')
df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

df.to_sql(name='green_taxi_data', con=engine, if_exists='append')

while True:
    t_start = time()
    df = next(df_iter)
    
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    
    df.to_sql(name='green_taxi_data', con=engine, if_exists='append')
    
    t_end = time()
    
    print(f'inserted another chunk..., took {t_end - t_start} seconds')

df_iter = pd.read_csv('taxi+_zone_lookup.csv', iterator=True, chunksize=100000)
df = next(df_iter)
df.head(0).to_sql(name='taxi_zone_lookup', con=engine, if_exists='replace')
df.to_sql(name='taxi_zone_lookup', con=engine, if_exists='append')
```

