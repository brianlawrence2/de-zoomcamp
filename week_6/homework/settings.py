import pyspark.sql.types as T

FHV_INPUT_DATA_PATH = '../resources/fhv_tripdata_2019-01.csv'
GREEN_INPUT_DATA_PATH = '../resources/green_tripdata_2019-01.csv'
BOOTSTRAP_SERVERS = 'localhost:9092'

TOPIC_WINDOWED_VENDOR_ID_COUNT = 'vendor_counts_windowed'
TOPIC_RIDES_ALL = 'rides_all'

PRODUCE_TOPIC_RIDES_FHV_CSV = CONSUME_TOPIC_RIDES_FHV_CSV = 'fhv_rides_csv'
PRODUCE_TOPIC_RIDES_GREEN_CSV = CONSUME_TOPIC_RIDES_GREEN_CSV = 'green_rides_csv'

#RIDE_SCHEMA = T.StructType(
#    [T.StructField("vendor_id", T.IntegerType()),
#     T.StructField('tpep_pickup_datetime', T.TimestampType()),
#     T.StructField('tpep_dropoff_datetime', T.TimestampType()),
#     T.StructField("passenger_count", T.IntegerType()),
#     T.StructField("trip_distance", T.FloatType()),
#     T.StructField("payment_type", T.IntegerType()),
#     T.StructField("total_amount", T.FloatType()),
#     ])

RIDE_SCHEMA = T.StructType(
    [T.StructField("PUlocationID", T.IntegerType()),]
)
