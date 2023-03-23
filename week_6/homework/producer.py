import csv
from time import sleep
from typing import Dict
from kafka import KafkaProducer

from settings import BOOTSTRAP_SERVERS, FHV_INPUT_DATA_PATH, PRODUCE_TOPIC_RIDES_FHV_CSV, \
                     GREEN_INPUT_DATA_PATH, PRODUCE_TOPIC_RIDES_GREEN_CSV


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('Record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


class RideCSVProducer:
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)
        # self.producer = Producer(producer_props)

    @staticmethod
    def read_records(resource_path: str):
        records, ride_keys = [], []
        i = 0
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header
            for row in reader:
                # vendor_id, passenger_count, trip_distance, payment_type, total_amount
                
                if resource_path == FHV_INPUT_DATA_PATH and row[3] != '':
                    records.append(f'{row[3]}')
                    ride_keys.append(str(row[3]))
                elif resource_path == GREEN_INPUT_DATA_PATH:
                    records.append(f'{row[5]}')
                    ride_keys.append(str(row[5]))
                    
                i += 1
                if i == 1000:
                    break
        return zip(ride_keys, records)

    def publish(self, topic: str, records: [str, str]):
        for key_value in records:
            key, value = key_value
            try:
                self.producer.send(topic=topic, key=key, value=value)
                print(f"Producing record for <key: {key}, value:{value}>")
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {value}: {e}")

        self.producer.flush()
        sleep(1)


if __name__ == "__main__":
    config = {
        'bootstrap_servers': [BOOTSTRAP_SERVERS],
        'key_serializer': lambda x: x.encode('utf-8'),
        'value_serializer': lambda x: x.encode('utf-8')
    }
    producer = RideCSVProducer(props=config)
    fhv_ride_records = producer.read_records(resource_path=FHV_INPUT_DATA_PATH)
    print(fhv_ride_records)
    producer.publish(topic=PRODUCE_TOPIC_RIDES_FHV_CSV, records=fhv_ride_records)
    
    green_ride_records = producer.read_records(resource_path=GREEN_INPUT_DATA_PATH)
    print(green_ride_records)
    producer.publish(topic=PRODUCE_TOPIC_RIDES_GREEN_CSV, records=green_ride_records)
